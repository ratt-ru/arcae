// With ninstances > 1, a writer (instance 0, via SpawnWriter) must exclude
// readers running on the *other* instances. The patched FileLocker only
// coordinates multi-reader/single-writer access among threads sharing a
// single FileLocker object; because arcae gives each instance its own
// TableProxy -> Table -> LockFile -> FileLocker, the
// coordination historically did not span instances and a reader could acquire a
// read lock on the same byte range while a writer held the write lock (silently
// downgrading it at the OS level).
//
// WriterExcludesReadersAcrossInstances is deterministic: it fails on the
// pre-fix build (readers observe the writer in its critical section) and passes
// once the lock state is shared across instances.

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/util/future.h>

#include <casacore/casa/Arrays/Array.h>
#include <casacore/casa/Arrays/ArrayMath.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>
#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/ArrColDesc.h>
#include <casacore/tables/Tables/ArrayColumn.h>
#include <casacore/tables/Tables/SetupNewTab.h>
#include <casacore/tables/Tables/TableProxy.h>

#include <tests/test_utils.h>

#include "arcae/isolated_table_proxy.h"

using ::arcae::detail::IsolatedTableProxy;
using ::arrow::Result;

using LockType = ::casacore::FileLocker::LockType;
using ::casacore::Array;
using ::casacore::ArrayColumn;
using ::casacore::ArrayColumnDesc;
using ::casacore::ColumnDesc;
using CasaInt = ::casacore::Int;
using MS = ::casacore::MeasurementSet;
using ::casacore::Record;
using ::casacore::SetupNewTable;
using ::casacore::Table;
using ::casacore::TableColumn;
using ::casacore::TableDesc;
using ::casacore::TableProxy;
using IPos = ::casacore::IPosition;

using namespace std::string_literals;
using namespace std::chrono_literals;

namespace {

static constexpr std::size_t knrow = 512;
static constexpr std::size_t knchan = 16;
static constexpr std::size_t kncorr = 4;

class ConcurrentReadWriteTest : public ::testing::Test {
 protected:
  std::string table_name_;

  void SetUp() override {
    auto* ti = ::testing::UnitTest::GetInstance()->current_test_info();
    table_name_ = std::string(ti->name() + "-"s + arcae::hexuuid(4) + ".table"s);

    auto table_desc = TableDesc(MS::requiredTableDesc());
    auto data_shape = IPos({kncorr, knchan});
    table_desc.addColumn(
        ArrayColumnDesc<CasaInt>("DATA", data_shape, ColumnDesc::FixedShape));
    auto setup = SetupNewTable(table_name_, table_desc, Table::New);
    auto ms = MS(setup, knrow);
    auto col = ArrayColumn<CasaInt>(TableColumn(ms, "DATA"));
    col.putColumn(Array<CasaInt>(IPos({kncorr, knchan, knrow}), CasaInt(0)));
  }

  Result<std::shared_ptr<IsolatedTableProxy>> OpenTable(std::size_t ninstances) {
    return IsolatedTableProxy::Make(
        [name = table_name_]() {
          auto lockoptions = Record();
          lockoptions.define("option", "user");
          auto tp = std::make_shared<TableProxy>(name, lockoptions, Table::Old);
          tp->reopenRW();
          return tp;
        },
        ninstances);
  }
};

// Deterministic gate: a writer on instance 0 must exclude readers on other
// instances for the duration that it holds the write lock.
TEST_F(ConcurrentReadWriteTest, WriterExcludesReadersAcrossInstances) {
  ASSERT_OK_AND_ASSIGN(auto itp, OpenTable(/*ninstances=*/2));
  auto writer = itp->SpawnWriter();

  std::atomic<bool> writer_in_critical{false};
  std::atomic<bool> reader_saw_writer{false};
  std::atomic<int> reads_completed{0};

  // The writer holds the write lock for a fixed window. The flag is only ever
  // true while the write lock is held (MaybeLockAndFinalise acquires before the
  // functor runs and releases after it returns).
  auto wfut = writer->RunAsync(
      [&](TableProxy&) -> Result<bool> {
        writer_in_critical = true;
        std::this_thread::sleep_for(400ms);
        writer_in_critical = false;
        return true;
      },
      LockType::Write);

  // Let the writer enter its critical section.
  std::this_thread::sleep_for(80ms);
  ASSERT_TRUE(writer_in_critical.load()) << "writer did not enter its critical section";

  // Fan readers out across the remaining instances while the writer holds the
  // lock. A reader that runs its functor (i.e. holds a read lock) while the
  // writer is still in its critical section proves the exclusion is broken.
  std::vector<arrow::Future<bool>> rfuts;
  while (writer_in_critical.load()) {
    rfuts.push_back(itp->RunAsync(
        [&](const TableProxy&) -> Result<bool> {
          if (writer_in_critical.load()) reader_saw_writer = true;
          return true;
        },
        LockType::Read));
    std::this_thread::sleep_for(5ms);
  }

  for (auto& f : rfuts) {
    auto r = f.MoveResult();
    if (r.ok()) ++reads_completed;
  }
  ASSERT_OK(wfut.status());

  EXPECT_FALSE(reader_saw_writer.load())
      << "A reader acquired a read lock while a writer held the write lock on "
         "another instance (cross-instance MRSW exclusion failed). "
      << reads_completed.load() << " reads completed.";

  // The writer borrows instance 0 from itp; its custom deleter releases the
  // borrowed proxy without closing it, so we drop it (rather than Close it,
  // which would double-close the shared proxy) before closing the parent.
  writer.reset();
  ASSERT_OK(itp->Close());
}

// Stress gate: a writer repeatedly writes a uniform value to every row while
// readers fan across the other instances. A correctly excluded read always
// observes an internally uniform column; a torn read (mixed old/new values)
// indicates the writer was not excluded or stale buffers were served.
TEST_F(ConcurrentReadWriteTest, NoTornReadsAcrossInstances) {
  static constexpr int knwrites = 200;
  ASSERT_OK_AND_ASSIGN(auto itp, OpenTable(/*ninstances=*/4));
  auto writer = itp->SpawnWriter();

  std::atomic<bool> writing{true};
  std::atomic<int> torn_reads{0};
  std::atomic<int> reads_completed{0};

  std::thread writer_thread([&]() {
    for (int w = 1; w <= knwrites; ++w) {
      auto status = writer->RunSync(
          [w](TableProxy& tp) -> Result<bool> {
            auto col = ArrayColumn<CasaInt>(TableColumn(tp.table(), "DATA"));
            col.putColumn(Array<CasaInt>(IPos({kncorr, knchan, knrow}), CasaInt(w)));
            return true;
          },
          LockType::Write);
      if (!status.ok()) {
        ADD_FAILURE() << "write failed: " << status.status();
        break;
      }
    }
    writing = false;
  });

  while (writing.load()) {
    std::vector<arrow::Future<bool>> rfuts;
    for (int i = 0; i < 16; ++i) {
      rfuts.push_back(itp->RunAsync(
          [](const TableProxy& tp) -> Result<bool> {
            auto col = ArrayColumn<CasaInt>(TableColumn(tp.table(), "DATA"));
            auto data = col.getColumn();
            // Uniform write => min == max for a non-torn read.
            return casacore::min(data) == casacore::max(data);
          },
          LockType::Read));
    }
    for (auto& f : rfuts) {
      auto r = f.MoveResult();
      if (r.ok()) {
        ++reads_completed;
        if (!*r) ++torn_reads;
      }
    }
  }

  writer_thread.join();
  EXPECT_EQ(torn_reads.load(), 0)
      << "observed torn reads (" << reads_completed.load() << " reads total)";

  writer.reset();
  ASSERT_OK(itp->Close());
}

}  // namespace
