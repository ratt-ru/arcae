#include <memory>
#include "gtest/gtest.h"

#include <arrow/ipc/json_simple.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>

#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/BasicSL/Complexfwd.h>
#include <casacore/casa/Utilities/DataType.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>
#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/SetupNewTab.h>
#include <casacore/tables/Tables/TableProxy.h>

#include <tests/test_utils.h>

#include <gtest/gtest.h>

#include "arcae/isolated_table_proxy.h"

using ::arcae::GetArrayColumn;
using ::arcae::detail::IsolatedTableProxy;

using casacore::Array;
using casacore::ArrayColumnDesc;
using casacore::ColumnDesc;
using casacore::Complex;
using MS = casacore::MeasurementSet;
using casacore::Record;
using casacore::SetupNewTable;
using casacore::Table;
using casacore::TableDesc;
using casacore::TableLock;
using casacore::TableProxy;
using IPos = casacore::IPosition;

using namespace std::string_literals;

static constexpr std::size_t knrow = 10;
static constexpr std::size_t knchan = 4;
static constexpr std::size_t kncorr = 2;

namespace {

class IsolatedTableProxyTest : public ::testing::Test {
 protected:
  std::string table_name_;

  void SetUp() override {
    auto* test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    table_name_ = std::string(test_info->name() + "-"s + arcae::hexuuid(4) + ".table"s);

    auto table_desc = TableDesc(MS::requiredTableDesc());
    auto data_shape = IPos({kncorr, knchan});
    auto data_column_desc =
        ArrayColumnDesc<Complex>("MODEL_DATA", data_shape, ColumnDesc::FixedShape);
    table_desc.addColumn(data_column_desc);
    auto setup_new_table = SetupNewTable(table_name_, table_desc, Table::New);
    auto ms = MS(setup_new_table, knrow);
    auto data = GetArrayColumn<Complex>(ms, MS::MODEL_DATA);
    data.putColumn(Array<Complex>(IPos({kncorr, knchan, knrow}), {1, 2}));
  }

  arrow::Result<std::shared_ptr<IsolatedTableProxy>> OpenTable() {
    return IsolatedTableProxy::Make([name = table_name_]() {
      auto lock = TableLock(TableLock::LockOption::AutoLocking);
      auto lockoptions = Record();
      lockoptions.define("option", "nolock");
      lockoptions.define("internal", lock.interval());
      lockoptions.define("maxwait", casacore::Int(lock.maxWait()));
      return std::make_shared<TableProxy>(name, lockoptions, Table::Old);
    });
  }
};

TEST_F(IsolatedTableProxyTest, MakeTable) {
  ASSERT_OK_AND_ASSIGN(
      auto itp, IsolatedTableProxy::Make([]() {
        auto* test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        auto name = std::string(test_info->name() + "-"s + arcae::hexuuid(4) + ".table"s);
        auto table_desc = TableDesc(MS::requiredTableDesc());
        auto setup_new_table = SetupNewTable(name, table_desc, Table::New);
        return std::make_shared<TableProxy>(MS(setup_new_table, knrow));
      }));

  {
    // Test in sync mode
    ASSERT_OK_AND_ASSIGN(
        auto nrow, itp->RunSync([](const TableProxy& tp) { return tp.table().nrow(); }));
    EXPECT_EQ(nrow, knrow);
  }
  {
    // Test in async mode
    auto fut = itp->RunAsync([](const TableProxy& tp) { return tp.table().nrow(); });
    ASSERT_OK_AND_ASSIGN(auto nrow, fut.MoveResult());
    EXPECT_EQ(nrow, knrow);
  }
}

TEST_F(IsolatedTableProxyTest, RunAsyncConstAndNonConst) {
  ASSERT_OK_AND_ASSIGN(auto itp, OpenTable());
  {
    // Test in sync mode
    ASSERT_OK_AND_ASSIGN(
        auto cnrow, itp->RunSync([](const TableProxy& tp) { return tp.table().nrow(); }));
    EXPECT_EQ(cnrow, knrow);
    ASSERT_OK_AND_ASSIGN(auto nrow,
                         itp->RunSync([](TableProxy& tp) { return tp.table().nrow(); }));
    EXPECT_EQ(nrow, knrow);
  }

  {
    // Test in async mode
    auto fut = itp->RunAsync([](const TableProxy& tp) { return tp.table().nrow(); });
    ASSERT_OK_AND_ASSIGN(auto cnrow, fut.MoveResult());
    EXPECT_EQ(cnrow, knrow);
    fut = itp->RunSync([](TableProxy& tp) { return tp.table().nrow(); });
    ASSERT_OK_AND_ASSIGN(auto nrow, fut.MoveResult());
  }
}

TEST_F(IsolatedTableProxyTest, GetColumn) {
  ASSERT_OK_AND_ASSIGN(auto itp, OpenTable());
  {
    // Test in sync mode
    ASSERT_OK_AND_ASSIGN(auto data,
                         itp->RunSync([column_name = "MODEL_DATA"](const TableProxy& tp) {
                           auto column = GetArrayColumn<Complex>(tp.table(), column_name);
                           return column.getColumn();
                         }));
    EXPECT_EQ(data.shape(), IPos({kncorr, knchan, knrow}));
  }

  {
    // Test in async mode
    auto fut = itp->RunAsync([column_name = "MODEL_DATA"](const TableProxy& tp) {
      auto column = GetArrayColumn<Complex>(tp.table(), column_name);
      return column.getColumn();
    });
    ASSERT_OK_AND_ASSIGN(auto data, fut.MoveResult());
    EXPECT_EQ(data.shape(), IPos({kncorr, knchan, knrow}));
  }
}

TEST_F(IsolatedTableProxyTest, FailIfClosed) {
  ASSERT_OK_AND_ASSIGN(auto itp, OpenTable());
  ASSERT_OK_AND_ASSIGN(auto close_result, itp->Close());
  EXPECT_EQ(close_result, true);
  ASSERT_NOT_OK(itp->RunSync([](const TableProxy& tp) { return true; }));
  ASSERT_OK_AND_ASSIGN(close_result, itp->Close());
  EXPECT_EQ(close_result, false);
}

}  // namespace
