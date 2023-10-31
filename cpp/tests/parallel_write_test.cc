#include <iostream>

#include <arcae/safe_table_proxy.h>
#include <arcae/table_factory.h>
#include <tests/test_utils.h>
#include <arrow/util/thread_pool.h>
#include <arrow/testing/gtest_util.h>
#include <casacore/tables/Tables.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>
#include <gtest/gtest.h>

using casacore::Array;
using casacore::ArrayColumn;
using casacore::ArrayColumnDesc;
using casacore::ColumnDesc;
using CasaComplex = casacore::Complex;
using MS = casacore::MeasurementSet;
using MSColumns = casacore::MSMainEnums::PredefinedColumns;
using casacore::SetupNewTable;
using casacore::ScalarColumn;
using casacore::Slice;
using casacore::Slicer;
using casacore::Table;
using casacore::TableDesc;
using casacore::TableColumn;
using casacore::TableProxy;
using casacore::TiledColumnStMan;
using IPos = casacore::IPosition;

using namespace std::string_literals;

static constexpr std::size_t knrow = 20;
static constexpr std::size_t knchan = 4;
static constexpr std::size_t kncorr = 1;
static constexpr std::size_t knthreads = 16;

static constexpr std::size_t kinc = 2;

class WriteTests : public ::testing::Test {
  protected:
    std::shared_ptr<arcae::SafeTableProxy> table_proxy_;
    std::string table_name_;

    void SetUp() override {
      auto factory = [this]() -> arrow::Result<std::shared_ptr<TableProxy>> {
        auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        auto table_desc = TableDesc(MS::requiredTableDesc());
        table_name_ = std::string(test_info->name() + "-"s + arcae::hexuuid(4) + ".table"s);

        auto data_shape = IPos({kncorr, knchan});
        auto tile_shape = IPos({kncorr, knchan, 1});
        auto data_column_desc = ArrayColumnDesc<CasaComplex>(
            "MODEL_DATA", data_shape, ColumnDesc::FixedShape);
        table_desc.addColumn(data_column_desc);
        auto storage_manager = TiledColumnStMan("TiledModelData", tile_shape);
        auto setup_new_table = SetupNewTable(table_name_, table_desc, Table::New);
        setup_new_table.bindColumn("MODEL_DATA", storage_manager);
        auto ms = MS(setup_new_table, knrow);

        // Create a ramp of data values and insert it into the data column
        auto data = Array<CasaComplex>(IPos{kncorr, knchan, knrow});

        for(auto [i, it] = std::tuple{0, data.begin()}; it != data.end(); ++it, ++i) {
          *it = i;
        }

        auto data_column = ArrayColumn<CasaComplex>(TableColumn(ms, "MODEL_DATA"));
        data_column.putColumn(data);

        return std::make_shared<TableProxy>(ms);
      };

      ASSERT_OK_AND_ASSIGN(table_proxy_, arcae::SafeTableProxy::Make(factory));
    }
};

// Parallel SafeTableProxy writes
TEST_F(WriteTests, Parallel) {
  ASSERT_TRUE(table_proxy_);
  ASSERT_EQ(table_proxy_->nRow(), knrow);
  table_proxy_.reset(); // close

  {
    // Write some data in parallel;
    std::vector<std::shared_ptr<arcae::SafeTableProxy>> writers;
    std::vector<arrow::Future<bool>> futures;
    ASSERT_OK_AND_ASSIGN(auto pool, arrow::internal::ThreadPool::Make(knthreads));

    // Create knthreads worth of SafeTableProxies
    for(std::size_t i=0; i < knthreads; ++i) {
      ASSERT_OK_AND_ASSIGN(auto writer, arcae::OpenTable(table_name_, false));
      writers.emplace_back(std::move(writer));
    }

    // Iterate over the space of knrows in increments of kinc
    // Each iteration will independently write it's part of the column
    for(auto [r, c] = std::tuple{std::size_t{0}, std::size_t{0}}; r < knrow; r += kinc, ++c) {
      std::size_t end = std::min(r + kinc, knrow);
      auto result = arrow::DeferNotOk(pool->Submit(
          [start=r, nrow=end-r, writer=writers[c % knthreads]]() mutable {
              return writer->run([start=start, nrow=nrow](TableProxy & proxy) mutable -> arrow::Result<bool> {
                  auto table = proxy.table();
                  auto table_column = TableColumn(table, "MODEL_DATA");
                  const auto & column_desc = table_column.columnDesc();
                  auto column = ArrayColumn<CasaComplex>(table_column);
                  column.setMaximumCacheSize(1);
                  auto data = Array<CasaComplex>(IPos({kncorr, knchan, int(nrow)}));
                  data.set(start);

                  try {
                      column.putColumnRange(Slice(start, nrow), data);
                      table.flush();
                  } catch(std::exception & e) {
                      return arrow::Status::Invalid("Write failed ", e.what());
                  }

                  return arrow::Result{true};
              });
          }));

      futures.push_back(result);
    }

    // Wait for all futures to complete
    auto all = arrow::All(futures).result().ValueOrDie();
    for(auto res: all) res.ValueOrDie();
    all.clear();
    futures.clear();
  }

  // Reopen the table
  ASSERT_OK_AND_ASSIGN(table_proxy_, arcae::OpenTable(table_name_, true));

  // Using the same iteration pattern for writes, read sections of the table
  // and compare them for the expected result
  for(auto [r, c] = std::tuple{std::size_t{0}, std::size_t{0}}; r < knrow; r += kinc, ++c) {
    std::size_t end = std::min(r + kinc, knrow);

    auto future = table_proxy_->run([start=r, nrow=end - r](TableProxy & proxy) mutable -> arrow::Result<Array<CasaComplex>> {
      auto table = proxy.table();
      auto table_column = TableColumn(table, "MODEL_DATA");
      const auto & column_desc = table_column.columnDesc();
      auto column = ArrayColumn<CasaComplex>(table_column);
      column.setMaximumCacheSize(1);

      try {
          return column.getColumnRange(Slice(start, nrow));
      } catch(std::exception & e) {
          return arrow::Status::Invalid("Write failed ", e.what());
      }
    });

    ASSERT_OK_AND_ASSIGN(auto data, future);

    for(auto it = data.begin(); it != data.end(); ++it) {
      EXPECT_EQ(*it, CasaComplex(r, 0));
    }
  }
}
