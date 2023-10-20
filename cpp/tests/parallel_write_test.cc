#include <iostream>

#include <arcae/safe_table_proxy.h>
#include <arcae/table_factory.h>
#include <tests/test_utils.h>
#include <arrow/util/thread_pool.h>
#include <arrow/testing/gtest_util.h>
#include <casacore/tables/Tables.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>
#include <gtest/gtest.h>

using namespace std::string_literals;

static constexpr std::size_t knrow = 20;
static constexpr std::size_t knchan = 4;
static constexpr std::size_t kncorr = 1;
static constexpr std::size_t knthreads = 16;

class WriteTests : public ::testing::Test {
  protected:
    std::shared_ptr<arcae::SafeTableProxy> table_proxy_;

    void SetUp() override {
      auto factory = []() -> arrow::Result<std::shared_ptr<casacore::TableProxy>> {
        auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        auto table_desc = casacore::TableDesc(casacore::MeasurementSet::requiredTableDesc());
        auto tablename = std::string(test_info->name() + "-"s + arcae::hexuuid(4) + ".table"s);
        auto setup_new_table = casacore::SetupNewTable(tablename, table_desc, casacore::Table::New);
        auto ms = casacore::MeasurementSet(setup_new_table, knrow);

        {
            auto data_shape = casacore::IPosition({kncorr, knchan});
            auto tile_shape = casacore::IPosition({1, kncorr, knchan});
            auto table_desc = casacore::TableDesc();
            auto column_desc = casacore::ArrayColumnDesc<casacore::Complex>(
                "MODEL_DATA", data_shape, casacore::ColumnDesc::FixedShape);
            table_desc.addColumn(column_desc);
            auto storage_manager = casacore::TiledColumnStMan("TiledModelData", tile_shape);
            ms.addColumn(table_desc, storage_manager);
        }

        return std::make_shared<casacore::TableProxy>(ms);
      };

      ASSERT_OK_AND_ASSIGN(table_proxy_, arcae::SafeTableProxy::Make(factory));
    }
};

// Parallel SafeTableProxy writes
TEST_F(WriteTests, Parallel) {
  ASSERT_TRUE(table_proxy_);
  ASSERT_EQ(table_proxy_->nRow(), knrow);

  auto data_result = table_proxy_->run(
    [](const casacore::TableProxy & proxy) -> arrow::Result<casacore::Array<casacore::Complex>> {
        auto table = proxy.table();
        auto table_column = casacore::TableColumn(table, "MODEL_DATA");
        auto column = casacore::ArrayColumn<casacore::Complex>(table_column);
        return column.getColumn();
    });

  ASSERT_OK_AND_ASSIGN(auto data, data_result);

  for(auto [i, it] = std::tuple{0, data.begin()}; it != data.end(); ++it, ++i) {
    *it = i;
  }

  auto name_result = table_proxy_->run(
    [data = std::move(data)](const casacore::TableProxy & proxy) -> arrow::Result<std::string> {
        auto table = proxy.table();
        auto table_column = casacore::TableColumn(table, "MODEL_DATA");
        auto column = casacore::ArrayColumn<casacore::Complex>(table_column);
        column.putColumn(data);
        return table.tableName();
    });

  ASSERT_OK_AND_ASSIGN(auto name, name_result);
  table_proxy_.reset();

  std::vector<std::shared_ptr<arcae::SafeTableProxy>> writers;

  for(std::size_t i=0; i < knthreads; ++i) {
    ASSERT_OK_AND_ASSIGN(auto writer, arcae::OpenTable(name, false));
    writers.emplace_back(std::move(writer));
  }

  ASSERT_OK_AND_ASSIGN(auto pool, arrow::internal::ThreadPool::Make(knthreads));

  static constexpr std::size_t inc = 2;

  std::vector<arrow::Future<bool>> futures;

  for(auto [r, c] = std::tuple{std::size_t{0}, std::size_t{0}}; r < knrow; r += inc, ++c) {
    std::size_t end = std::min(r + inc, knrow);
    auto result = arrow::DeferNotOk(pool->Submit(
        [start=r, nrow=end-r, writer=writers[c % knthreads]]() mutable {
            return writer->run([start=start, nrow=nrow](casacore::TableProxy & proxy) mutable -> arrow::Result<bool> {
                auto table = proxy.table();
                auto table_column = casacore::TableColumn(table, "MODEL_DATA");
                auto & column_desc = table_column.columnDesc();
                auto column = casacore::ArrayColumn<casacore::Complex>(table_column);
                column.setMaximumCacheSize(1);
                auto data = casacore::Array<casacore::Complex>(casacore::IPosition({kncorr, knchan, int(nrow)}));
                data.set(start);

                try {
                    column.putColumnRange(casacore::Slice(start, nrow), data);
                    table.flush();
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("Write failed ", e.what());
                }

                return arrow::Result{true};
            });
        }));

    futures.push_back(result);
  }

  auto all = arrow::All(futures).result().ValueOrDie();
  for(auto res: all) res.ValueOrDie();
}