#include <random>

#include <arcae/new_convert_visitor.h>
#include <arcae/safe_table_proxy.h>
#include <arcae/table_factory.h>
#include <casacore/tables/Tables.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>
#include <tests/test_utils.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <arrow/testing/gtest_util.h>

using casacore::Array;
using casacore::ArrayColumn;
using casacore::ArrayColumnDesc;
using casacore::ColumnDesc;
using CasaComplex = casacore::Complex;
using MS = casacore::MeasurementSet;
using MSColumns = casacore::MSMainEnums::PredefinedColumns;
using casacore::SetupNewTable;
using casacore::ScalarColumn;
using casacore::Slicer;
using casacore::Table;
using casacore::TableDesc;
using casacore::TableColumn;
using casacore::TableProxy;
using casacore::TiledColumnStMan;
using IPos = casacore::IPosition;

using namespace std::string_literals;

static constexpr std::size_t knrow = 10;
static constexpr std::size_t knchan = 4;
static constexpr std::size_t kncorr = 1;

template <typename T> ScalarColumn<T>
GetScalarColumn(const MS & ms, MSColumns column) {
    return ScalarColumn<T>(TableColumn(ms, MS::columnName(column)));
}

template <typename T> ScalarColumn<T>
GetScalarColumn(const MS & ms, const std::string & column) {
    return ScalarColumn<T>(TableColumn(ms, column));
}

template <typename T> ArrayColumn<T>
GetArrayColumn(const MS & ms, MSColumns column) {
    return ArrayColumn<T>(TableColumn(ms, MS::columnName(column)));
}

template <typename T> ArrayColumn<T>
GetArrayColumn(const MS & ms, const std::string & column) {
  return ArrayColumn<T>(TableColumn(ms, column));
}

class ColumnConvertTest : public ::testing::Test {
  protected:
    std::shared_ptr<arcae::SafeTableProxy> table_proxy_;
    std::string table_name_;

    void SetUp() override {
      auto factory = [this]() -> arrow::Result<std::shared_ptr<TableProxy>> {
        auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        table_name_ = std::string(test_info->name() + "-"s + arcae::hexuuid(4) + ".table"s);

        auto table_desc = TableDesc(MS::requiredTableDesc());
        auto data_shape = IPos({kncorr, knchan});
        auto tile_shape = IPos({kncorr, knchan, 1});
        auto data_column_desc = ArrayColumnDesc<CasaComplex>(
            "MODEL_DATA", data_shape, ColumnDesc::FixedShape);

        auto corrected_column_desc = ArrayColumnDesc<CasaComplex>(
            "VAR_DATA", 2);

        table_desc.addColumn(data_column_desc);
        table_desc.addColumn(corrected_column_desc);
        auto storage_manager = TiledColumnStMan("TiledModelData", tile_shape);
        auto setup_new_table = SetupNewTable(table_name_, table_desc, Table::New);
        setup_new_table.bindColumn("MODEL_DATA", storage_manager);
        auto ms = MS(setup_new_table, knrow);

        auto field = GetScalarColumn<int>(ms, MS::FIELD_ID);
        auto ddid = GetScalarColumn<int>(ms, MS::DATA_DESC_ID);
        auto scan = GetScalarColumn<int>(ms, MS::SCAN_NUMBER);
        auto time = GetScalarColumn<double>(ms, MS::TIME);
        auto ant1 = GetScalarColumn<int>(ms, MS::ANTENNA1);
        auto ant2 = GetScalarColumn<int>(ms, MS::ANTENNA2);
        auto data = GetArrayColumn<CasaComplex>(ms, MS::MODEL_DATA);
        auto corrected_data = GetArrayColumn<CasaComplex>(ms, "VAR_DATA");

        time.putColumn({0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
        field.putColumn({0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        ddid.putColumn({0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        ant1.putColumn({0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        ant2.putColumn({1, 1, 1, 1, 1, 1, 1, 1, 1, 1});
        data.putColumn(Array<CasaComplex>(IPos({kncorr, knchan, knrow}), {1, 2}));

        auto rd = std::random_device{};
        auto gen = std::mt19937{rd()};
        auto chan_dist = std::uniform_int_distribution<>(knchan, knchan + 2);
        auto corr_dist = std::uniform_int_distribution<>(kncorr, kncorr + 1);

        for(std::size_t i=0; i < knrow; ++i) {
          auto array = Array<CasaComplex>(
                  IPos({corr_dist(gen), chan_dist(gen), 1}),
                  {static_cast<float>(i), static_cast<float>(i)});

          corrected_data.putColumnCells(casacore::RefRows(i, i), array);
        }

        return std::make_shared<TableProxy>(ms);
      };

      ASSERT_OK_AND_ASSIGN(table_proxy_, arcae::SafeTableProxy::Make(factory));
    }
};

TEST_F(ColumnConvertTest, SelectFromRange) {
  auto result = table_proxy_->run([](const TableProxy & proxy) -> arrow::Result<std::shared_ptr<arrow::Array>> {
    auto table = proxy.table();
    // auto data_column = GetArrayColumn<CasaComplex>(table, "VAR_DATA");
    // auto row_range = Slicer(IPos({0}), IPos({5}), Slicer::endIsLast);
    // auto section = Slicer(IPos({0, 0}), IPos({0, 2}), Slicer::endIsLast);
    // auto data = data_column.getColumnRange(row_range, section);

    auto data_column = GetScalarColumn<casacore::Int>(table, "ANTENNA1");

    using CM = arcae::ColumnMapping<casacore::rownr_t>;
    auto row_ids = CM::ColumnIds(data_column.nrow(), 0);

    for(std::size_t i=0; i < row_ids.size(); ++i) {
      row_ids[i] = i;
    }

    auto column_map = CM{CM::ColumnSelection{std::move(row_ids)}};
    auto visitor = arcae::NewConvertVisitor(data_column, column_map);
    ARROW_RETURN_NOT_OK(visitor.Visit(data_column.columnDesc().dataType()));
    return visitor.array_;
  });

  ASSERT_OK_AND_ASSIGN(auto blah, result);
}
