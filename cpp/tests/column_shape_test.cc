#include "arcae/column_shape.h"

#include <arrow/testing/gtest_util.h>

#include <casacore/casa/Utilities/DataType.h>
#include <casacore/casa/aipsxtype.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/BasicSL/Complexfwd.h>
#include <casacore/tables/Tables/ArrColDesc.h>
#include <casacore/tables/Tables/RefRows.h>
#include <casacore/tables/Tables/TableProxy.h>
#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/TableColumn.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "arcae/safe_table_proxy.h"
#include "arcae/selection.h"

#include <tests/test_utils.h>


using ::arcae::detail::Selection;
using ::arcae::detail::SelectionBuilder;
using ::arcae::detail::ColumnShapeData;

using casacore::Array;
using casacore::ArrayColumn;
using casacore::ArrayColumnDesc;
using casacore::ColumnDesc;
using casacore::Complex;
using casacore::DataType;
using MS = casacore::MeasurementSet;
using MSColumns = casacore::MSMainEnums::PredefinedColumns;
using casacore::Record;
using casacore::SetupNewTable;
using casacore::ScalarColumn;
using casacore::Table;
using casacore::TableDesc;
using casacore::TableColumn;
using casacore::TableLock;
using casacore::TableProxy;
using casacore::TiledColumnStMan;
using IPos = casacore::IPosition;

using namespace std::string_literals;

static constexpr std::size_t knrow = 10;
static constexpr std::size_t knchan = 4;
static constexpr std::size_t kncorr = 2;

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

class ColumnShapeTest : public ::testing::Test {
  protected:
    TableProxy table_proxy_;
    std::string table_name_;
    std::size_t nelements_;

    void SetUp() override {
      auto factory = [this]() -> arrow::Result<std::shared_ptr<TableProxy>> {
        auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        table_name_ = std::string(test_info->name() + "-"s + arcae::hexuuid(4) + ".table"s);

        auto table_desc = TableDesc(MS::requiredTableDesc());
        auto data_shape = IPos({kncorr, knchan});
        auto tile_shape = IPos({kncorr, knchan, 1});
        auto data_column_desc = ArrayColumnDesc<Complex>(
            "MODEL_DATA", data_shape, ColumnDesc::FixedShape);

        auto var_column_desc = ArrayColumnDesc<Complex>(
            "VAR_DATA", 2);

        auto var_fixed_column_desc = ArrayColumnDesc<Complex>(
            "VAR_FIXED_DATA", 2);

        table_desc.addColumn(data_column_desc);
        table_desc.addColumn(var_column_desc);
        table_desc.addColumn(var_fixed_column_desc);
        auto storage_manager = TiledColumnStMan("TiledModelData", tile_shape);
        auto setup_new_table = SetupNewTable(table_name_, table_desc, Table::New);
        setup_new_table.bindColumn("MODEL_DATA", storage_manager);
        auto ms = MS(setup_new_table, knrow);

        auto field = GetScalarColumn<casacore::Int>(ms, MS::FIELD_ID);
        auto ddid = GetScalarColumn<casacore::Int>(ms, MS::DATA_DESC_ID);
        auto scan = GetScalarColumn<casacore::Int>(ms, MS::SCAN_NUMBER);
        auto time = GetScalarColumn<casacore::Double>(ms, MS::TIME);
        auto ant1 = GetScalarColumn<casacore::Int>(ms, MS::ANTENNA1);
        auto ant2 = GetScalarColumn<casacore::Int>(ms, MS::ANTENNA2);
        auto data = GetArrayColumn<Complex>(ms, MS::MODEL_DATA);
        auto var_data = GetArrayColumn<Complex>(ms, "VAR_DATA");
        auto var_fixed_data = GetArrayColumn<Complex>(ms, "VAR_FIXED_DATA");

        time.putColumn({0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
        field.putColumn({0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        ddid.putColumn({0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        ant1.putColumn({0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        ant2.putColumn({1, 1, 1, 1, 1, 1, 1, 1, 1, 1});
        data.putColumn(Array<Complex>(IPos({kncorr, knchan, knrow}), {1, 2}));
        var_fixed_data.putColumn(Array<Complex>(IPos({kncorr, knchan, knrow}), {1, 2}));

        auto varshapes = std::vector<IPos>{
          {3, 2, 1}, {4, 1, 1}, {4, 2, 1}, {2, 2, 1}, {2, 1, 1},
          {3, 2, 1}, {4, 1, 1}, {4, 2, 1}, {2, 2, 1}, {2, 1, 1}};

        assert(varshapes.size() == knrow);

        nelements_ = std::accumulate(std::begin(varshapes), std::end(varshapes), std::size_t{0},
                                     [](auto init, auto & shape) -> std::size_t
                                      { return init + shape.product(); });

        for(std::size_t i=0; i < knrow; ++i) {
          auto corrected_array = Array<Complex>(
                  varshapes[i],
                  {static_cast<float>(i), static_cast<float>(i)});

          var_data.putColumnCells(casacore::RefRows(i, i), corrected_array);
        }

        return std::make_shared<TableProxy>(ms);
      };

      ASSERT_OK_AND_ASSIGN(auto stp, arcae::SafeTableProxy::Make(factory));
      stp.reset();

      auto lock = TableLock(TableLock::LockOption::AutoNoReadLocking);
      auto lockoptions = Record();
      lockoptions.define("option", "auto");
      lockoptions.define("internal", lock.interval());
      lockoptions.define("maxwait", casacore::Int(lock.maxWait()));
      table_proxy_ = TableProxy(table_name_, lockoptions, Table::Old);
    }
};

namespace {

TEST_F(ColumnShapeTest, Fixed) {
  auto fixed = GetArrayColumn<Complex>(table_proxy_.table(), "MODEL_DATA");
  ASSERT_OK_AND_ASSIGN(auto shape_data, ColumnShapeData::Make(fixed));

  EXPECT_EQ(shape_data.GetName(), "MODEL_DATA");
  EXPECT_TRUE(shape_data.IsFixed());
  EXPECT_EQ(shape_data.GetShape(), IPos({2, 4, 10}));
  EXPECT_EQ(shape_data.nDim(), 3);
  EXPECT_EQ(shape_data.GetDataType(), DataType::TpComplex);
}

TEST_F(ColumnShapeTest, FixedSelection) {
  auto fixed = GetArrayColumn<Complex>(table_proxy_.table(), "MODEL_DATA");
  auto sel = SelectionBuilder::FromInit({{0, 1}});
  ASSERT_OK_AND_ASSIGN(auto shape_data, ColumnShapeData::Make(fixed, sel));

  EXPECT_EQ(shape_data.GetName(), "MODEL_DATA");
  EXPECT_TRUE(shape_data.IsFixed());
  EXPECT_EQ(shape_data.GetShape(), IPos({2, 4, 2}));
  EXPECT_EQ(shape_data.nDim(), 3);
  EXPECT_EQ(shape_data.GetDataType(), DataType::TpComplex);

  sel = SelectionBuilder::FromInit({{0, 1}, {0, 1}, {0}});
  ASSERT_OK_AND_ASSIGN(shape_data, ColumnShapeData::Make(fixed, sel));

  EXPECT_EQ(shape_data.GetName(), "MODEL_DATA");
  EXPECT_TRUE(shape_data.IsFixed());
  EXPECT_EQ(shape_data.GetShape(), IPos({1, 2, 2}));
  EXPECT_EQ(shape_data.nDim(), 3);
  EXPECT_EQ(shape_data.GetDataType(), DataType::TpComplex);
}

TEST_F(ColumnShapeTest, Variable) {
  auto var = GetArrayColumn<Complex>(table_proxy_.table(), "VAR_DATA");
  ASSERT_OK_AND_ASSIGN(auto shape_data, ColumnShapeData::Make(var));

  EXPECT_EQ(shape_data.GetName(), "VAR_DATA");
  EXPECT_FALSE(shape_data.IsFixed());
  EXPECT_EQ(shape_data.nDim(), 3);
  EXPECT_EQ(shape_data.GetRowShape(0), IPos({3, 2}));
  EXPECT_EQ(shape_data.GetRowShape(9), IPos({2, 1}));
  EXPECT_EQ(shape_data.GetDataType(), DataType::TpComplex);
}

TEST_F(ColumnShapeTest, VariableSelection) {
  auto var = GetArrayColumn<Complex>(table_proxy_.table(), "VAR_DATA");
  auto sel = SelectionBuilder::FromInit({{0, 1}});
  ASSERT_OK_AND_ASSIGN(auto shape_data, ColumnShapeData::Make(var, sel));

  EXPECT_EQ(shape_data.GetName(), "VAR_DATA");
  EXPECT_FALSE(shape_data.IsFixed());
  EXPECT_EQ(shape_data.GetRowShape(0), IPos({3, 2}));
  EXPECT_EQ(shape_data.GetRowShape(1), IPos({4, 1}));
  EXPECT_EQ(shape_data.GetDataType(), DataType::TpComplex);

  sel = SelectionBuilder::FromInit({{0, 9}, {0}});
  ASSERT_OK_AND_ASSIGN(shape_data, ColumnShapeData::Make(var, sel));
  EXPECT_EQ(shape_data.GetName(), "VAR_DATA");
  EXPECT_FALSE(shape_data.IsFixed());
  EXPECT_EQ(shape_data.GetRowShape(0), IPos({3, 1}));
  EXPECT_EQ(shape_data.GetRowShape(1), IPos({2, 1}));
  EXPECT_EQ(shape_data.GetDataType(), DataType::TpComplex);

  // Fixed shape selection over variably shaped data
  sel = SelectionBuilder::FromInit({{0, 9}, {0}, {0, 1}});
  ASSERT_OK_AND_ASSIGN(shape_data, ColumnShapeData::Make(var, sel));
  EXPECT_EQ(shape_data.GetName(), "VAR_DATA");
  EXPECT_TRUE(shape_data.IsFixed());
  EXPECT_EQ(shape_data.GetShape(), IPos({2, 1, 2}));
  EXPECT_EQ(shape_data.GetDataType(), DataType::TpComplex);
}

} // namespace