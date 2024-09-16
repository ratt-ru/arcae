#include <arrow/ipc/json_simple.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/type_fwd.h>

#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/BasicSL/Complexfwd.h>
#include <casacore/casa/Utilities/DataType.h>
#include <casacore/casa/aipsxtype.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>
#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/ArrColDesc.h>
#include <casacore/tables/Tables/RefRows.h>
#include <casacore/tables/Tables/TableColumn.h>
#include <casacore/tables/Tables/TableProxy.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arcae/result_shape.h"
#include "arcae/selection.h"

#include <tests/test_utils.h>

using ::arcae::GetArrayColumn;
using ::arcae::GetScalarColumn;
using ::arcae::detail::ResultShapeData;
using ::arcae::detail::Selection;
using ::arcae::detail::SelectionBuilder;

using ::arrow::ipc::internal::json::ArrayFromJSON;

using casacore::Array;
using casacore::ArrayColumnDesc;
using casacore::ColumnDesc;
using casacore::Complex;
using casacore::DataType;
using MS = casacore::MeasurementSet;
using MSColumns = casacore::MSMainEnums::PredefinedColumns;
using casacore::Record;
using casacore::SetupNewTable;
using casacore::Table;
using casacore::TableDesc;
using casacore::TableLock;
using casacore::TableProxy;
using casacore::TiledColumnStMan;
using IPos = casacore::IPosition;

using namespace std::string_literals;

static constexpr std::size_t knrow = 10;
static constexpr std::size_t knchan = 4;
static constexpr std::size_t kncorr = 2;

namespace {

class ColumnShapeTest : public ::testing::Test {
 protected:
  TableProxy table_proxy_;
  std::string table_name_;
  std::size_t nelements_;

  void SetUp() override {
    auto* test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    table_name_ = std::string(test_info->name() + "-"s + arcae::hexuuid(4) + ".table"s);

    auto table_desc = TableDesc(MS::requiredTableDesc());
    auto data_shape = IPos({kncorr, knchan});
    auto tile_shape = IPos({kncorr, knchan, 1});
    auto data_column_desc =
        ArrayColumnDesc<Complex>("MODEL_DATA", data_shape, ColumnDesc::FixedShape);

    auto var_column_desc = ArrayColumnDesc<Complex>("VAR_DATA", 2);

    auto var_fixed_column_desc = ArrayColumnDesc<Complex>("VAR_FIXED_DATA", 2);

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

    auto varshapes =
        std::vector<IPos>{{3, 2, 1}, {4, 1, 1}, {4, 2, 1}, {2, 2, 1}, {2, 1, 1},
                          {3, 2, 1}, {4, 1, 1}, {4, 2, 1}, {2, 2, 1}, {2, 1, 1}};

    assert(varshapes.size() == knrow);

    nelements_ = std::accumulate(
        std::begin(varshapes), std::end(varshapes), std::size_t{0},
        [](auto init, auto& shape) -> std::size_t { return init + shape.product(); });

    for (std::size_t i = 0; i < knrow; ++i) {
      auto fv = static_cast<float>(i);
      auto corrected_array = Array<Complex>(varshapes[i], {fv, fv});
      var_data.putColumnCells(casacore::RefRows(i, i), corrected_array);
    }

    table_proxy_ = TableProxy(ms);
  }
};

TEST_F(ColumnShapeTest, ReadFixed) {
  auto fixed = GetArrayColumn<Complex>(table_proxy_.table(), "MODEL_DATA");
  ASSERT_OK_AND_ASSIGN(auto shape_data, ResultShapeData::MakeRead(fixed));

  EXPECT_EQ(shape_data.GetName(), "MODEL_DATA");
  EXPECT_TRUE(shape_data.IsFixed());
  EXPECT_EQ(shape_data.GetShape(), IPos({2, 4, 10}));
  EXPECT_EQ(shape_data.nDim(), 3);
  EXPECT_EQ(shape_data.nRows(), 10);
  EXPECT_EQ(shape_data.nElements(), 80);
  EXPECT_EQ(shape_data.GetDataType(), DataType::TpComplex);
  ASSERT_OK_AND_ASSIGN(auto offsets, shape_data.GetOffsets());
}

TEST_F(ColumnShapeTest, ReadFixedSelection) {
  auto fixed = GetArrayColumn<Complex>(table_proxy_.table(), "MODEL_DATA");
  auto sel = SelectionBuilder::FromInit({{0, 1}});
  ASSERT_OK_AND_ASSIGN(auto shape_data, ResultShapeData::MakeRead(fixed, sel));

  EXPECT_EQ(shape_data.GetName(), "MODEL_DATA");
  EXPECT_TRUE(shape_data.IsFixed());
  EXPECT_EQ(shape_data.GetShape(), IPos({2, 4, 2}));
  EXPECT_EQ(shape_data.nDim(), 3);
  EXPECT_EQ(shape_data.nRows(), 2);
  EXPECT_EQ(shape_data.nElements(), 16);
  EXPECT_EQ(shape_data.GetDataType(), DataType::TpComplex);
  ASSERT_OK_AND_ASSIGN(auto offsets, shape_data.GetOffsets());

  sel = SelectionBuilder::FromInit({{0, 1}, {0, 1}, {0}});
  ASSERT_OK_AND_ASSIGN(shape_data, ResultShapeData::MakeRead(fixed, sel));

  EXPECT_EQ(shape_data.GetName(), "MODEL_DATA");
  EXPECT_TRUE(shape_data.IsFixed());
  EXPECT_EQ(shape_data.GetShape(), IPos({1, 2, 2}));
  EXPECT_EQ(shape_data.nDim(), 3);
  EXPECT_EQ(shape_data.nRows(), 2);
  EXPECT_EQ(shape_data.nElements(), 4);
  EXPECT_EQ(shape_data.GetDataType(), DataType::TpComplex);
  ASSERT_OK_AND_ASSIGN(offsets, shape_data.GetOffsets());
}

TEST_F(ColumnShapeTest, ReadVariable) {
  auto var = GetArrayColumn<Complex>(table_proxy_.table(), "VAR_DATA");
  ASSERT_OK_AND_ASSIGN(auto shape_data, ResultShapeData::MakeRead(var));

  EXPECT_EQ(shape_data.GetName(), "VAR_DATA");
  EXPECT_FALSE(shape_data.IsFixed());
  EXPECT_EQ(shape_data.nDim(), 3);
  EXPECT_EQ(shape_data.nRows(), 10);
  EXPECT_EQ(shape_data.GetRowShape(0), IPos({3, 2}));
  EXPECT_EQ(shape_data.GetRowShape(9), IPos({2, 1}));
  EXPECT_EQ(shape_data.nElements(), nelements_);
  EXPECT_EQ(shape_data.GetDataType(), DataType::TpComplex);
  ASSERT_OK_AND_ASSIGN(auto offsets, shape_data.GetOffsets());
}

TEST_F(ColumnShapeTest, ReadVariableSelection) {
  auto var = GetArrayColumn<Complex>(table_proxy_.table(), "VAR_DATA");
  auto sel = SelectionBuilder::FromInit({{0, 1}});
  ASSERT_OK_AND_ASSIGN(auto shape_data, ResultShapeData::MakeRead(var, sel));

  EXPECT_EQ(shape_data.GetName(), "VAR_DATA");
  EXPECT_FALSE(shape_data.IsFixed());
  EXPECT_EQ(shape_data.nDim(), 3);
  EXPECT_EQ(shape_data.nRows(), 2);
  EXPECT_EQ(shape_data.GetRowShape(0), IPos({3, 2}));
  EXPECT_EQ(shape_data.GetRowShape(1), IPos({4, 1}));
  EXPECT_EQ(shape_data.nElements(), 10);
  EXPECT_EQ(shape_data.GetDataType(), DataType::TpComplex);
  ASSERT_OK_AND_ASSIGN(auto offsets, shape_data.GetOffsets());

  sel = SelectionBuilder::FromInit({{0, 9}, {0}});
  ASSERT_OK_AND_ASSIGN(shape_data, ResultShapeData::MakeRead(var, sel));
  EXPECT_EQ(shape_data.GetName(), "VAR_DATA");
  EXPECT_FALSE(shape_data.IsFixed());
  EXPECT_EQ(shape_data.nDim(), 3);
  EXPECT_EQ(shape_data.nRows(), 2);
  EXPECT_EQ(shape_data.nElements(), 5);
  EXPECT_EQ(shape_data.GetRowShape(0), IPos({3, 1}));
  EXPECT_EQ(shape_data.GetRowShape(1), IPos({2, 1}));
  EXPECT_EQ(shape_data.GetDataType(), DataType::TpComplex);
  ASSERT_OK_AND_ASSIGN(offsets, shape_data.GetOffsets());

  // Fixed shape selection over variably shaped data
  sel = SelectionBuilder::FromInit({{0, 9}, {0}, {0, 1}});
  ASSERT_OK_AND_ASSIGN(shape_data, ResultShapeData::MakeRead(var, sel));
  EXPECT_EQ(shape_data.GetName(), "VAR_DATA");
  EXPECT_TRUE(shape_data.IsFixed());
  EXPECT_EQ(shape_data.nDim(), 3);
  EXPECT_EQ(shape_data.nRows(), 2);
  EXPECT_EQ(shape_data.nElements(), 4);
  EXPECT_EQ(shape_data.GetShape(), IPos({2, 1, 2}));
  EXPECT_EQ(shape_data.GetDataType(), DataType::TpComplex);
  ASSERT_OK_AND_ASSIGN(offsets, shape_data.GetOffsets());
}

TEST_F(ColumnShapeTest, WriteFixed) {
  auto fixed = GetArrayColumn<Complex>(table_proxy_.table(), "MODEL_DATA");
  auto dtype = arrow::fixed_size_list(
      arrow::fixed_size_list(arrow::fixed_size_list(arrow::float32(), 2), 2), 2);
  ASSERT_OK_AND_ASSIGN(auto data, ArrayFromJSON(dtype,
                                                R"([[[[0, 0], [1, 1]],
                                          [[2, 2], [3, 3]]],
                                         [[[4, 4], [5, 5]],
                                          [[6, 6], [7, 7]]]])"));

  ASSERT_OK_AND_ASSIGN(auto shape_data, ResultShapeData::MakeWrite(fixed, data));
  EXPECT_EQ(shape_data.GetName(), "MODEL_DATA");
  EXPECT_TRUE(shape_data.IsFixed());
  EXPECT_EQ(shape_data.nDim(), 3);
  EXPECT_EQ(shape_data.nRows(), 2);
  EXPECT_EQ(shape_data.nElements(), 8);
  EXPECT_EQ(shape_data.GetShape(), IPos({2, 2, 2}));
  EXPECT_EQ(shape_data.GetDataType(), DataType::TpComplex);
  ASSERT_OK_AND_ASSIGN(auto offsets, shape_data.GetOffsets());
}

TEST_F(ColumnShapeTest, WriteVariable) {
  auto var = GetArrayColumn<Complex>(table_proxy_.table(), "VAR_DATA");
  auto dtype = arrow::list(arrow::list(arrow::list(arrow::float32())));

  // Supplied as a list, but actually fixed
  ASSERT_OK_AND_ASSIGN(auto data, ArrayFromJSON(dtype,
                                                R"([[[[0, 0], [1, 1], [2, 2]],
                                          [[3, 3], [4, 4], [5, 5]]],
                                         [[[6, 6], [7, 7], [8, 8]],
                                          [[9, 9], [10, 10], [11, 11]]]])"));

  ASSERT_OK_AND_ASSIGN(auto shape_data, ResultShapeData::MakeWrite(var, data));
  EXPECT_EQ(shape_data.GetName(), "VAR_DATA");
  EXPECT_TRUE(shape_data.IsFixed());
  EXPECT_EQ(shape_data.nDim(), 3);
  EXPECT_EQ(shape_data.nRows(), 2);
  EXPECT_EQ(shape_data.nElements(), 12);
  EXPECT_EQ(shape_data.GetShape(), IPos({3, 2, 2}));
  EXPECT_EQ(shape_data.GetDataType(), DataType::TpComplex);
  ASSERT_OK_AND_ASSIGN(auto offsets, shape_data.GetOffsets());

  // Variably shaped
  ASSERT_OK_AND_ASSIGN(data, ArrayFromJSON(dtype,
                                           R"([[[[0, 0], [1, 1]],
                                          [[3, 3], [4, 4]]],
                                         [[[6, 6], [7, 7], [8, 8]],
                                          [[9, 9], [10, 10], [11, 11]]]])"));

  ASSERT_OK_AND_ASSIGN(shape_data, ResultShapeData::MakeWrite(var, data));
  EXPECT_EQ(shape_data.GetName(), "VAR_DATA");
  EXPECT_FALSE(shape_data.IsFixed());
  EXPECT_EQ(shape_data.nDim(), 3);
  EXPECT_EQ(shape_data.nRows(), 2);
  EXPECT_EQ(shape_data.nElements(), 10);
  EXPECT_EQ(shape_data.GetDataType(), DataType::TpComplex);
  EXPECT_EQ(shape_data.GetRowShape(0), IPos({2, 2}));
  EXPECT_EQ(shape_data.GetRowShape(1), IPos({3, 2}));
  ASSERT_OK_AND_ASSIGN(offsets, shape_data.GetOffsets());
}

}  // namespace
