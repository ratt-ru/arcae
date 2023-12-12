#include <memory>
#include <string>

#include <arrow/result.h>
#include <arrow/ipc/json_simple.h>
#include <arrow/testing/gtest_util.h>

#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>
#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/ArrColDesc.h>
#include <casacore/tables/Tables/Table.h>
#include <casacore/tables/Tables/TableColumn.h>
#include <casacore/tables/Tables/TableProxy.h>
#include <casacore/tables/Tables/RefRows.h>
#include <tests/test_utils.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "arcae/safe_table_proxy.h"
#include "arcae/column_mapper.h"
#include "arcae/column_read_visitor.h"
#include "arcae/column_write_visitor.h"
#include "arrow/type_fwd.h"


using arrow::ipc::internal::json::ArrayFromJSON;

using casacore::Array;
using casacore::ArrayColumn;
using casacore::ArrayColumnDesc;
using casacore::ColumnDesc;
using MS = casacore::MeasurementSet;
using MSColumns = casacore::MSMainEnums::PredefinedColumns;
using casacore::SetupNewTable;
using casacore::ScalarColumn;
using casacore::Table;
using casacore::TableDesc;
using casacore::TableColumn;
using casacore::TableProxy;
using casacore::TiledColumnStMan;
using IPos = casacore::IPosition;

using arcae::ColumnMapping;
using arcae::ColumnReadVisitor;
using arcae::ColumnWriteVisitor;

using namespace std::string_literals;

static constexpr std::size_t knrow = 2;
static constexpr std::size_t knchan = 2;
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

class ColumnWriteTest : public ::testing::Test {
  protected:
    casacore::TableProxy table_proxy_;
    std::string table_name_;

    void SetUp() override {
      auto factory = [this]() -> arrow::Result<std::shared_ptr<TableProxy>> {
        auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        table_name_ = std::string(test_info->name() + "-"s + arcae::hexuuid(4) + ".table"s);

        auto table_desc = TableDesc(MS::requiredTableDesc());
        auto data_shape = IPos({kncorr, knchan});
        auto tile_shape = IPos({kncorr, knchan, 1});

        auto fixed_column_desc = ArrayColumnDesc<casacore::Int>(
          "FIXED_DATA", data_shape, ColumnDesc::FixedShape);
        table_desc.addColumn(fixed_column_desc);
        auto var_column_desc = ArrayColumnDesc<casacore::Int>(
          "VAR_DATA", 2);
        table_desc.addColumn(var_column_desc);
        auto var_fixed_column_desc = ArrayColumnDesc<casacore::Int>(
          "VAR_FIXED_DATA", 2);
        table_desc.addColumn(var_fixed_column_desc);

        auto fixed_complex_column_desc = ArrayColumnDesc<casacore::DComplex>(
          "FIXED_COMPLEX", data_shape, ColumnDesc::FixedShape);
        table_desc.addColumn(fixed_complex_column_desc);
        auto var_complex_column_desc = ArrayColumnDesc<casacore::DComplex>(
          "VAR_COMPLEX", 2);
        table_desc.addColumn(var_complex_column_desc);
        auto var_fixed_complex_column_desc = ArrayColumnDesc<casacore::DComplex>(
          "VAR_FIXED_COMPLEX", 2);
        table_desc.addColumn(var_fixed_complex_column_desc);

        auto string_column_desc = ArrayColumnDesc<casacore::String>(
          "FIXED_STRING", data_shape, ColumnDesc::FixedShape);
        table_desc.addColumn(string_column_desc);
        auto var_string_column_desc = ArrayColumnDesc<casacore::String>(
          "VAR_STRING", 2);
        table_desc.addColumn(var_string_column_desc);
        auto var_fixed_string_column_desc = ArrayColumnDesc<casacore::String>(
          "VAR_FIXED_STRING", 2);
        table_desc.addColumn(var_fixed_string_column_desc);

        auto storage_manager = TiledColumnStMan("TiledModelData", tile_shape);
        auto setup_new_table = SetupNewTable(table_name_, table_desc, Table::New);
        setup_new_table.bindColumn("FIXED_DATA", storage_manager);

        auto ms = MS(setup_new_table, knrow);

        auto var_data = GetArrayColumn<casacore::Int>(ms, "VAR_DATA");
        auto fixed_data = GetArrayColumn<casacore::Int>(ms, "FIXED_DATA");
        auto var_fixed_data = GetArrayColumn<casacore::Int>(ms, "VAR_FIXED_DATA");

        auto var_complex = GetArrayColumn<casacore::DComplex>(ms, "VAR_COMPLEX");
        auto fixed_complex = GetArrayColumn<casacore::DComplex>(ms, "FIXED_COMPLEX");
        auto var_fixed_complex = GetArrayColumn<casacore::DComplex>(ms, "VAR_FIXED_COMPLEX");

        auto string_data = GetArrayColumn<casacore::String>(ms, "FIXED_STRING");
        auto var_string_data = GetArrayColumn<casacore::String>(ms, "VAR_STRING");
        auto var_fixed_string_data = GetArrayColumn<casacore::String>(ms, "VAR_FIXED_STRING");

        for(auto [r, v] = std::tuple{ssize_t{0}, std::size_t{0}}; r < knrow; ++r) {
          auto var_array = Array<casacore::Int>(IPos({
            ssize_t{kncorr} - (r % 2), ssize_t{knchan} - (r % 2), 1}), 0);
          auto var_complex_array = Array<casacore::DComplex>(IPos({
            ssize_t{kncorr} - (r % 2), ssize_t{knchan} - (r % 2), 1}), 0);
          auto var_string = Array<casacore::String>(IPos({
            ssize_t{kncorr} - (r % 2), ssize_t{knchan} - (r % 2), 1}));
          var_data.putColumnCells(casacore::RefRows(r, r), var_array);
          var_complex.putColumnCells(casacore::RefRows(r, r), var_complex_array);
          var_string_data.putColumnCells(casacore::RefRows(r, r), var_string);
        }

        for(auto [r, v] = std::tuple{ssize_t{0}, std::size_t{0}}; r < knrow; ++r) {
          auto fixed_array = Array<casacore::Int>(IPos({kncorr, knchan, 1}), 0);
          fixed_data.putColumnCells(casacore::RefRows(r, r), fixed_array);
          var_fixed_data.putColumnCells(casacore::RefRows(r, r), fixed_array);
        }

        for(auto [r, v] = std::tuple{ssize_t{0}, std::size_t{0}}; r < knrow; ++r) {
          auto string_array = Array<casacore::String>(IPos({kncorr, knchan, 1}));
          string_data.putColumnCells(casacore::RefRows(r, r), string_array);
          var_fixed_string_data.putColumnCells(casacore::RefRows(r, r), string_array);
        }

        return std::make_shared<TableProxy>(ms);
      };

      ASSERT_OK_AND_ASSIGN(auto stp, arcae::SafeTableProxy::Make(factory));
      stp.reset();

      auto lock = casacore::TableLock(casacore::TableLock::LockOption::AutoNoReadLocking);
      auto lockoptions = casacore::Record();
      lockoptions.define("option", "auto");
      lockoptions.define("internal", lock.interval());
      lockoptions.define("maxwait", casacore::Int(lock.maxWait()));
      table_proxy_ = casacore::TableProxy(table_name_, lockoptions, casacore::Table::Old);
      table_proxy_.reopenRW();

    }
};

TEST_F(ColumnWriteTest, WriteVisitorFixedNumeric) {
  const auto & table = table_proxy_.table();
  const auto shape = IPos{kncorr, knchan, knrow};

  for(auto & column: {"FIXED_DATA", "VAR_FIXED_DATA"}) {
    using CT = casacore::Int;
    auto fixed = GetArrayColumn<CT>(table, column);
    auto zeroes = casacore::Array<CT>(shape, 0);

    {
      // Fixed data column, get entire domain
      fixed.putColumn(zeroes);
      ASSERT_OK_AND_ASSIGN(auto column_map, (ColumnMapping::Make(fixed, {})));

      auto dtype = arrow::fixed_size_list(
                    arrow::fixed_size_list(
                      arrow::fixed_size_list(
                        arrow::int32(), 2), 2), 2);
      ASSERT_OK_AND_ASSIGN(auto data,
                           ArrayFromJSON(dtype,
                                         R"([[[[0, 1], [2, 3]], [[4, 5], [6, 7]]]])"));

      auto visitor = ColumnWriteVisitor(column_map, data);
      auto visit_status = visitor.Visit(fixed.columnDesc().dataType());
      ASSERT_OK(visit_status);

      auto read_visitor = ColumnReadVisitor(column_map);
      visit_status = read_visitor.Visit(fixed.columnDesc().dataType());
      ASSERT_OK(visit_status);
      ASSERT_TRUE(data->Equals(read_visitor.array_));

      EXPECT_THAT(fixed.getColumn(),  ::testing::ElementsAre(
        CT{0}, CT{1}, CT{2}, CT{3},
        CT{4}, CT{5}, CT{6}, CT{7}));
    }

    {
      // Fixed data column, get all rows, first channel and correlation
      fixed.putColumn(zeroes);
      ASSERT_OK_AND_ASSIGN(auto column_map, (ColumnMapping::Make(fixed, {{}, {0}, {0}})));

      auto dtype = arrow::fixed_size_list(
                    arrow::fixed_size_list(
                      arrow::fixed_size_list(
                        arrow::int32(), 1), 1), 2);
      ASSERT_OK_AND_ASSIGN(auto data, ArrayFromJSON(dtype, R"([[[[0]], [[4]]]])"));

      auto write_visitor = ColumnWriteVisitor(column_map, data);
      auto visit_status = write_visitor.Visit(fixed.columnDesc().dataType());
      ASSERT_OK(visit_status);

      auto read_visitor = ColumnReadVisitor(column_map);
      visit_status = read_visitor.Visit(fixed.columnDesc().dataType());
      ASSERT_OK(visit_status);
      ASSERT_TRUE(data->Equals(read_visitor.array_));

      EXPECT_THAT(fixed.getColumn(),  ::testing::ElementsAre(
        CT{0}, CT{0}, CT{0}, CT{0},
        CT{4}, CT{0}, CT{0}, CT{0}));

    }

    {
      fixed.putColumn(zeroes);
      // Fixed data column, get all rows, last channel and correlation
      ASSERT_OK_AND_ASSIGN(auto column_map, (ColumnMapping::Make(fixed, {{}, {1}, {1}})));
      auto dtype = arrow::fixed_size_list(
                    arrow::fixed_size_list(
                      arrow::fixed_size_list(
                        arrow::int32(), 1), 1), 2);
      ASSERT_OK_AND_ASSIGN(auto data, ArrayFromJSON(dtype, R"([[[[3]], [[7]]]])"));

      auto write_visitor = ColumnWriteVisitor(column_map, data);
      auto visit_status = write_visitor.Visit(fixed.columnDesc().dataType());
      ASSERT_OK(visit_status);

      auto read_visitor = ColumnReadVisitor(column_map);
      visit_status = read_visitor.Visit(fixed.columnDesc().dataType());
      ASSERT_OK(visit_status);
      ASSERT_TRUE(data->Equals(read_visitor.array_));

      EXPECT_THAT(fixed.getColumn(),  ::testing::ElementsAre(
        CT{0}, CT{0}, CT{0}, CT{3},
        CT{0}, CT{0}, CT{0}, CT{7}));
    }
  }
}

TEST_F(ColumnWriteTest, WriteVisitorFixedComplex) {
  const auto & table = table_proxy_.table();
  const auto shape = IPos{kncorr, knchan, knrow};

  auto fixed = GetArrayColumn<casacore::DComplex>(table, "FIXED_COMPLEX");
  auto zeroes = casacore::Array<casacore::DComplex>(shape, 0);

  {
    // Fixed data column, get entire domain
    fixed.putColumn(zeroes);
    ASSERT_OK_AND_ASSIGN(auto column_map, (ColumnMapping::Make(fixed, {})));

    auto dtype = arrow::fixed_size_list(
                  arrow::fixed_size_list(
                    arrow::fixed_size_list(
                      arrow::fixed_size_list(
                        arrow::float64(), 2), 2), 2), 2);
    ASSERT_OK_AND_ASSIGN(auto data,
                          ArrayFromJSON(dtype,
                                        R"([[[[[0, 0], [1, 1]], [[2, 2], [3, 3]]],
                                             [[[4, 4], [5, 5]], [[6, 6], [7, 7]]]]])"));

    auto visitor = ColumnWriteVisitor(column_map, data);
    auto visit_status = visitor.Visit(fixed.columnDesc().dataType());
    ASSERT_OK(visit_status);

    auto read_visitor = ColumnReadVisitor(column_map);
    visit_status = read_visitor.Visit(fixed.columnDesc().dataType());
    ASSERT_OK(visit_status);
    ASSERT_TRUE(data->Equals(read_visitor.array_));

    // Sanity check the values via casacore
    using CT = casacore::DComplex;
    EXPECT_THAT(fixed.getColumn(),  ::testing::ElementsAre(
      CT{0, 0}, CT{1, 1}, CT{2, 2}, CT{3, 3},
      CT{4, 4}, CT{5, 5}, CT{6, 6}, CT{7, 7}));
  }
}

TEST_F(ColumnWriteTest, WriteVisitorFixedString) {
  const auto & table = table_proxy_.table();
  const auto shape = IPos{kncorr, knchan, knrow};

  for(auto & column: {"FIXED_STRING", "VAR_FIXED_STRING"}) {
    using CT = casacore::String;
    auto fixed = GetArrayColumn<CT>(table, column);
    auto zeroes = casacore::Array<CT>(shape, CT{""});

    {
      // Fixed data column, get entire domain
      fixed.putColumn(zeroes);
      ASSERT_OK_AND_ASSIGN(auto column_map, (ColumnMapping::Make(fixed, {})));

      auto dtype = arrow::fixed_size_list(
                    arrow::fixed_size_list(
                      arrow::fixed_size_list(
                        arrow::utf8(), 2), 2), 2);
      ASSERT_OK_AND_ASSIGN(auto data, ArrayFromJSON(
                           dtype,
                           R"([[[["0", "1"], ["2", "3"]], [["4", "5"], ["6", "7"]]]])"));

      auto visitor = ColumnWriteVisitor(column_map, data);
      auto visit_status = visitor.Visit(fixed.columnDesc().dataType());
      ASSERT_OK(visit_status);

      auto read_visitor = ColumnReadVisitor(column_map);
      visit_status = read_visitor.Visit(fixed.columnDesc().dataType());
      ASSERT_OK(visit_status);
      ASSERT_TRUE(data->Equals(read_visitor.array_));

      // Sanity check the values via casacore
      EXPECT_THAT(fixed.getColumn(),  ::testing::ElementsAre(
        CT{"0"}, CT{"1"}, CT{"2"}, CT{"3"},
        CT{"4"}, CT{"5"}, CT{"6"}, CT{"7"}));
    }

    {
      // Fixed data column, get all rows, first channel and correlation
      fixed.putColumn(zeroes);
      ASSERT_OK_AND_ASSIGN(auto column_map, (ColumnMapping::Make(fixed, {{}, {0}, {0}})));

      auto dtype = arrow::fixed_size_list(
                    arrow::fixed_size_list(
                      arrow::fixed_size_list(
                        arrow::utf8(), 1), 1), 2);
      ASSERT_OK_AND_ASSIGN(auto data, ArrayFromJSON(dtype, R"([[[["0"]], [["4"]]]])"));

      auto write_visitor = ColumnWriteVisitor(column_map, data);
      auto visit_status = write_visitor.Visit(fixed.columnDesc().dataType());
      ASSERT_OK(visit_status);

      auto read_visitor = ColumnReadVisitor(column_map);
      visit_status = read_visitor.Visit(fixed.columnDesc().dataType());
      ASSERT_OK(visit_status);
      ASSERT_TRUE(data->Equals(read_visitor.array_));

      // Sanity check values via casacore
      EXPECT_THAT(fixed.getColumn(),  ::testing::ElementsAre(
        CT{"0"}, CT{""}, CT{""}, CT{""},
        CT{"4"}, CT{""}, CT{""}, CT{""}));
    }

    {
      fixed.putColumn(zeroes);
      // Fixed data column, get all rows, last channel and correlation
      ASSERT_OK_AND_ASSIGN(auto column_map, (ColumnMapping::Make(fixed, {{}, {1}, {1}})));
      auto dtype = arrow::fixed_size_list(
                    arrow::fixed_size_list(
                      arrow::fixed_size_list(
                        arrow::utf8(), 1), 1), 2);
      ASSERT_OK_AND_ASSIGN(auto data, ArrayFromJSON(dtype, R"([[[["3"]], [["7"]]]])"));

      auto write_visitor = ColumnWriteVisitor(column_map, data);
      auto visit_status = write_visitor.Visit(fixed.columnDesc().dataType());
      ASSERT_OK(visit_status);

      auto read_visitor = ColumnReadVisitor(column_map);
      visit_status = read_visitor.Visit(fixed.columnDesc().dataType());
      ASSERT_OK(visit_status);
      ASSERT_TRUE(data->Equals(read_visitor.array_));

      // Sanity check values via casacore
      EXPECT_THAT(fixed.getColumn(),  ::testing::ElementsAre(
        CT{""}, CT{""}, CT{""}, CT{"3"},
        CT{""}, CT{""}, CT{""}, CT{"7"}));

    }
  }
}


TEST_F(ColumnWriteTest, WriteVisitorVariableNumeric) {
  const auto & table = table_proxy_.table();
  using CT = casacore::Int;

  for(auto & column: {"VAR_DATA"}) {
    {
      auto dtype = arrow::list(arrow::list(arrow::int32()));
      ASSERT_OK_AND_ASSIGN(auto data,
                           ArrayFromJSON(dtype,
                                         R"([[[0, 1], [2, 3]], [[4]]])"));

      // Variable data column, get entire domain
      auto var = GetArrayColumn<CT>(table, column);
      ASSERT_OK_AND_ASSIGN(auto column_map, (ColumnMapping::Make(var, {})));
      auto write_visitor = ColumnWriteVisitor(column_map, data);
      auto visit_status = write_visitor.Visit(var.columnDesc().dataType());
      ASSERT_OK(visit_status);

      auto read_visitor = ColumnReadVisitor(column_map);
      visit_status = read_visitor.Visit(var.columnDesc().dataType());
      ASSERT_OK(visit_status);
      ASSERT_TRUE(data->Equals(read_visitor.array_));

      // Sanity check values via casacore
      EXPECT_THAT(var.getColumnCells(casacore::RefRows(0, 0)),
                  ::testing::ElementsAre(CT{0}, CT{1}, CT{2}, CT{3}));
      EXPECT_THAT(var.getColumnCells(casacore::RefRows(1, 1)),
                  ::testing::ElementsAre(CT{4}));

    }
  }
}

TEST_F(ColumnWriteTest, WriteVisitorVariableString) {
  const auto & table = table_proxy_.table();
  using CT = casacore::String;

  for(auto & column: {"VAR_STRING"}) {
    {
      auto dtype = arrow::list(arrow::list(arrow::utf8()));
      ASSERT_OK_AND_ASSIGN(auto data,
                           ArrayFromJSON(dtype,
                                         R"([[["0", "1"], ["2", "3"]], [["4"]]])"));

      // Variable data column, get entire domain
      auto var = GetArrayColumn<CT>(table, column);
      ASSERT_OK_AND_ASSIGN(auto column_map, (ColumnMapping::Make(var, {})));
      auto write_visitor = ColumnWriteVisitor(column_map, data);
      auto visit_status = write_visitor.Visit(var.columnDesc().dataType());
      ASSERT_OK(visit_status);

      auto read_visitor = ColumnReadVisitor(column_map);
      visit_status = read_visitor.Visit(var.columnDesc().dataType());
      ASSERT_OK(visit_status);
      ASSERT_TRUE(data->Equals(read_visitor.array_));

      // Sanity check values via casacore
      EXPECT_THAT(var.getColumnCells(casacore::RefRows(0, 0)),
                  ::testing::ElementsAre(CT{"0"}, CT{"1"}, CT{"2"}, CT{"3"}));
      EXPECT_THAT(var.getColumnCells(casacore::RefRows(1, 1)),
                  ::testing::ElementsAre(CT{"4"}));

    }
  }
}
