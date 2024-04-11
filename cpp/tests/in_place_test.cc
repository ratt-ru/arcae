#include <memory>

#include <arrow/api.h>
#include <arrow/ipc/json_simple.h>
#include <arrow/type_fwd.h>
#include <arrow/testing/gtest_util.h>

#include <casacore/casa/aipsxtype.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/ArrayColumn.h>
#include <casacore/tables/Tables/ArrColDesc.h>
#include <casacore/tables/Tables/ColumnDesc.h>
#include <casacore/tables/Tables/SetupNewTab.h>
#include <casacore/tables/Tables/TableProxy.h>

#include <arcae/base_column_map.h>
#include <arcae/safe_table_proxy.h>
#include <arcae/column_read_map.h>
#include <arcae/column_read_visitor.h>
#include <arcae/column_write_map.h>
#include <arcae/column_write_visitor.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <tests/test_utils.h>

using namespace std::string_literals;

static constexpr casacore::rownr_t knrow = 10;

using ::arrow::ipc::internal::json::ArrayFromJSON;

using ::arcae::ColumnReadMap;
using ::arcae::ColumnWriteMap;

using ::casacore::ArrayColumn;
using ::casacore::ArrayColumnDesc;
using ::casacore::ColumnDesc;
using ::casacore::IPosition;
using ::casacore::SetupNewTable;
using ::casacore::TableColumn;
using ::casacore::TableDesc;
using ::casacore::TableLock;
using LockOption = ::casacore::TableLock::LockOption;
using ::casacore::TableProxy;
using ::casacore::Table;

template <typename T> ArrayColumn<T>
GetArrayColumn(const Table & table, const std::string & column) {
  return ArrayColumn<T>(TableColumn(table, column));
}

class InPlaceReadTest : public ::testing::Test {
  protected:
    casacore::TableProxy table_proxy_;
    std::string table_name_;

    void SetUp() override {
      auto factory = [this]() -> arrow::Result<std::shared_ptr<TableProxy>> {
        auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        table_name_ = std::string(test_info->name() + "-"s + arcae::hexuuid(4) + ".table"s);

        auto table_desc = TableDesc{};
        auto varcol_desc = ArrayColumnDesc<casacore::Int>(
          "DATA", 2);
        table_desc.addColumn(varcol_desc);
        auto float_varcol_desc = ArrayColumnDesc<casacore::Float>(
          "FLOAT_DATA", 2);
        table_desc.addColumn(float_varcol_desc);
        auto complex_varcol_desc = ArrayColumnDesc<casacore::Complex>(
          "VAR_COMPLEX", 2);
        table_desc.addColumn(complex_varcol_desc);
        auto fixed_complex_coldesc = ArrayColumnDesc<casacore::Complex>(
          "FIXED_COMPLEX", IPosition({3, 3}), ColumnDesc::FixedShape);
        table_desc.addColumn(fixed_complex_coldesc);

        auto setup_new_table = SetupNewTable(table_name_, table_desc, Table::New);
        auto table = casacore::Table(setup_new_table, knrow);
        return std::make_shared<TableProxy>(table);
      };

      ASSERT_OK_AND_ASSIGN(auto stp, arcae::SafeTableProxy::Make(factory));
      stp.reset();

      auto lock = TableLock(LockOption::AutoNoReadLocking);
      auto lockoptions = casacore::Record();
      lockoptions.define("option", "auto");
      lockoptions.define("internal", lock.interval());
      lockoptions.define("maxwait", casacore::Int(lock.maxWait()));
      table_proxy_ = TableProxy(table_name_, lockoptions, Table::Old);
      table_proxy_.reopenRW();
    }
};

TEST_F(InPlaceReadTest, FloatReadToResult) {
  const auto & table = table_proxy_.table();
  using CT = casacore::Float;

  {
    // Write some fixed shape data to 2 rows
    ASSERT_OK_AND_ASSIGN(auto data,
                         ArrayFromJSON(arrow::fixed_size_list(
                                        arrow::fixed_size_list(arrow::float32(), 3), 3),
                                       R"([[[1, 2, 3],
                                            [0, 0, 0],
                                            [4, 5, 6]],
                                           [[7, 8, 9],
                                            [0, 0, 0],
                                            [10, 11, 12]]])"));

    auto var = GetArrayColumn<CT>(table, "FLOAT_DATA");
    ASSERT_OK_AND_ASSIGN(auto write_map, ColumnWriteMap::Make(var, {{0, 3}}, data));
    auto write_visitor = arcae::ColumnWriteVisitor(write_map);
    ASSERT_OK(write_visitor.Visit());

    EXPECT_THAT(var.getColumnCells(casacore::RefRows(0, 0)),
                ::testing::ElementsAre(CT{1}, CT{2}, CT{3},
                                       CT{0}, CT{0}, CT{0},
                                       CT{4}, CT{5}, CT{6}));

    EXPECT_FALSE(var.isDefined(1));
    EXPECT_FALSE(var.isDefined(2));

    EXPECT_THAT(var.getColumnCells(casacore::RefRows(3, 3)),
                ::testing::ElementsAre(CT{7}, CT{8}, CT{9},
                                       CT{0}, CT{0}, CT{0},
                                       CT{10}, CT{11}, CT{12}));

    {
      // Create a result array and read from the written rows, ignoring two rows
      ASSERT_OK_AND_ASSIGN(auto result,
                          ArrayFromJSON(arrow::fixed_size_list(
                                          arrow::fixed_size_list(arrow::float32(), 3), 3),
                                        R"([[[0, 0, 0],
                                              [0, 0, 0],
                                              [0, 0, 0]],
                                            [[0, 0, 0],
                                              [0, 0, 0],
                                              [0, 0, 0]],
                                            [[0, 0, 0],
                                              [0, 0, 0],
                                              [0, 0, 0]],
                                            [[0, 0, 0],
                                              [0, 0, 0],
                                              [0, 0, 0]]])"));


      ASSERT_OK_AND_ASSIGN(auto read_map, ColumnReadMap::Make(var, {{0, -1, -1, 3}}, result));
      auto read_visitor = arcae::ColumnReadVisitor(read_map);
      ASSERT_OK(read_visitor.Visit());


      ASSERT_OK_AND_ASSIGN(auto expected,
                          ArrayFromJSON(arrow::fixed_size_list(
                                          arrow::fixed_size_list(arrow::float32(), 3), 3),
                                        R"([[[1, 2, 3],
                                              [0, 0, 0],
                                              [4, 5, 6]],
                                            [[0, 0, 0],
                                              [0, 0, 0],
                                              [0, 0, 0]],
                                            [[0, 0, 0],
                                              [0, 0, 0],
                                              [0, 0, 0]],
                                            [[7, 8, 9],
                                              [0, 0, 0],
                                              [10, 11, 12]]])"));

      ASSERT_TRUE(read_visitor.array_->Equals(expected)) << read_visitor.array_->Diff(*expected);
      ASSERT_TRUE(result->Equals(expected)) << result->Diff(*expected);
    }

    {
      ASSERT_OK_AND_ASSIGN(auto result,
                          ArrayFromJSON(arrow::fixed_size_list(
                                          arrow::fixed_size_list(arrow::float32(), 3), 3),
                                        R"([[[0, 0, 0],
                                              [0, 0, 0],
                                              [0, 0, 0]],
                                            [[0, 0, 0],
                                              [0, 0, 0],
                                              [0, 0, 0]],
                                            [[0, 0, 0],
                                              [0, 0, 0],
                                              [0, 0, 0]],
                                            [[0, 0, 0],
                                              [0, 0, 0],
                                              [0, 0, 0]]])"));


      ASSERT_OK_AND_ASSIGN(auto read_map, ColumnReadMap::Make(var, {{0, -1, -1, 3}, {}, {2, -1, 0}}, result));
      auto read_visitor = arcae::ColumnReadVisitor(read_map);
      ASSERT_OK(read_visitor.Visit());

      ASSERT_OK_AND_ASSIGN(auto expected,
                          ArrayFromJSON(arrow::fixed_size_list(
                                          arrow::fixed_size_list(arrow::float32(), 3), 3),
                                        R"([[[3, 0, 1],
                                              [0, 0, 0],
                                              [6, 0, 4]],
                                            [[0, 0, 0],
                                              [0, 0, 0],
                                              [0, 0, 0]],
                                            [[0, 0, 0],
                                              [0, 0, 0],
                                              [0, 0, 0]],
                                            [[9, 0, 7],
                                              [0, 0, 0],
                                              [12, 0, 10]]])"));

      ASSERT_TRUE(read_visitor.array_->Equals(expected)) << read_visitor.array_->Diff(*expected);
      ASSERT_TRUE(result->Equals(expected)) << result->Diff(*expected);
    }
  }
}

TEST_F(InPlaceReadTest, VariableComplexReadToResult) {
  const auto & table = table_proxy_.table();
  using CT = casacore::Complex;

  {
    // Write some fixed shape data to 2 rows
    ASSERT_OK_AND_ASSIGN(auto data,
                         ArrayFromJSON(arrow::fixed_size_list(
                                        arrow::fixed_size_list(
                                          arrow::fixed_size_list(
                                            arrow::float32(), 2), 3), 3),
                                       R"([[[[1, 1], [2, 2], [3, 3]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[4, 4], [5, 5], [6, 6]]],
                                           [[[7, 7], [8, 8], [9, 9]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[10, 10], [11, 11], [12, 12]]]])"));

    auto var = GetArrayColumn<CT>(table, "VAR_COMPLEX");
    ASSERT_OK_AND_ASSIGN(auto write_map, ColumnWriteMap::Make(var, {{0, 3}}, data));
    auto write_visitor = arcae::ColumnWriteVisitor(write_map);
    ASSERT_OK(write_visitor.Visit());

    EXPECT_THAT(var.getColumnCells(casacore::RefRows(0, 0)),
                ::testing::ElementsAre(CT{1, 1}, CT{2, 2}, CT{3, 3},
                                       CT{0, 0}, CT{0, 0}, CT{0, 0},
                                       CT{4, 4}, CT{5, 5}, CT{6, 6}));

    EXPECT_FALSE(var.isDefined(1));
    EXPECT_FALSE(var.isDefined(2));

    EXPECT_THAT(var.getColumnCells(casacore::RefRows(3, 3)),
                ::testing::ElementsAre(CT{7, 7}, CT{8, 8}, CT{9, 9},
                                       CT{0, 0}, CT{0, 0}, CT{0, 0},
                                       CT{10, 10}, CT{11, 11}, CT{12, 12}));


    {
      // Create a result array and read from the written rows, ignoring two rows
      ASSERT_OK_AND_ASSIGN(auto result,
                         ArrayFromJSON(arrow::fixed_size_list(
                                        arrow::fixed_size_list(
                                          arrow::fixed_size_list(
                                            arrow::float32(), 2), 3), 3),
                                       R"([[[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]],
                                           [[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]],
                                           [[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]],
                                           [[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]]])"));


      ASSERT_OK_AND_ASSIGN(auto read_map, ColumnReadMap::Make(var, {{0, -1, -1, 3}}, result));
      auto read_visitor = arcae::ColumnReadVisitor(read_map);
      ASSERT_OK(read_visitor.Visit());


      ASSERT_OK_AND_ASSIGN(auto expected,
                         ArrayFromJSON(arrow::fixed_size_list(
                                        arrow::fixed_size_list(
                                          arrow::fixed_size_list(
                                            arrow::float32(), 2), 3), 3),
                                       R"([[[[1, 1], [2, 2], [3, 3]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[4, 4], [5, 5], [6, 6]]],
                                           [[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]],
                                           [[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]],
                                           [[[7, 7], [8, 8], [9, 9]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[10, 10], [11, 11], [12, 12]]]])"));


      ASSERT_TRUE(read_visitor.array_->Equals(expected)) << read_visitor.array_->Diff(*expected);
      ASSERT_TRUE(result->Equals(expected)) << result->Diff(*expected);
    }

    {
      // Create a result array and read from the written rows, ignoring two rows
      ASSERT_OK_AND_ASSIGN(auto result,
                         ArrayFromJSON(arrow::fixed_size_list(
                                        arrow::fixed_size_list(
                                          arrow::fixed_size_list(
                                            arrow::float32(), 2), 3), 3),
                                       R"([[[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]],
                                           [[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]],
                                           [[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]],
                                           [[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]]])"));


      ASSERT_OK_AND_ASSIGN(auto read_map, ColumnReadMap::Make(var, {{0, -1, -1, 3}, {}, {2, -1, 0}}, result));
      auto read_visitor = arcae::ColumnReadVisitor(read_map);
      ASSERT_OK(read_visitor.Visit());

      ASSERT_OK_AND_ASSIGN(auto expected,
                         ArrayFromJSON(arrow::fixed_size_list(
                                        arrow::fixed_size_list(
                                          arrow::fixed_size_list(
                                            arrow::float32(), 2), 3), 3),
                                       R"([[[[3, 3], [0, 0], [1, 1]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[6, 6], [0, 0], [4, 4]]],
                                           [[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]],
                                           [[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]],
                                           [[[9, 9], [0, 0], [7, 7]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[12, 12], [0, 0], [10, 10]]]])"));

      ASSERT_TRUE(read_visitor.array_->Equals(expected)) << read_visitor.array_->Diff(*expected);
      ASSERT_TRUE(result->Equals(expected)) << result->Diff(*expected);
    }
  }
}

TEST_F(InPlaceReadTest, FixedComplexReadToResult) {
  const auto & table = table_proxy_.table();
  using CT = casacore::Complex;

  auto fixed = GetArrayColumn<CT>(table, "FIXED_COMPLEX");
  auto zeroes = casacore::Array<CT>(IPosition({3, 3, knrow}), CT{0});
  fixed.putColumn(zeroes);

  {
    // Write some fixed shape data to 2 rows
    ASSERT_OK_AND_ASSIGN(auto data,
                         ArrayFromJSON(arrow::fixed_size_list(
                                        arrow::fixed_size_list(
                                          arrow::fixed_size_list(
                                            arrow::float32(), 2), 3), 3),
                                       R"([[[[1, 1], [2, 2], [3, 3]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[4, 4], [5, 5], [6, 6]]],
                                           [[[7, 7], [8, 8], [9, 9]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[10, 10], [11, 11], [12, 12]]]])"));


    ASSERT_OK_AND_ASSIGN(auto write_map, ColumnWriteMap::Make(fixed, {{0, 3}}, data));
    auto write_visitor = arcae::ColumnWriteVisitor(write_map);
    ASSERT_OK(write_visitor.Visit());

    EXPECT_THAT(fixed.getColumnCells(casacore::RefRows(0, 0)),
                ::testing::ElementsAre(CT{1, 1}, CT{2, 2}, CT{3, 3},
                                       CT{0, 0}, CT{0, 0}, CT{0, 0},
                                       CT{4, 4}, CT{5, 5}, CT{6, 6}));

    EXPECT_THAT(fixed.getColumnCells(casacore::RefRows(1, 1)),
                ::testing::ElementsAre(CT{0, 0}, CT{0, 0}, CT{0, 0},
                                       CT{0, 0}, CT{0, 0}, CT{0, 0},
                                       CT{0, 0}, CT{0, 0}, CT{0, 0}));


    EXPECT_THAT(fixed.getColumnCells(casacore::RefRows(2, 2)),
                ::testing::ElementsAre(CT{0, 0}, CT{0, 0}, CT{0, 0},
                                       CT{0, 0}, CT{0, 0}, CT{0, 0},
                                       CT{0, 0}, CT{0, 0}, CT{0, 0}));

    EXPECT_THAT(fixed.getColumnCells(casacore::RefRows(3, 3)),
                ::testing::ElementsAre(CT{7, 7}, CT{8, 8}, CT{9, 9},
                                       CT{0, 0}, CT{0, 0}, CT{0, 0},
                                       CT{10, 10}, CT{11, 11}, CT{12, 12}));


    {
      // Create a result array and read from the written rows, ignoring two rows
      ASSERT_OK_AND_ASSIGN(auto result,
                         ArrayFromJSON(arrow::fixed_size_list(
                                        arrow::fixed_size_list(
                                          arrow::fixed_size_list(
                                            arrow::float32(), 2), 3), 3),
                                       R"([[[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]],
                                           [[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]],
                                           [[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]],
                                           [[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]]])"));


      ASSERT_OK_AND_ASSIGN(auto read_map, ColumnReadMap::Make(fixed, {{0, -1, -1, 3}}, result));
      auto read_visitor = arcae::ColumnReadVisitor(read_map);
      ASSERT_OK(read_visitor.Visit());


      ASSERT_OK_AND_ASSIGN(auto expected,
                         ArrayFromJSON(arrow::fixed_size_list(
                                        arrow::fixed_size_list(
                                          arrow::fixed_size_list(
                                            arrow::float32(), 2), 3), 3),
                                       R"([[[[1, 1], [2, 2], [3, 3]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[4, 4], [5, 5], [6, 6]]],
                                           [[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]],
                                           [[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]],
                                           [[[7, 7], [8, 8], [9, 9]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[10, 10], [11, 11], [12, 12]]]])"));


      ASSERT_TRUE(read_visitor.array_->Equals(expected)) << read_visitor.array_->Diff(*expected);
      ASSERT_TRUE(result->Equals(expected)) << result->Diff(*expected);
    }

    {
      // Create a result array and read from the written rows, ignoring two rows
      ASSERT_OK_AND_ASSIGN(auto result,
                         ArrayFromJSON(arrow::fixed_size_list(
                                        arrow::fixed_size_list(
                                          arrow::fixed_size_list(
                                            arrow::float32(), 2), 3), 3),
                                       R"([[[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]],
                                           [[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]],
                                           [[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]],
                                           [[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]]])"));


      ASSERT_OK_AND_ASSIGN(auto read_map, ColumnReadMap::Make(fixed, {{0, -1, -1, 3}, {}, {2, -1, 0}}, result));
      auto read_visitor = arcae::ColumnReadVisitor(read_map);
      ASSERT_OK(read_visitor.Visit());

      ASSERT_OK_AND_ASSIGN(auto expected,
                         ArrayFromJSON(arrow::fixed_size_list(
                                        arrow::fixed_size_list(
                                          arrow::fixed_size_list(
                                            arrow::float32(), 2), 3), 3),
                                       R"([[[[3, 3], [0, 0], [1, 1]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[6, 6], [0, 0], [4, 4]]],
                                           [[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]],
                                           [[[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[0, 0], [0, 0], [0, 0]]],
                                           [[[9, 9], [0, 0], [7, 7]],
                                            [[0, 0], [0, 0], [0, 0]],
                                            [[12, 12], [0, 0], [10, 10]]]])"));

      ASSERT_TRUE(read_visitor.array_->Equals(expected)) << read_visitor.array_->Diff(*expected);
      ASSERT_TRUE(result->Equals(expected)) << result->Diff(*expected);
    }
  }
}