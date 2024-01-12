#include <arrow/api.h>
#include <arrow/ipc/json_simple.h>
#include <arrow/testing/gtest_util.h>

#include <arcae/safe_table_proxy.h>
#include <arcae/column_read_map.h>
#include <arcae/column_write_map.h>
#include <arcae/column_read_visitor.h>
#include <arcae/column_write_visitor.h>

#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/Arrays/Slicer.h>
#include <casacore/casa/aipsxtype.h>
#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/ArrColDesc.h>
#include <casacore/tables/Tables/ArrayColumn.h>
#include <casacore/tables/Tables/SetupNewTab.h>
#include <casacore/tables/Tables/Table.h>
#include <casacore/tables/Tables/TableColumn.h>
#include <casacore/tables/Tables/TableProxy.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <tests/test_utils.h>

using namespace std::string_literals;

using arrow::ipc::internal::json::ArrayFromJSON;

using ::arcae::ColumnReadMap;
using ::arcae::ColumnWriteMap;

using ::casacore::ArrayColumn;
using ::casacore::TableColumn;
using ::casacore::SetupNewTable;
using ::casacore::Slicer;
using ::casacore::TableProxy;
using ::casacore::Table;
using IPos = ::casacore::IPosition;

static constexpr casacore::rownr_t knrow = 10;

template <typename T> ArrayColumn<T>
GetArrayColumn(const Table & table, const std::string & column) {
  return ArrayColumn<T>(TableColumn(table, column));
}

class EmptyVariableWriteTest : public ::testing::Test {
  protected:
    casacore::TableProxy table_proxy_;
    std::string table_name_;

    void SetUp() override {
      auto factory = [this]() -> arrow::Result<std::shared_ptr<TableProxy>> {
        auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        table_name_ = std::string(test_info->name() + "-"s + arcae::hexuuid(4) + ".table"s);

        auto table_desc = casacore::TableDesc{};
        auto varcol_desc = casacore::ArrayColumnDesc<casacore::Int>(
          "DATA", 2);
        table_desc.addColumn(varcol_desc);
        auto complex_varcold_desc = casacore::ArrayColumnDesc<casacore::Complex>(
          "VAR_COMPLEX", 2);
        table_desc.addColumn(complex_varcold_desc);

        auto setup_new_table = SetupNewTable(table_name_, table_desc, Table::New);
        auto table = casacore::Table(setup_new_table, knrow);
        return std::make_shared<TableProxy>(table);
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


TEST_F(EmptyVariableWriteTest, EmptyVariableColumnWriteSanity) {
  const auto & table = table_proxy_.table();
  using CT = casacore::Int;

  auto var = GetArrayColumn<casacore::Int>(table, "DATA");
  auto array = casacore::Array<casacore::Int>(IPos({2, 2, 2}), CT{2});
  var.putColumnRange(casacore::Slicer(IPos{2}, IPos{3}, Slicer::endIsLast), array);

  for(std::size_t r=0; r < knrow; ++r) {
    if(r == 2 || r == 3) {
      ASSERT_TRUE(var.isDefined(r));
      ASSERT_EQ(var.shape(r), IPos({2, 2}));
    } else {
      ASSERT_FALSE(var.isDefined(r));
    }
  }

  auto row2 = var.getColumnRange(Slicer(IPos({2}), IPos({2}), Slicer::endIsLast));
  EXPECT_EQ(row2.shape(), IPos({2, 2, 1}));
  EXPECT_THAT(row2, ::testing::ElementsAre(CT{2}, CT{2}, CT{2}, CT{2}));

  // This destroys the original data
  var.setShape(2, IPos({3, 3}));
  auto row2_2 = var.getColumnRange(Slicer(IPos({2}), IPos({2}), Slicer::endIsLast));
  EXPECT_EQ(row2_2.shape(), IPos({3, 3, 1}));
}

TEST_F(EmptyVariableWriteTest, WriteToEmptyVariableColumn) {
  const auto & table = table_proxy_.table();
  using CT = casacore::Int;

  {
    // Variable data column, entire domain
    auto dtype = arrow::list(arrow::list(arrow::int32()));
    ASSERT_OK_AND_ASSIGN(auto data,
                          ArrayFromJSON(dtype,
                                        R"([[[0, 1],
                                             [2, 3]],
                                            [[4, 5, 6],
                                             [7, 8, 9],
                                             [10, 11, 12]]])"));

    auto var = GetArrayColumn<casacore::Int>(table, "DATA");
    ASSERT_OK_AND_ASSIGN(auto write_map, ColumnWriteMap::Make(var, {{0, 3}}, data, ColumnWriteMap::C_ORDER));
    auto write_visitor = arcae::ColumnWriteVisitor(write_map, data);
    ASSERT_OK(write_visitor.Visit());

    ASSERT_OK_AND_ASSIGN(auto read_map, ColumnReadMap::Make(var, {{0, 3}}));
    auto read_visitor = arcae::ColumnReadVisitor(read_map);
    ASSERT_OK(read_visitor.Visit(var.columnDesc().dataType()));
    ASSERT_TRUE(read_visitor.array_->Equals(data));

    EXPECT_TRUE(var.isDefined(0));
    auto row0 = var.getColumnRange(Slicer({IPos{0}, IPos{0}, Slicer::endIsLast}));
    EXPECT_THAT(row0, ::testing::ElementsAre(CT{0}, CT{1}, CT{2}, CT{3}));

    EXPECT_TRUE(var.isDefined(3));
    auto row3 = var.getColumnRange(Slicer({IPos{3}, IPos{3}, Slicer::endIsLast}));
    EXPECT_THAT(row3, ::testing::ElementsAre(
      CT{4}, CT{5}, CT{6},
      CT{7}, CT{8}, CT{9},
      CT{10}, CT{11}, CT{12}));

    EXPECT_FALSE(var.isDefined(1));
  }


  {
    // Given the write above, can we now write a selection of data?
    auto dtype = arrow::list(arrow::list(arrow::int32()));
    ASSERT_OK_AND_ASSIGN(auto data,
                          ArrayFromJSON(dtype,
                                        R"([[[22, 33]],
                                            [[77, 88, 99]]])"));

    auto var = GetArrayColumn<casacore::Int>(table, "DATA");
    ASSERT_OK_AND_ASSIGN(auto write_map, ColumnWriteMap::Make(var, {{0, 3}, {1}}, data));
    auto write_visitor = arcae::ColumnWriteVisitor(write_map, data);
    ASSERT_OK(write_visitor.Visit());

    ASSERT_OK_AND_ASSIGN(auto read_map, ColumnReadMap::Make(var, {{0, 3}, {1}}));
    auto read_visitor = arcae::ColumnReadVisitor(read_map);
    ASSERT_OK(read_visitor.Visit(var.columnDesc().dataType()));
    ASSERT_TRUE(read_visitor.array_->Equals(data));

    EXPECT_TRUE(var.isDefined(0));
    auto row0 = var.getColumnRange(Slicer({IPos{0}, IPos{0}, Slicer::endIsLast}));
    EXPECT_THAT(row0, ::testing::ElementsAre(CT{0}, CT{1}, CT{22}, CT{33}));

    EXPECT_TRUE(var.isDefined(3));
    auto row3 = var.getColumnRange(Slicer({IPos{3}, IPos{3}, Slicer::endIsLast}));
    EXPECT_THAT(row3, ::testing::ElementsAre(
      CT{4}, CT{5}, CT{6},
      CT{77}, CT{88}, CT{99},
      CT{10}, CT{11}, CT{12}));

    EXPECT_FALSE(var.isDefined(1));
  }
}

TEST_F(EmptyVariableWriteTest, WritePartialVariableEmptyColumn) {
  const auto & table = table_proxy_.table();
  using CT = casacore::Int;

  {
    // Variable data column, partial domain
    auto dtype = arrow::fixed_size_list(arrow::fixed_size_list(arrow::int32(), 2), 2);
    ASSERT_OK_AND_ASSIGN(auto data,
                          ArrayFromJSON(dtype,
                                        R"([[[0, 1],
                                             [2, 3]],
                                            [[4, 5],
                                             [7, 8]]])"));

    auto var = GetArrayColumn<casacore::Int>(table, "DATA");
    auto sel = arcae::ColumnSelection{{0, 3}, {1, 5}, {2, 4}};
    ASSERT_OK_AND_ASSIGN(auto write_map, ColumnWriteMap::Make(var, sel, data));
    auto write_visitor = arcae::ColumnWriteVisitor(write_map, data);
    ASSERT_OK(write_visitor.Visit());

    ASSERT_OK_AND_ASSIGN(auto read_map, ColumnReadMap::Make(var, sel));
    auto read_visitor = arcae::ColumnReadVisitor(read_map);
    ASSERT_OK(read_visitor.Visit(var.columnDesc().dataType()));

    ASSERT_TRUE(read_visitor.array_->Equals(data));

    EXPECT_TRUE(var.isDefined(0));
    auto row0 = var.getColumnRange(Slicer({IPos{0}, IPos{0}, Slicer::endIsLast}));
    EXPECT_EQ(row0.shape(), IPos({5, 6, 1}));
    EXPECT_EQ(row0(IPos({2, 1, 0})), 0);
    EXPECT_EQ(row0(IPos({4, 1, 0})), 1);
    EXPECT_EQ(row0(IPos({2, 5, 0})), 2);
    EXPECT_EQ(row0(IPos({4, 5, 0})), 3);

    EXPECT_TRUE(var.isDefined(3));
    auto row3 = var.getColumnRange(Slicer({IPos{3}, IPos{3}, Slicer::endIsLast}));
    EXPECT_EQ(row3(IPos({2, 1, 0})), 4);
    EXPECT_EQ(row3(IPos({4, 1, 0})), 5);
    EXPECT_EQ(row3(IPos({2, 5, 0})), 7);
    EXPECT_EQ(row3(IPos({4, 5, 0})), 8);

    EXPECT_FALSE(var.isDefined(1));
  }
}

TEST_F(EmptyVariableWriteTest, WritePartialComplexVariableEmptyColumn) {
  const auto & table = table_proxy_.table();
  using CT = casacore::Complex;

  {
    // Variable data column, partial domain
    auto dtype = arrow::fixed_size_list(
                  arrow::fixed_size_list(
                    arrow::fixed_size_list(
                      arrow::float32(), 2), 2), 2);
    ASSERT_OK_AND_ASSIGN(auto data,
                          ArrayFromJSON(dtype,
                          R"([[[[0, 1], [2, 3]],
                               [[4, 5], [6, 7]]],
                              [[[8, 9], [10, 11]],
                               [[12, 13], [14, 15]]]])"));

    auto var = GetArrayColumn<CT>(table, "VAR_COMPLEX");
    auto sel = arcae::ColumnSelection{{0, 3}, {1, 5}, {2, 4}};
    ASSERT_OK_AND_ASSIGN(auto write_map, ColumnWriteMap::Make(var, sel, data));
    auto write_visitor = arcae::ColumnWriteVisitor(write_map, data);
    ASSERT_OK(write_visitor.Visit());

    ASSERT_OK_AND_ASSIGN(auto read_map, ColumnReadMap::Make(var, sel));
    auto read_visitor = arcae::ColumnReadVisitor(read_map);
    ASSERT_OK(read_visitor.Visit(var.columnDesc().dataType()));

    ASSERT_TRUE(read_visitor.array_->Equals(data));

    EXPECT_TRUE(var.isDefined(0));
    auto row0 = var.getColumnRange(Slicer({IPos{0}, IPos{0}, Slicer::endIsLast}));
    EXPECT_EQ(row0.shape(), IPos({5, 6, 1}));
    EXPECT_EQ(row0(IPos({2, 1, 0})), CT(0, 1));
    EXPECT_EQ(row0(IPos({4, 1, 0})), CT(2, 3));
    EXPECT_EQ(row0(IPos({2, 5, 0})), CT(4, 5));
    EXPECT_EQ(row0(IPos({4, 5, 0})), CT(6, 7));

    EXPECT_TRUE(var.isDefined(3));
    auto row3 = var.getColumnRange(Slicer({IPos{3}, IPos{3}, Slicer::endIsLast}));
    EXPECT_EQ(row3(IPos({2, 1, 0})), CT(8, 9));
    EXPECT_EQ(row3(IPos({4, 1, 0})), CT(10, 11));
    EXPECT_EQ(row3(IPos({2, 5, 0})), CT(12, 13));
    EXPECT_EQ(row3(IPos({4, 5, 0})), CT(14, 15));

    EXPECT_FALSE(var.isDefined(1));
  }
}