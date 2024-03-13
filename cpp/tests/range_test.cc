#include <arrow/result.h>
#include <arrow/testing/gtest_util.h>

#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/BasicSL/Complexfwd.h>
#include <casacore/tables/Tables/ArrColDesc.h>
#include <casacore/tables/Tables/RefRows.h>
#include <casacore/tables/Tables/Table.h>
#include <casacore/tables/Tables/TableProxy.h>
#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/TableColumn.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>

#include <arcae/column_read_map.h>
#include <arcae/safe_table_proxy.h>
#include <arcae/table_factory.h>

#include <tests/test_utils.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

using arcae::ColumnReadMap;
using arcae::ColumnSelection;
using arcae::IdMap;
using arcae::Range;
using casacore::ArrayColumn;
using casacore::ArrayColumnDesc;
using casacore::ColumnDesc;
using CasaComplex = casacore::Complex;
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

using namespace std::string_literals;

static constexpr std::size_t knrow = 20;
static constexpr std::size_t knchan = 20;
static constexpr std::size_t kncorr = 20;

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

class RangeTest : public ::testing::Test {
  protected:
    std::string table_name_;
    casacore::TableProxy proxy_;

    void SetUp() override {
      auto factory = [this]() -> arrow::Result<std::shared_ptr<TableProxy>> {
        auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        table_name_ = std::string(test_info->name() + "-"s + arcae::hexuuid(4) + ".table"s);

        auto table_desc = TableDesc(MS::requiredTableDesc());
        auto data_shape = IPos({kncorr, knchan});
        auto tile_shape = IPos({kncorr, knchan, 1});
        auto data_column_desc = ArrayColumnDesc<CasaComplex>(
            "MODEL_DATA", data_shape, ColumnDesc::FixedShape);

        table_desc.addColumn(data_column_desc);
        auto storage_manager = TiledColumnStMan("TiledModelData", tile_shape);
        auto setup_new_table = SetupNewTable(table_name_, table_desc, Table::New);
        setup_new_table.bindColumn("MODEL_DATA", storage_manager);
        auto ms = MS(setup_new_table, knrow);
        return std::make_shared<TableProxy>(ms);
      };

      ASSERT_OK_AND_ASSIGN(auto table_proxy_, arcae::SafeTableProxy::Make(factory));
      table_proxy_.reset();

      auto lock = casacore::TableLock(casacore::TableLock::LockOption::AutoNoReadLocking);
      auto lockoptions = casacore::Record();
      lockoptions.define("option", "auto");
      lockoptions.define("internal", lock.interval());
      lockoptions.define("maxwait", casacore::Int(lock.maxWait()));
      proxy_ = casacore::TableProxy(table_name_, lockoptions, casacore::Table::Old);
    }
};



TEST_F(RangeTest, CheckMapsAndRangesSingleton) {
  auto data = GetArrayColumn<CasaComplex>(proxy_.table(), MS::MODEL_DATA);
  ASSERT_OK_AND_ASSIGN(auto map, ColumnReadMap::Make(data, ColumnSelection{{0}}));

  EXPECT_EQ(map.DimMap(0).size(), 0);
  EXPECT_EQ(map.DimMap(1).size(), 0);
  EXPECT_THAT(map.DimMap(2), ::testing::ElementsAre(IdMap{0, 0}));

  EXPECT_THAT(map.DimRanges(0), ::testing::ElementsAre(Range{0, 20, Range::FREE}));
  EXPECT_THAT(map.DimRanges(1), ::testing::ElementsAre(Range{0, 20, Range::FREE}));
  EXPECT_THAT(map.DimRanges(2), ::testing::ElementsAre(Range{0, 1, Range::MAP}));
}

TEST_F(RangeTest, CheckMapsAndRangesMultiple) {
  auto data = GetArrayColumn<CasaComplex>(proxy_.table(), MS::MODEL_DATA);
  auto selection = ColumnSelection{
        {4, 3, 2, 1, 8, 7},    // Two disjoint ranges
        {5, 6},                // One range
        {7, 9, 8, 12, 11}};    // Two disjoint ranges
  ASSERT_OK_AND_ASSIGN(auto map, ColumnReadMap::Make(data, std::move(selection)));

  EXPECT_THAT(map.DimMap(2), ::testing::ElementsAre(
        IdMap{1, 3},
        IdMap{2, 2},
        IdMap{3, 1},
        IdMap{4, 0},
        IdMap{7, 5},
        IdMap{8, 4}));

  EXPECT_THAT(map.DimRanges(2), ::testing::ElementsAre(
        Range{0, 4, Range::MAP},
        Range{4, 6, Range::MAP}));

  EXPECT_THAT(map.DimMap(1), ::testing::ElementsAre(
        IdMap{5, 0}, IdMap{6, 1}));

  EXPECT_THAT(map.DimRanges(1), ::testing::ElementsAre(
        Range{0, 2, Range::MAP}));

  EXPECT_THAT(map.DimMap(0), ::testing::ElementsAre(
        IdMap{7, 0},
        IdMap{8, 2},
        IdMap{9, 1},
        IdMap{11, 4},
        IdMap{12, 3}));

  EXPECT_THAT(map.DimRanges(0), ::testing::ElementsAre(
        Range{0, 3, Range::MAP},
        Range{3, 5, Range::MAP}));

  ASSERT_FALSE(map.IsSimple());
}


TEST_F(RangeTest, TestNegativeRows) {
  auto data = GetArrayColumn<CasaComplex>(proxy_.table(), MS::MODEL_DATA);
    ASSERT_OK_AND_ASSIGN(auto map, ColumnReadMap::Make(data, {{1, -1, -3, 4}, {-1, 1}, {-1, 1}}));
    EXPECT_THAT(map.DimMap(0), ::testing::ElementsAre(IdMap{1, 0}));
    EXPECT_THAT(map.DimMap(1), ::testing::ElementsAre(IdMap{1, 0}));
    EXPECT_THAT(map.DimMap(2), ::testing::ElementsAre(IdMap{1, 0}, IdMap{4, 1}));

}


TEST_F(RangeTest, TestSimplicity) {
  auto data = GetArrayColumn<CasaComplex>(proxy_.table(), MS::MODEL_DATA);

  {
    ASSERT_OK_AND_ASSIGN(auto map, ColumnReadMap::Make(data, {{1, 2, 3, 4}}));
    ASSERT_TRUE(map.IsSimple());
  }

  {
    ASSERT_OK_AND_ASSIGN(auto map, ColumnReadMap::Make(data, {{1, 2, 3, 4}, {0, 1}, {0, 1}}));
    ASSERT_TRUE(map.IsSimple());
  }

  {
    // Multiple mapping ranges (discontiguous)
    ASSERT_OK_AND_ASSIGN(auto map, ColumnReadMap::Make(data, {{1, 2, 4, 5}}));
    ASSERT_FALSE(map.IsSimple());
  }

  {
    // Not monotically increasing
    ASSERT_OK_AND_ASSIGN(auto map, ColumnReadMap::Make(data, {{4, 3, 2, 1}}));
    ASSERT_FALSE(map.IsSimple());
  }
}
