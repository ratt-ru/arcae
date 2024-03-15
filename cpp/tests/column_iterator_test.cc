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

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <arcae/safe_table_proxy.h>
#include <arcae/column_read_map.h>

#include <tests/test_utils.h>

using casacore::Array;
using casacore::ArrayColumn;
using casacore::ArrayColumnDesc;
using MS = casacore::MeasurementSet;
using MSColumns = casacore::MSMainEnums::PredefinedColumns;
using casacore::SetupNewTable;
using casacore::ScalarColumn;
using casacore::Slicer;
using casacore::Table;
using casacore::TableDesc;
using casacore::TableColumn;
using casacore::TableProxy;
using IPos = casacore::IPosition;

using arcae::ColumnReadMap;

using namespace std::string_literals;

static constexpr std::size_t knrow = 3;
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

class ColumnIteratorTest : public ::testing::Test {
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

        auto var_column_desc = ArrayColumnDesc<casacore::Int>("VAR_DATA", 2);
        table_desc.addColumn(var_column_desc);
        auto setup_new_table = SetupNewTable(table_name_, table_desc, Table::New);
        auto ms = MS(setup_new_table, knrow);

        auto time_data = GetScalarColumn<casacore::Double>(ms, "TIME");
        auto var_data = GetArrayColumn<casacore::Int>(ms, "VAR_DATA");

        for(auto [r, v] = std::tuple{ssize_t{0}, std::size_t{0}}; r < knrow; ++r) {
          auto var_array = Array<casacore::Int>(IPos({
            ssize_t{kncorr} - (r % 2), ssize_t{knchan} - (r % 2), 1}));
          for(auto it = std::begin(var_array); it != std::end(var_array); ++it, ++v) *it = v;
          var_data.putColumnCells(casacore::RefRows(r, r), var_array);
          time_data.put(r, r);
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
    }
};

TEST_F(ColumnIteratorTest, SelectionVariable) {
  const auto & table = table_proxy_.table();

  {
    // Variable data column
    auto var_data = GetArrayColumn<casacore::Int>(table, "VAR_DATA");
    {
      // Get row 0
      ASSERT_OK_AND_ASSIGN(auto map, ColumnReadMap::Make(var_data, {{0}}));
      ASSERT_EQ(map.nRanges(), 1);
      ASSERT_EQ(map.nElements(), 4);
      ASSERT_EQ(map.GetOutputShape(), IPos({2, 2, 1}));
      auto rit = map.RangeBegin();
      ASSERT_EQ(rit.GetRowSlicer(), Slicer(IPos({0}), IPos({0}), Slicer::endIsLast));
      ASSERT_EQ(rit.GetSectionSlicer(), Slicer(IPos({0, 0}), IPos({1, 1}), Slicer::endIsLast));
      auto array = var_data.getColumnRange(rit.GetRowSlicer(), rit.GetSectionSlicer());
      auto mit = rit.MapBegin();
      ASSERT_EQ(mit.ChunkOffset(), 0);
      ASSERT_EQ(mit.GlobalOffset(), 0);
      ASSERT_EQ(array.data()[mit.ChunkOffset()], 0);
      ++mit;
      ASSERT_EQ(mit.ChunkOffset(), 1);
      ASSERT_EQ(mit.GlobalOffset(), 1);
      ASSERT_EQ(array.data()[mit.ChunkOffset()], 1);
      ++mit;
      ASSERT_EQ(mit.ChunkOffset(), 2);
      ASSERT_EQ(mit.GlobalOffset(), 2);
      ASSERT_EQ(array.data()[mit.ChunkOffset()], 2);
      ++mit;
      ASSERT_EQ(mit.ChunkOffset(), 3);
      ASSERT_EQ(mit.GlobalOffset(), 3);
      ASSERT_EQ(array.data()[mit.ChunkOffset()], 3);
      ++mit;
      ASSERT_EQ(mit, rit.MapEnd());
      ++rit;
      ASSERT_EQ(rit, map.RangeEnd());
    }
    // Get row 1
    {
      ASSERT_OK_AND_ASSIGN(auto map, ColumnReadMap::Make(var_data, {{1}}));
      ASSERT_EQ(map.nRanges(), 1);
      ASSERT_EQ(map.nElements(), 1);
      auto rit = map.RangeBegin();
      ASSERT_EQ(map.GetOutputShape(), IPos({1, 1, 1}));
      ASSERT_EQ(rit.GetRowSlicer(), Slicer(IPos({1}), IPos({1}), Slicer::endIsLast));
      ASSERT_EQ(rit.GetSectionSlicer(), Slicer(IPos({0, 0}), IPos({0, 0}), Slicer::endIsLast));
      auto array = var_data.getColumnRange(rit.GetRowSlicer(), rit.GetSectionSlicer());
      auto mit = rit.MapBegin();
      ASSERT_EQ(mit.ChunkOffset(), 0);
      ASSERT_EQ(mit.GlobalOffset(), 0);
      ASSERT_EQ(array.data()[mit.ChunkOffset()], 4);
      ++mit;
      ASSERT_EQ(mit, rit.MapEnd());
      ++rit;
      ASSERT_EQ(rit, map.RangeEnd());
    }
    {
      // Get row 0 and 1
      ASSERT_OK_AND_ASSIGN(auto map, ColumnReadMap::Make(var_data, {{0, 1}}));
      ASSERT_EQ(map.nRanges(), 2);
      ASSERT_EQ(map.nElements(), 5);
      ASSERT_FALSE(map.GetOutputShape().ok());
      auto rit = map.RangeBegin();
      ASSERT_EQ(rit.GetRowSlicer(), Slicer(IPos({0}), IPos({0}), Slicer::endIsLast));
      ASSERT_EQ(rit.GetSectionSlicer(), Slicer(IPos({0, 0}), IPos({1, 1}), Slicer::endIsLast));

      {
        auto array = var_data.getColumnRange(rit.GetRowSlicer(), rit.GetSectionSlicer());

        auto mit = rit.MapBegin();
        ASSERT_EQ(mit.ChunkOffset(), 0);
        ASSERT_EQ(mit.GlobalOffset(), 0);
        ASSERT_EQ(array.data()[mit.ChunkOffset()], 0);
        ++mit;
        ASSERT_EQ(mit.ChunkOffset(), 1);
        ASSERT_EQ(mit.GlobalOffset(), 1);
        ASSERT_EQ(array.data()[mit.ChunkOffset()], 1);
        ++mit;
        ASSERT_EQ(mit.ChunkOffset(), 2);
        ASSERT_EQ(mit.GlobalOffset(), 2);
        ASSERT_EQ(array.data()[mit.ChunkOffset()], 2);
        ++mit;
        ASSERT_EQ(mit.ChunkOffset(), 3);
        ASSERT_EQ(mit.GlobalOffset(), 3);
        ASSERT_EQ(array.data()[mit.ChunkOffset()], 3);
        ++mit;
        ASSERT_EQ(mit, rit.MapEnd());
      }
      ++rit;

      ASSERT_EQ(rit.GetRowSlicer(), Slicer(IPos({1}), IPos({1}), Slicer::endIsLast));
      ASSERT_EQ(rit.GetSectionSlicer(), Slicer(IPos({0, 0}), IPos({0, 0}), Slicer::endIsLast));

      {
        auto array = var_data.getColumnRange(rit.GetRowSlicer(), rit.GetSectionSlicer());
        auto mit = rit.MapBegin();
        ASSERT_EQ(mit.ChunkOffset(), 0);
        ASSERT_EQ(mit.GlobalOffset(), 4);
        ASSERT_EQ(array.data()[mit.ChunkOffset()], 4);
        ++mit;
        ASSERT_EQ(mit, rit.MapEnd());
      }

      ++rit;
      ASSERT_EQ(rit, map.RangeEnd());
    }
  }
}

TEST_F(ColumnIteratorTest, OutOfOrderSelection) {
  const auto & table = table_proxy_.table();

  auto time = GetScalarColumn<casacore::Double>(table, "TIME");

  ASSERT_OK_AND_ASSIGN(auto map, ColumnReadMap::Make(time, {{2, 0}}));
  ASSERT_EQ(map.nRanges(), 2);
  ASSERT_EQ(map.nElements(), 2);
  ASSERT_EQ(map.GetOutputShape(), IPos({2}));
  auto rit = map.RangeBegin();
  ASSERT_EQ(rit.GetRowSlicer(), Slicer(IPos({0}), IPos({0}), Slicer::endIsLast));
  auto mit = rit.MapBegin();
  auto array = time.getColumnRange(rit.GetRowSlicer());
  ASSERT_EQ(mit.ChunkOffset(), 0);
  ASSERT_EQ(mit.GlobalOffset(), 1);
  ASSERT_EQ(array.data()[mit.ChunkOffset()], 0);
  ++mit;
  ASSERT_EQ(mit, rit.MapEnd());

  ++rit;
  mit = rit.MapBegin();
  array = time.getColumnRange(rit.GetRowSlicer());
  ASSERT_EQ(rit.GetRowSlicer(), Slicer(IPos({2}), IPos({2}), Slicer::endIsLast));
  ASSERT_EQ(mit.ChunkOffset(), 0);
  ASSERT_EQ(mit.GlobalOffset(), 0);
  ASSERT_EQ(array.data()[mit.ChunkOffset()], 2);
  ++mit;
  ASSERT_EQ(mit, rit.MapEnd());

  ++rit;
  ASSERT_EQ(rit, map.RangeEnd());
}
