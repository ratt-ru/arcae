#include <algorithm>
#include <memory>
#include <string>

#include <arrow/result.h>
#include <arrow/ipc/json_simple.h>
#include <arrow/testing/gtest_util.h>

#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>
#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/ColumnDesc.h>
#include <casacore/tables/Tables/ArrColDesc.h>
#include <casacore/tables/Tables/ScaColDesc.h>
#include <casacore/tables/Tables/Table.h>
#include <casacore/tables/Tables/TableColumn.h>
#include <casacore/tables/Tables/TableProxy.h>
#include <casacore/tables/Tables/RefRows.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <arcae/base_column_map.h>
#include <arcae/safe_table_proxy.h>
#include <arcae/column_read_map.h>
#include <arcae/column_write_map.h>
#include <arcae/column_read_visitor.h>
#include <arcae/column_write_visitor.h>

#include <tests/test_utils.h>

using arrow::ipc::internal::json::ArrayFromJSON;

using casacore::Array;
using casacore::ArrayColumn;
using casacore::ArrayColumnDesc;
using casacore::ColumnDesc;
using MS = casacore::MeasurementSet;
using MSColumns = casacore::MSMainEnums::PredefinedColumns;
using casacore::SetupNewTable;
using casacore::ScalarColumn;
using casacore::ScalarColumnDesc;
using casacore::Table;
using casacore::TableDesc;
using casacore::TableColumn;
using casacore::TableProxy;
using casacore::TiledColumnStMan;
using IPos = casacore::IPosition;

using arcae::ColumnReadMap;
using arcae::ColumnWriteMap;
using arcae::ColumnReadVisitor;
using arcae::ColumnWriteVisitor;

using namespace std::string_literals;

static constexpr std::size_t knrow = 1000000;
static constexpr std::size_t knchan = 16;
static constexpr std::size_t kncorr = 4;

static constexpr std::size_t knrowtile = 100;

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

class TransposeTest : public ::testing::Test {
  protected:
    casacore::TableProxy table_proxy_;
    std::string table_name_;

    void SetUp() override {
      auto factory = [this]() -> arrow::Result<std::shared_ptr<TableProxy>> {
        auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        table_name_ = std::string(test_info->name() + "-"s + arcae::hexuuid(4) + ".table"s);

        auto table_desc = TableDesc(MS::requiredTableDesc());
        auto data_shape = IPos({kncorr, knchan});
        auto tile_shape = IPos({kncorr, knchan, knrowtile});

        auto fixed_coldesc = ArrayColumnDesc<casacore::Complex>(
          "FIXED_DATA", data_shape, ColumnDesc::FixedShape);
        table_desc.addColumn(fixed_coldesc);

        auto storage_manager = TiledColumnStMan("TiledModelData", tile_shape);
        auto setup_new_table = SetupNewTable(table_name_, table_desc, Table::New);
        setup_new_table.bindColumn("FIXED_DATA", storage_manager);

        auto ms = MS(setup_new_table, knrow);
        auto fixed_data = GetArrayColumn<casacore::Complex>(ms, "FIXED_DATA");

        for(auto [r, v] = std::tuple{ssize_t{0}, std::size_t{0}}; r < knrow; ++r) {
          auto fixed_array = Array<casacore::Complex>(IPos({kncorr, knchan, 1}), {float(r), float(r)});
          auto refrows = casacore::RefRows(r, r);
          fixed_data.putColumnCells(refrows, fixed_array);
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


TEST_F(TransposeTest, Basic) {
  auto fixed_data = GetArrayColumn<casacore::Complex>(table_proxy_.table(), "FIXED_DATA");

  auto row_ids = std::vector<arcae::RowId>(knrow, 0);
  std::random_shuffle(std::begin(row_ids), std::end(row_ids));
  auto selection = arcae::ColumnSelection{row_ids, {}, {}};
  ASSERT_OK_AND_ASSIGN(auto read_map, ColumnReadMap::Make(fixed_data, selection));
  auto visitor = ColumnReadVisitor(read_map);
  ASSERT_OK(visitor.Visit());
}