#include "arrow/ipc/json_simple.h"
#include <casacore/tables/Tables/ArrayColumn.h>
#include <memory>

#include <arrow/api.h>
#include <arrow/testing/gtest_util.h>

#include <casacore/casa/aipsxtype.h>
#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/ArrColDesc.h>
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

TEST_F(InPlaceReadTest, Basic) {
  const auto & table = table_proxy_.table();
  using CT = casacore::Float;

  {
    auto dtype = arrow::list(arrow::list(arrow::int32()));
    ASSERT_OK_AND_ASSIGN(auto data,
                          ArrayFromJSON(dtype,
                                        R"([[[0, 1],
                                             [2, 3]],
                                            [[4, 5, 6],
                                             [7, 8, 9],
                                             [10, 11, 12]]])"));

    auto var = GetArrayColumn<casacore::Int>(table, "DATA");
    ASSERT_OK_AND_ASSIGN(auto write_map, ColumnWriteMap::Make(var, {{0, 3}}, data));
    auto write_visitor = arcae::ColumnWriteVisitor(write_map);
    ASSERT_OK(write_visitor.Visit());

    ASSERT_OK_AND_ASSIGN(auto read_map, ColumnReadMap::Make(var, {{0, -1, -1, 3}}));
    auto read_visitor = arcae::ColumnReadVisitor(read_map);
    ASSERT_OK(read_visitor.Visit());
    ASSERT_TRUE(read_visitor.array_->Equals(data));
  }
}