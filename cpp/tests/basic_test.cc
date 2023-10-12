
#include <arcae/safe_table_proxy.h>
#include <arrow/testing/gtest_util.h>
#include <casacore/tables/Tables.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>
#include <gtest/gtest.h>

// Basic creation of a SafeTableProxy
TEST(BasicTableTests, CasaTable) {

  auto factory = []() -> arrow::Result<std::shared_ptr<casacore::TableProxy>> {
    auto table_desc = casacore::TableDesc(casacore::MeasurementSet::requiredTableDesc());
    auto setup_new_table = casacore::SetupNewTable("test.table", table_desc, casacore::Table::New);
    auto ms = casacore::MeasurementSet(setup_new_table, 100);
    return std::make_shared<casacore::TableProxy>(ms);
  };

  ASSERT_OK_AND_ASSIGN(auto table_proxy, arcae::SafeTableProxy::Make(factory));
  ASSERT_EQ(table_proxy->nRow(), 100);
}