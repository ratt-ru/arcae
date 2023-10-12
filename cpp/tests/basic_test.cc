#include <iostream>

#include <arcae/safe_table_proxy.h>
#include <tests/test_utils.h>
#include <arrow/testing/gtest_util.h>
#include <casacore/tables/Tables.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>
#include <gtest/gtest.h>

using namespace std::string_literals;

class BasicTableTests : public ::testing::Test {
  protected:
    std::shared_ptr<arcae::SafeTableProxy> table_proxy_;

    void SetUp() override {
      auto factory = []() -> arrow::Result<std::shared_ptr<casacore::TableProxy>> {
        auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        auto table_desc = casacore::TableDesc(casacore::MeasurementSet::requiredTableDesc());
        auto table_name = std::string(test_info->name() + "-"s + arcae::hexuuid(4) + ".table"s);
        auto setup_new_table = casacore::SetupNewTable(table_name, table_desc, casacore::Table::New);
        auto ms = casacore::MeasurementSet(setup_new_table, 100);
        return std::make_shared<casacore::TableProxy>(ms);
      };

      ASSERT_OK_AND_ASSIGN(table_proxy_, arcae::SafeTableProxy::Make(factory));
    }
};

// Basic creation of a SafeTableProxy
TEST_F(BasicTableTests, CasaTable) {
  ASSERT_TRUE(table_proxy_);
  ASSERT_EQ(table_proxy_->nRow(), 100);
}