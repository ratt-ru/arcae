#include <filesystem>
#include <memory>

#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/type_fwd.h>
#include <arrow/ipc/json_simple.h>
#include <arrow/testing/gtest_util.h>

#include <casacore/casa/Utilities/DataType.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/BasicSL/Complexfwd.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>
#include <casacore/tables/Tables/SetupNewTab.h>
#include <casacore/tables/Tables/TableProxy.h>

#include <tests/test_utils.h>

#include <gtest/gtest.h>

#include "arcae/new_table_proxy.h"
#include "arcae/selection.h"

using ::arrow::ipc::internal::json::ArrayFromJSON;

using ::arcae::GetArrayColumn;
using ::arcae::GetScalarColumn;
using ::arcae::detail::IndexType;
using ::arcae::detail::NewTableProxy;
using ::arcae::detail::SelectionBuilder;

using ::casacore::Array;
using ::casacore::ArrayColumnDesc;
using ::casacore::ColumnDesc;
using ::casacore::Complex;
using MS = casacore::MeasurementSet;
using ::casacore::Record;
using ::casacore::ScalarColumnDesc;
using ::casacore::SetupNewTable;
using ::casacore::String;
using ::casacore::Table;
using ::casacore::TableDesc;
using ::casacore::TableLock;
using ::casacore::TableProxy;
using ::casacore::Vector;
using ::casacore::IPosition;

using namespace std::string_literals;

static constexpr std::size_t knrow = 10;
static constexpr std::size_t knchan = 4;
static constexpr std::size_t kncorr = 2;

namespace {

class NewTableProxyTest : public ::testing::Test {
  protected:
    std::string table_name_;

    void SetUp() override {
      auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info();
      table_name_ = std::string(test_info->name() + "-"s + arcae::hexuuid(4) + ".table"s);

      auto table_desc = TableDesc(MS::requiredTableDesc());
      auto data_shape = IPosition({kncorr, knchan});
      auto data_column_desc = ArrayColumnDesc<Complex>(
        "MODEL_DATA", data_shape, ColumnDesc::FixedShape);
      table_desc.addColumn(data_column_desc);
      auto str_column_desc = ScalarColumnDesc<String>("STRING_DATA");
      table_desc.addColumn(str_column_desc);
      auto setup_new_table = SetupNewTable(table_name_, table_desc, Table::New);
      auto ms = MS(setup_new_table, knrow);
      auto data = GetArrayColumn<Complex>(ms, "MODEL_DATA");
      auto time = GetScalarColumn<casacore::Double>(ms, "TIME");
      data.putColumn(Array<Complex>(IPosition({kncorr, knchan, knrow}), {1, 2}));
      auto str_data = arcae::GetScalarColumn<String>(ms, "STRING_DATA");
      str_data.putColumn(Vector<String>(IPosition({knrow}), String("FOO")));

      for(std::size_t i = 0; i < knrow; ++i) time.put(i, i);
      for(std::size_t i = 0; i < knrow; ++i) {
        str_data.put(i, "FOO-" + std::to_string(i));
      }
      Array<Complex> complex_vals(IPosition({kncorr, knchan, knrow}));
      for(std::size_t i = 0; i < complex_vals.size(); ++i) {
        auto v = Complex::value_type(i);
        complex_vals.data()[i] = {v, v};
      }
      data.putColumn(complex_vals);
    }

    arrow::Result<std::shared_ptr<NewTableProxy>> OpenTable() {
      return NewTableProxy::Make(
        [name = table_name_]() {
          auto lock = TableLock(TableLock::LockOption::AutoLocking);
          auto lockoptions = Record();
          lockoptions.define("option", "nolock");
          lockoptions.define("internal", lock.interval());
          lockoptions.define("maxwait", casacore::Int(lock.maxWait()));
          return std::make_shared<TableProxy>(name, lockoptions, Table::Old);
      });
    }

    void TearDown() override {
      std::filesystem::remove_all(table_name_);
    }
};

TEST_F(NewTableProxyTest, Basic) {
  ASSERT_OK_AND_ASSIGN(auto ntp, OpenTable());

  {
    ASSERT_OK_AND_ASSIGN(auto time, ntp->GetColumn("TIME"));
    ASSERT_OK_AND_ASSIGN(auto uvw, ntp->GetColumn("UVW"));
    ASSERT_OK_AND_ASSIGN(auto data, ntp->GetColumn("MODEL_DATA"));
    ASSERT_OK_AND_ASSIGN(auto str, ntp->GetColumn("STRING_DATA"));

    ASSERT_OK_AND_ASSIGN(auto expected,
                         ArrayFromJSON(arrow::float64(),
                                       "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]"));
    EXPECT_TRUE(time->Equals(expected));

    ASSERT_OK_AND_ASSIGN(expected,
                         ArrayFromJSON(arrow::utf8(),
                                       R"([
                                        "FOO-0",
                                        "FOO-1",
                                        "FOO-2",
                                        "FOO-3",
                                        "FOO-4",
                                        "FOO-5",
                                        "FOO-6",
                                        "FOO-7",
                                        "FOO-8",
                                        "FOO-9"])"));
    EXPECT_TRUE(str->Equals(expected));
  }

  {
    std::vector<IndexType> rows = {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
    auto selection = SelectionBuilder()
                      .Add(std::move(rows))
                      .Build();

    ASSERT_OK_AND_ASSIGN(auto time, ntp->GetColumn("TIME", selection));
    ASSERT_OK_AND_ASSIGN(auto str, ntp->GetColumn("STRING_DATA", selection));

    ASSERT_OK_AND_ASSIGN(auto expected,
                         ArrayFromJSON(arrow::float64(),
                                       "[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]"));
    EXPECT_TRUE(time->Equals(expected));

    ASSERT_OK_AND_ASSIGN(expected,
                         ArrayFromJSON(arrow::utf8(),
                                       R"([
                                        "FOO-9",
                                        "FOO-8",
                                        "FOO-7",
                                        "FOO-6",
                                        "FOO-5",
                                        "FOO-4",
                                        "FOO-3",
                                        "FOO-2",
                                        "FOO-1",
                                        "FOO-0"])"));
  }
}

}