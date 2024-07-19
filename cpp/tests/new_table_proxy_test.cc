#include <algorithm>
#include <cassert>
#include <cstddef>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <sys/types.h>

#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/ipc/json_simple.h>
#include <arrow/util/logging.h>
#include <arrow/testing/gtest_util.h>

#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/BasicSL/Complexfwd.h>
#include <casacore/casa/Utilities/DataType.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>
#include <casacore/tables/Tables/ColumnDesc.h>
#include <casacore/tables/Tables/SetupNewTab.h>
#include <casacore/tables/Tables/TableProxy.h>

#include <tests/test_utils.h>

#include <gtest/gtest.h>

#include "arcae/new_table_proxy.h"
#include "arcae/selection.h"

using ::arcae::GetArrayColumn;
using ::arcae::GetScalarColumn;
using ::arcae::detail::Index;
using ::arcae::detail::IndexSpan;
using ::arcae::detail::IndexType;
using ::arcae::detail::NewTableProxy;
using ::arcae::detail::SelectionBuilder;

using ::casacore::Array;
using ::casacore::ArrayColumnDesc;
using ::casacore::ColumnDesc;
using ::casacore::Complex;
using MS = casacore::MeasurementSet;
using ::casacore::Record;
using ::casacore::SetupNewTable;
using ::casacore::String;
using ::casacore::Table;
using ::casacore::TableDesc;
using ::casacore::TableLock;
using ::casacore::TableProxy;
using ::casacore::IPosition;

using namespace std::string_literals;

static constexpr std::size_t knrow = 10;
static constexpr std::size_t knchan = 16;
static constexpr std::size_t kncorr = 4;

static constexpr std::size_t kNDim = 3;
static constexpr std::array<std::size_t, kNDim> kDimensions = {kncorr, knchan, knrow};
static constexpr std::size_t kElements = knrow*knchan*kncorr;

namespace {

// Structure parametrizing a test case
struct Parametrization {
  // For each dimension, is the selection empty
  std::array<bool, kNDim> empty_selection = {false, false, false};
  // Indicates whether each dimension should be randomised
  std::array<bool, kNDim> randomise = {false, false, false};
  // For each dimension, number of elements to be removed
  std::array<std::size_t, kNDim> remove = {0, 0, 0};
};

class FixedTableProxyTest : public ::testing::TestWithParam<Parametrization> {
  protected:
    std::string table_name_;
    Array<Complex> complex_vals_;
    Array<String> str_vals_;

    void SetUp() override {
      auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info();
      table_name_ = test_info->name() + "-"s + arcae::hexuuid(4) + ".table"s;
      // Replace / in the Parametrized Test name with '~'
      for(int i = 0; i < table_name_.size(); ++i) {
        if(table_name_[i] == '/') table_name_[i] = '~';
      }
      auto table_desc = TableDesc(MS::requiredTableDesc());
      auto data_shape = IPosition({kncorr, knchan});

      auto data_column_desc = ArrayColumnDesc<Complex>(
        "MODEL_DATA", data_shape, ColumnDesc::FixedShape);
      table_desc.addColumn(data_column_desc);
      auto var_data_column_desc = ArrayColumnDesc<Complex>(
        "VAR_DATA", data_shape.size());
      table_desc.addColumn(var_data_column_desc);
      auto str_data_column_desc = ArrayColumnDesc<String>(
        "STRING_DATA", data_shape, ColumnDesc::FixedShape);
      table_desc.addColumn(str_data_column_desc);
      auto var_str_data_column_desc = ArrayColumnDesc<String>(
        "VAR_STRING_DATA", data_shape.size());
      table_desc.addColumn(var_str_data_column_desc);

      auto setup_new_table = SetupNewTable(table_name_, table_desc, Table::New);
      auto ms = MS(setup_new_table, knrow);
      auto data = GetArrayColumn<Complex>(ms, "MODEL_DATA");
      auto str_data = GetArrayColumn<String>(ms, "STRING_DATA");
      auto var_str_data = GetArrayColumn<String>(ms, "VAR_STRING_DATA");
      auto var_data = GetArrayColumn<Complex>(ms, "VAR_DATA");
      auto time = GetScalarColumn<casacore::Double>(ms, "TIME");

      // Initialise the time column
      for(std::size_t i = 0; i < knrow; ++i) time.put(i, i);

      // Initialise data and string data
      complex_vals_ = Array<Complex>(IPosition({kncorr, knchan, knrow}));
      str_vals_ = Array<String>(IPosition({kncorr, knchan, knrow}));

      for(std::size_t i = 0; i < complex_vals_.size(); ++i) {
        auto v = Complex::value_type(i);
        complex_vals_.data()[i] = {v, v};
        str_vals_.data()[i] = "FOO-" + std::to_string(i);
      }

      // Write test data to the MS
      data.putColumn(complex_vals_);
      var_data.putColumn(complex_vals_);
      str_data.putColumn(str_vals_);
      var_str_data.putColumn(str_vals_);
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

INSTANTIATE_TEST_SUITE_P(
  ParametrizationSuite,
  FixedTableProxyTest,
  ::testing::Values(
    Parametrization{{true, true, true}, {false, false, false}},
    Parametrization{{false, false, false}, {false, false, false}},
    Parametrization{{false, false, false}, {true, true, true}},
    Parametrization{{false, false, false}, {true, true, true}, {1, 3, 5}},
    Parametrization{{false, false, false}, {false, false, false}, {1, 3, 5}},
    Parametrization{{true, true, true}, {true, true, true}, {1, 3, 5}},
    Parametrization{{true, true, true}, {false, false, false}, {1, 3, 5}}));

TEST_P(FixedTableProxyTest, Fixed) {
  ASSERT_OK_AND_ASSIGN(auto ntp, OpenTable());
  const auto & params = GetParam();
  auto builder = SelectionBuilder().Order('F');

  EXPECT_EQ(kNDim, 3);
  std::vector<Index> indices(kNDim);

  for(std::size_t d = 0; d < kNDim; ++d) {
    // Generate a range of indices
    indices[d] = Index(kDimensions[d], 0);
    std::iota(std::begin(indices[d]), std::end(indices[d]), 0);

    if(params.empty_selection[d]) {
      // indices[d] corresponds to an empty selection
      builder.AddEmpty();
    } else {
      // Maybe shuffle the indices
      if(params.randomise[d]) {
        std::random_shuffle(std::begin(indices[d]), std::end(indices[d]));
      }
      // Maybe remove a number of values from the selection
      if(params.remove[d] > 0) {
        EXPECT_TRUE(params.remove[d] < kDimensions[d])
          << "Cannot remove more than "
          << kDimensions[d] << " indices "
          << "from dimension " << d;
        double ratio = double(kDimensions[d] - 1) / double(params.remove[d]);
        for(std::ptrdiff_t r = params.remove[d] - 1; r >= 0; --r) {
          indices[d].erase(std::begin(indices[d]) + int(round(r*ratio)));
        }
        EXPECT_EQ(indices[d].size(), kDimensions[d] - params.remove[d]);
      }
      builder.Add(indices[d]);
    }
  }

  auto selection = builder.Build();
  EXPECT_EQ(selection.Size(), kNDim);
  EXPECT_EQ(indices.size(), kNDim);

  // Sanity check that empty selection dimensions
  // align with the full index
  for(std::size_t d = 0; d < kNDim; ++d) {
    if(!params.empty_selection[d]) continue;
    EXPECT_FALSE(selection.FSpan(d).ok());
    auto result = selection.FSpan(d);
    Index expected(kDimensions[d], 0);
    std::iota(std::begin(expected), std::end(expected), 0);
    EXPECT_EQ(indices[d], expected);
  }

  auto & corrs = indices[0];
  auto & chans = indices[1];
  auto & rows = indices[2];

  ASSERT_OK_AND_ASSIGN(auto data, ntp->GetColumn("MODEL_DATA", selection));
  ASSERT_OK_AND_ASSIGN(auto var_data, ntp->GetColumn("VAR_DATA", selection));
  ASSERT_OK_AND_ASSIGN(auto str_data, ntp->GetColumn("STRING_DATA", selection));
  ASSERT_OK_AND_ASSIGN(auto var_str_data, ntp->GetColumn("VAR_STRING_DATA", selection));

  // Get the underlying buffer containing the complex values
  auto fixed_data_buffer = data->data()
                    ->child_data[0] // chan
                    ->child_data[0] // corr
                    ->child_data[0] // complex pair
                    ->buffers[1]    // value buffer
                    ->span_as<Complex>();

  auto var_data_buffer = var_data->data()
                    ->child_data[0] // chan
                    ->child_data[0] // corr
                    ->child_data[0] // complex pair
                    ->buffers[1]
                    ->span_as<Complex>();

  // Get the underyling string array
  str_data = arrow::MakeArray(str_data->data()
                    ->child_data[0]   // chan
                    ->child_data[0]); // corr
  var_str_data = arrow::MakeArray(var_str_data->data()
                    ->child_data[0]   // chan
                    ->child_data[0]); // corr

  auto nIndexElements = rows.size()*chans.size()*corrs.size();
  EXPECT_EQ(fixed_data_buffer.size(), nIndexElements);
  EXPECT_EQ(var_data_buffer.size(), nIndexElements);
  EXPECT_EQ(str_data->length(), nIndexElements);
  EXPECT_EQ(var_str_data->length(), nIndexElements);

  // Check that the expected value at the selected disk indices
  // matches the buffer values
  for(std::size_t r = 0; r < rows.size(); ++r) {
    auto row = rows[r];
    EXPECT_TRUE(row < knrow);  // sanity
    for(std::size_t ch = 0; ch < chans.size(); ++ch) {
      auto chan = chans[ch];
      EXPECT_TRUE(chan < knchan);  // sanity
      for(std::size_t co = 0; co < corrs.size(); ++co) {
        auto corr = corrs[co];
        EXPECT_TRUE(corr < kncorr);  // sanity
        auto buf_i = r*chans.size()*corrs.size() + ch*corrs.size() + co;
        auto real = row*knchan*kncorr + chan*kncorr + corr;
        EXPECT_EQ(complex_vals_(IPosition({corr, chan, row})), Complex(real, real));
        EXPECT_EQ(fixed_data_buffer[buf_i], Complex(real, real))
          << '[' << co << ',' << ch << ',' << r << ']' << ' '
          << '[' << corr << ',' << chan << ',' << row << ']'
          << ' ' << buf_i << ' ' << real;
        EXPECT_EQ(var_data_buffer[buf_i], Complex(real, real));
        ASSERT_OK_AND_ASSIGN(auto str_val, str_data->GetScalar(buf_i));
        EXPECT_EQ(str_val->ToString(), "FOO-" + std::to_string(real));
        ASSERT_OK_AND_ASSIGN(str_val, var_str_data->GetScalar(buf_i));
        EXPECT_EQ(str_val->ToString(), "FOO-" + std::to_string(real));
      }
    }
  }
}


class VariableProxyTest : public ::testing::TestWithParam<Parametrization> {
  protected:
    std::string table_name_;
    std::vector<casacore::IPosition> row_shapes_ = {{4, 16}, {3, 15}, {4, 14}, {3, 17}};
    std::vector<Array<Complex>> complex_vals_;
    std::vector<Array<String>> str_vals_;

    void SetUp() override {
      auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info();
      table_name_ = test_info->name() + "-"s + arcae::hexuuid(4) + ".table"s;
      // Replace / in the Parametrized Test name with '~'
      for(int i = 0; i < table_name_.size(); ++i) {
        if(table_name_[i] == '/') table_name_[i] = '~';
      }
      auto table_desc = TableDesc(MS::requiredTableDesc());
      auto var_data_column_desc = ArrayColumnDesc<Complex>(
        "VAR_DATA", 2);
      table_desc.addColumn(var_data_column_desc);
      auto var_str_data_column_desc = ArrayColumnDesc<String>(
        "VAR_STRING_DATA", 2);
      table_desc.addColumn(var_str_data_column_desc);

      auto setup_new_table = SetupNewTable(table_name_, table_desc, Table::New);
      auto ms = MS(setup_new_table, nRow());
      auto var_data = GetArrayColumn<Complex>(ms, "VAR_DATA");
      auto var_str_data = GetArrayColumn<String>(ms, "VAR_STRING_DATA");
      auto time = GetScalarColumn<casacore::Double>(ms, "TIME");

      // Initialise the time column
      for(std::size_t i = 0; i < nRow(); ++i) time.put(i, i);

      // Initialise data and string data
      // Write test data to the MS
      std::size_t real = 0;
      complex_vals_.resize(nRow());
      str_vals_.resize(nRow());

      for(std::size_t i = 0; i < nRow(); ++i) {
        complex_vals_[i] = Array<Complex>(row_shapes_[i]);
        str_vals_[i] = Array<String>(row_shapes_[i]);

        for(std::size_t s = 0; s < row_shapes_[i].product(); ++s) {
          complex_vals_[i].data()[s] = Complex(real, real);
          str_vals_[i].data()[s] = "FOO-" + std::to_string(real);
          ++real;
        }

        var_data.put(i, complex_vals_[i]);
        var_str_data.put(i, str_vals_[i]);
      }
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

    std::size_t RealValue(std::size_t row, std::size_t chan, std::size_t corr) const {
      assert(row < row_shapes_.size());
      const auto & shape = row_shapes_[row];
      auto nchan = shape[1];
      auto ncorr = shape[0];
      assert(chan < nchan);
      assert(corr < ncorr);
      std::size_t offset = chan*ncorr + corr;
      for(std::size_t r = 0; r < row; ++r) {
        offset += row_shapes_[r].product();
      }
      return offset;
    }

    std::size_t nRow() const { return row_shapes_.size(); }

    void TearDown() override {
      std::filesystem::remove_all(table_name_);
    }
};


INSTANTIATE_TEST_SUITE_P(
  ParametrizationSuite,
  VariableProxyTest,
  ::testing::Values(
    Parametrization{{true, true, true}, {false, false, false}},
    Parametrization{{false, false, false}, {false, false, false}},
    Parametrization{{false, false, false}, {true, true, true}},
    Parametrization{{false, false, false}, {true, true, true}, {1, 3, 2}},
    Parametrization{{false, false, false}, {false, false, false}, {1, 3, 2}},
    Parametrization{{true, true, true}, {true, true, true}, {1, 3, 5}},
    Parametrization{{true, true, true}, {false, false, false}, {1, 3, 2}})
    );

TEST_P(VariableProxyTest, Variable) {
  ASSERT_OK_AND_ASSIGN(auto ntp, OpenTable());
  EXPECT_TRUE(nRow() > 0);
  auto params = GetParam();

  // Derive minimum viable dimension size.
  // i.e. the maximum viable selection size along each dimension
  // as well as the maximum of all dimension sizes
  std::size_t max_dim = nRow();
  casacore::IPosition min_shape(kNDim, std::numeric_limits<ssize_t>::max());
  min_shape[2] = nRow();  // row dimension is trivial
  for(const auto & shape : row_shapes_) {
    EXPECT_EQ(shape.size(), kNDim - 1);
    for(std::size_t d = 0; d < shape.size(); ++d) {
      min_shape[d] = std::min(min_shape[d], shape[d]);
      max_dim = std::max<std::size_t>(max_dim, shape[d]);
    }
  }

  // Setup a dummy index for empty dimensions
  Index dummy_index(max_dim, 0);
  std::iota(std::begin(dummy_index), std::end(dummy_index), 0);

  ASSERT_OK_AND_ASSIGN(auto var_data, ntp->GetColumn("VAR_DATA"));
  ASSERT_OK_AND_ASSIGN(auto str_data, ntp->GetColumn("VAR_STRING_DATA"));

  auto builder = SelectionBuilder().Order('F');
  std::vector<Index> indices(kNDim);

  for(std::size_t d = 0; d < kNDim; ++d) {
    if(params.empty_selection[d]) {
      builder.AddEmpty();
    } else {
      indices[d] = Index(min_shape[d], 0);
      std::iota(std::begin(indices[d]), std::end(indices[d]), 0);

      // Shuffle the selection along this dimension
      if(params.randomise[d]) {
        std::random_shuffle(std::begin(indices[d]), std::end(indices[d]));
      }

      // Maybe remove a number of values from the selection
      if(params.remove[d] > 0) {
        EXPECT_TRUE(params.remove[d] < min_shape[d])
          << "Cannot remove more than "
          << min_shape[d] << " indices "
          << "from dimension " << d;
        double ratio = double(min_shape[d] - 1) / double(params.remove[d]);
        for(std::ptrdiff_t r = params.remove[d] - 1; r >= 0; --r) {
          indices[d].erase(std::begin(indices[d]) + int(round(r*ratio)));
        }
        EXPECT_EQ(indices[d].size(), min_shape[d] - params.remove[d]);
      }

      builder.Add(indices[d]);
    }
  }

  auto selection = builder.Build();
  EXPECT_EQ(selection.Size(), 3);
  ASSERT_OK_AND_ASSIGN(var_data, ntp->GetColumn("VAR_DATA", selection));
  ASSERT_OK_AND_ASSIGN(str_data, ntp->GetColumn("VAR_STRING_DATA", selection));

  auto GetIndexSpan = [&](auto dim, auto dim_size) -> IndexSpan {
    if(auto r = selection.FSpan(dim, kNDim); r.ok()) return r.ValueOrDie();
    return IndexSpan(dummy_index).subspan(0, dim_size);
  };

  auto var_data_buffer = var_data->data()
                    ->child_data[0] // chan
                    ->child_data[0] // corr
                    ->child_data[0] // complex pair
                    ->buffers[1]
                    ->span_as<Complex>();

  // Get the underyling string array
  str_data = arrow::MakeArray(str_data->data()
                    ->child_data[0]   // chan
                    ->child_data[0]); // corr

  auto rows = GetIndexSpan(2, nRow());
  std::size_t row_offset = 0;

  // Check that the expected value at the selected disk indices
  // matches the buffer values
  for(std::size_t r = 0; r < rows.size(); ++r) {
    auto row = rows[r];
    EXPECT_TRUE(row < nRow()); // sanity
    const auto & shape = row_shapes_[row];
    auto chans = GetIndexSpan(1, shape[1]);
    auto corrs = GetIndexSpan(0, shape[0]);
    for(std::size_t ch = 0; ch < chans.size(); ++ch) {
      auto chan = chans[ch];
      EXPECT_TRUE(chan < shape[1]);  // sanity
      for(std::size_t co = 0; co < corrs.size(); ++co) {
        auto corr = corrs[co];
        EXPECT_TRUE(corr < shape[0]);  // sanity
        auto real = RealValue(row, chan, corr);
        auto buf_i = row_offset + ch*corrs.size() + co;
        EXPECT_EQ(var_data_buffer[buf_i], Complex(real, real));
        ASSERT_OK_AND_ASSIGN(auto str_scalar, str_data->GetScalar(buf_i));
        EXPECT_EQ(str_scalar->ToString(), "FOO-" + std::to_string(real));
      }
    }
    // Advance row offset within the output buffer
    row_offset += chans.size() * corrs.size();
  }

  // All values in the output have been considered
  EXPECT_EQ(row_offset, var_data_buffer.size());
  EXPECT_EQ(row_offset, str_data->length());
}

}