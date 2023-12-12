#include <memory>
#include <numeric>
#include <sys/types.h>

#include <arrow/api.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/BasicSL/Complexfwd.h>
#include <casacore/tables/Tables/ArrColDesc.h>
#include <casacore/tables/Tables/RefRows.h>
#include <casacore/tables/Tables/TableProxy.h>
#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/TableColumn.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>
#include <tests/test_utils.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <arrow/testing/gtest_util.h>

#include <arcae/column_mapper.h>
#include <arcae/safe_table_proxy.h>
#include <arcae/table_factory.h>


using arcae::ColumnMapping;
using casacore::Array;
using casacore::ArrayColumn;
using casacore::ArrayColumnDesc;
using casacore::ColumnDesc;
using CasaComplex = casacore::Complex;
using MS = casacore::MeasurementSet;
using MSColumns = casacore::MSMainEnums::PredefinedColumns;
using casacore::SetupNewTable;
using casacore::ScalarColumn;
using casacore::Slicer;
using casacore::Table;
using casacore::TableDesc;
using casacore::TableColumn;
using casacore::TableProxy;
using casacore::TiledColumnStMan;
using IPos = casacore::IPosition;

using namespace std::string_literals;

static constexpr std::size_t knrow = 10;
static constexpr std::size_t knchan = 4;
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

class ColumnConvertTest : public ::testing::Test {
  protected:
    std::shared_ptr<arcae::SafeTableProxy> table_proxy_;
    std::string table_name_;
    std::size_t nelements_;

    void SetUp() override {
      auto factory = [this]() -> arrow::Result<std::shared_ptr<TableProxy>> {
        auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        table_name_ = std::string(test_info->name() + "-"s + arcae::hexuuid(4) + ".table"s);

        auto table_desc = TableDesc(MS::requiredTableDesc());
        auto data_shape = IPos({kncorr, knchan});
        auto tile_shape = IPos({kncorr, knchan, 1});
        auto data_column_desc = ArrayColumnDesc<CasaComplex>(
            "MODEL_DATA", data_shape, ColumnDesc::FixedShape);

        auto var_column_desc = ArrayColumnDesc<CasaComplex>(
            "VAR_DATA", 2);

        auto var_fixed_column_desc = ArrayColumnDesc<CasaComplex>(
            "VAR_FIXED_DATA", 2);

        table_desc.addColumn(data_column_desc);
        table_desc.addColumn(var_column_desc);
        table_desc.addColumn(var_fixed_column_desc);
        auto storage_manager = TiledColumnStMan("TiledModelData", tile_shape);
        auto setup_new_table = SetupNewTable(table_name_, table_desc, Table::New);
        setup_new_table.bindColumn("MODEL_DATA", storage_manager);
        auto ms = MS(setup_new_table, knrow);

        auto field = GetScalarColumn<casacore::Int>(ms, MS::FIELD_ID);
        auto ddid = GetScalarColumn<casacore::Int>(ms, MS::DATA_DESC_ID);
        auto scan = GetScalarColumn<casacore::Int>(ms, MS::SCAN_NUMBER);
        auto time = GetScalarColumn<casacore::Double>(ms, MS::TIME);
        auto ant1 = GetScalarColumn<casacore::Int>(ms, MS::ANTENNA1);
        auto ant2 = GetScalarColumn<casacore::Int>(ms, MS::ANTENNA2);
        auto data = GetArrayColumn<CasaComplex>(ms, MS::MODEL_DATA);
        auto var_data = GetArrayColumn<CasaComplex>(ms, "VAR_DATA");
        auto var_fixed_data = GetArrayColumn<CasaComplex>(ms, "VAR_FIXED_DATA");

        time.putColumn({0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0});
        field.putColumn({0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        ddid.putColumn({0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        ant1.putColumn({0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        ant2.putColumn({1, 1, 1, 1, 1, 1, 1, 1, 1, 1});
        data.putColumn(Array<CasaComplex>(IPos({kncorr, knchan, knrow}), {1, 2}));
        var_fixed_data.putColumn(Array<CasaComplex>(IPos({kncorr, knchan, knrow}), {1, 2}));

        auto varshapes = std::vector<casacore::IPosition>{
          {3, 2, 1}, {4, 1, 1}, {4, 2, 1}, {2, 2, 1}, {2, 1, 1},
          {3, 2, 1}, {4, 1, 1}, {4, 2, 1}, {2, 2, 1}, {2, 1, 1}};

        assert(varshapes.size() == knrow);

        nelements_ = std::accumulate(std::begin(varshapes), std::end(varshapes), std::size_t{0},
                                     [](auto init, auto & shape) -> std::size_t
                                      { return init + shape.product(); });

        for(std::size_t i=0; i < knrow; ++i) {
          auto corrected_array = Array<CasaComplex>(
                  varshapes[i],
                  {static_cast<float>(i), static_cast<float>(i)});

          var_data.putColumnCells(casacore::RefRows(i, i), corrected_array);
        }

        return std::make_shared<TableProxy>(ms);
      };

      ASSERT_OK_AND_ASSIGN(table_proxy_, arcae::SafeTableProxy::Make(factory));
    }
};

TEST_F(ColumnConvertTest, SelectFromRange) {
  table_proxy_.reset();

  auto lock = casacore::TableLock(casacore::TableLock::LockOption::AutoNoReadLocking);
  auto lockoptions = casacore::Record();
  lockoptions.define("option", "auto");
  lockoptions.define("internal", lock.interval());
  lockoptions.define("maxwait", casacore::Int(lock.maxWait()));
  auto proxy = casacore::TableProxy(table_name_, lockoptions, casacore::Table::Old);

  {
    // Select all data from the VAR_DATA column
    auto data_column = GetArrayColumn<CasaComplex>(proxy.table(), "VAR_DATA");
    ASSERT_OK_AND_ASSIGN(auto map, ColumnMapping::Make(data_column, arcae::ColumnSelection{{}}));
    ASSERT_TRUE(map.shape_provider_.IsVarying());
    ASSERT_FALSE(map.shape_provider_.IsDefinitelyFixed());
    ASSERT_FALSE(map.shape_provider_.IsActuallyFixed());
    ASSERT_EQ(map.shape_provider_.nDim(), 3);
    ASSERT_EQ(map.ranges_.size(), 3);
    ASSERT_EQ(map.nRanges(), 10);
    ASSERT_EQ(map.nElements(), 24 + 24);
    ASSERT_EQ(map.shape_provider_.var_data_->row_shapes_.size(), 10);
    EXPECT_THAT(map.shape_provider_.var_data_->row_shapes_,
                ::testing::ElementsAre(IPos{3, 2}, IPos{4, 1}, IPos{4, 2}, IPos{2, 2}, IPos{2, 1},
                                       IPos{3, 2}, IPos{4, 1}, IPos{4, 2}, IPos{2, 2}, IPos{2, 1}));

    ASSERT_EQ(map.shape_provider_.var_data_->offsets_.size(), 2);
    arrow::Int32Builder builder;
    ASSERT_OK(builder.AppendValues({0, 3, 6, 10, 14, 18, 20, 22, 24, 27, 30, 34, 38, 42, 44, 46, 48}));
    ASSERT_OK_AND_ASSIGN(auto offsets1, builder.Finish());
    ASSERT_TRUE(offsets1->Equals(map.shape_provider_.var_data_->offsets_[0]));
    builder.Reset();
    ASSERT_OK(builder.AppendValues({0, 2, 3, 5, 7, 8,  10, 11, 13, 15, 16}));
    ASSERT_OK_AND_ASSIGN(auto offsets2, builder.Finish());
    ASSERT_TRUE(offsets2->Equals(map.shape_provider_.var_data_->offsets_[1]));

    for(auto [r, rit]=std::tuple{0, map.RangeBegin()}; rit != map.RangeEnd(); ++rit, ++r) {
      ASSERT_EQ(rit.GetRowSlicer(), Slicer(IPos({r}), IPos({r}), Slicer::endIsLast));
      ASSERT_EQ(rit.GetSectionSlicer().length(), map.shape_provider_.var_data_->row_shapes_[r]);
      data_column.getColumnRange(rit.GetRowSlicer(), rit.GetSectionSlicer());
    }
  }

  {
    // Select some rows from the VAR_DATA column
    auto data_column = GetArrayColumn<CasaComplex>(proxy.table(), "VAR_DATA");
    auto row_ids = arcae::RowIds{0, 1, 2, 3, 6, 7, 8, 9};
    ASSERT_OK_AND_ASSIGN(auto map, ColumnMapping::Make(data_column, arcae::ColumnSelection{{row_ids}}));

    ASSERT_TRUE(map.shape_provider_.IsVarying());
    ASSERT_FALSE(map.shape_provider_.IsDefinitelyFixed());
    ASSERT_FALSE(map.shape_provider_.IsActuallyFixed());
    ASSERT_EQ(map.shape_provider_.nDim(), 3);
    ASSERT_EQ(map.ranges_.size(), 3);
    ASSERT_EQ(map.ranges_[2].size(), 8);
    ASSERT_EQ(map.nRanges(), 8);
    ASSERT_EQ(map.nElements(), 22 + 18);
    ASSERT_EQ(map.shape_provider_.var_data_->row_shapes_.size(), 8);
    EXPECT_THAT(map.shape_provider_.var_data_->row_shapes_,
                ::testing::ElementsAre(IPos{3, 2}, IPos{4, 1}, IPos{4, 2}, IPos{2, 2},
                                       IPos{4, 1}, IPos{4, 2}, IPos{2, 2}, IPos{2, 1}));

    ASSERT_EQ(map.shape_provider_.var_data_->offsets_.size(), 2);
    arrow::Int32Builder builder;
    ASSERT_OK(builder.AppendValues({0, 3, 6, 10, 14, 18, 20, 22, 26, 30, 34, 36, 38, 40}));
    ASSERT_OK_AND_ASSIGN(auto offsets1, builder.Finish());
    ASSERT_TRUE(offsets1->Equals(map.shape_provider_.var_data_->offsets_[0]));
    builder.Reset();
    ASSERT_OK(builder.AppendValues({0, 2, 3, 5, 7, 8, 10, 12, 13}));
    ASSERT_OK_AND_ASSIGN(auto offsets2, builder.Finish());
    ASSERT_TRUE(offsets2->Equals(map.shape_provider_.var_data_->offsets_[1]));

    for(auto [r, rit]=std::tuple{0, map.RangeBegin()}; rit != map.RangeEnd(); ++rit, ++r) {
      auto rid = static_cast<ssize_t>(row_ids[r]);
      ASSERT_EQ(rit.GetRowSlicer(), Slicer(IPos({rid}), IPos({rid}), Slicer::endIsLast));
      ASSERT_EQ(rit.GetSectionSlicer().length(), map.shape_provider_.var_data_->row_shapes_[r]);
      data_column.getColumnRange(rit.GetRowSlicer(), rit.GetSectionSlicer());
    }
  }


  {
    // Select some rows and a channel from the VAR_DATA column
    auto data_column = GetArrayColumn<CasaComplex>(proxy.table(), "VAR_DATA");
    auto row_ids = arcae::RowIds{0, 1, 2, 3, 6, 7, 8, 9};
    auto chan_ids = arcae::RowIds{0};
    ASSERT_OK_AND_ASSIGN(auto map, ColumnMapping::Make(data_column, arcae::ColumnSelection{{row_ids, chan_ids}}));

    ASSERT_TRUE(map.shape_provider_.IsVarying());
    ASSERT_FALSE(map.shape_provider_.IsDefinitelyFixed());
    ASSERT_FALSE(map.shape_provider_.IsActuallyFixed());
    ASSERT_EQ(map.shape_provider_.nDim(), 3);
    ASSERT_EQ(map.ranges_.size(), 3);
    ASSERT_EQ(map.ranges_[2].size(), 8);
    ASSERT_EQ(map.nRanges(), 8);
    ASSERT_EQ(map.nElements(), 25);
    ASSERT_EQ(map.shape_provider_.var_data_->row_shapes_.size(), 8);
    EXPECT_THAT(map.shape_provider_.var_data_->row_shapes_,
                ::testing::ElementsAre(IPos{3, 1}, IPos{4, 1}, IPos{4, 1}, IPos{2, 1},
                                       IPos{4, 1}, IPos{4, 1}, IPos{2, 1}, IPos{2, 1}));

    ASSERT_EQ(map.shape_provider_.var_data_->offsets_.size(), 2);
    arrow::Int32Builder builder;
    ASSERT_OK(builder.AppendValues({0, 3, 7, 11, 13, 17, 21, 23, 25}));
    ASSERT_OK_AND_ASSIGN(auto offsets1, builder.Finish());
    ASSERT_TRUE(offsets1->Equals(map.shape_provider_.var_data_->offsets_[0]));
    builder.Reset();
    ASSERT_OK(builder.AppendValues({0, 1, 2, 3, 4, 5, 6, 7, 8}));
    ASSERT_OK_AND_ASSIGN(auto offsets2, builder.Finish());
    ASSERT_TRUE(offsets2->Equals(map.shape_provider_.var_data_->offsets_[1]));

    for(auto [r, rit]=std::tuple{0, map.RangeBegin()}; rit != map.RangeEnd(); ++rit, ++r) {
      auto rid = static_cast<ssize_t>(row_ids[r]);
      ASSERT_EQ(rit.GetRowSlicer(), Slicer(IPos({rid}), IPos({rid}), Slicer::endIsLast));
      ASSERT_EQ(rit.GetSectionSlicer().length(),
                IPos({map.shape_provider_.var_data_->row_shapes_[r][0], 1}));
      data_column.getColumnRange(rit.GetRowSlicer(), rit.GetSectionSlicer());
    }
  }


  {
    // Select some rows and a corr from the VAR_DATA column
    auto data_column = GetArrayColumn<CasaComplex>(proxy.table(), "VAR_DATA");
    auto row_ids = arcae::RowIds{0, 1, 2, 3, 6, 7, 8, 9};
    auto corr_ids = arcae::RowIds{0};
    ASSERT_OK_AND_ASSIGN(auto map, ColumnMapping::Make(data_column, arcae::ColumnSelection{{row_ids, {}, corr_ids}}));

    ASSERT_TRUE(map.shape_provider_.IsVarying());
    ASSERT_FALSE(map.shape_provider_.IsDefinitelyFixed());
    ASSERT_FALSE(map.shape_provider_.IsActuallyFixed());
    ASSERT_EQ(map.shape_provider_.nDim(), 3);
    ASSERT_EQ(map.ranges_.size(), 3);
    ASSERT_EQ(map.ranges_[2].size(), 8);
    ASSERT_EQ(map.nRanges(), 8);
    ASSERT_EQ(map.nElements(), 13);
    ASSERT_EQ(map.shape_provider_.var_data_->row_shapes_.size(), 8);
    EXPECT_THAT(map.shape_provider_.var_data_->row_shapes_,
                ::testing::ElementsAre(IPos{1, 2}, IPos{1, 1}, IPos{1, 2}, IPos{1, 2},
                                       IPos{1, 1}, IPos{1, 2}, IPos{1, 2}, IPos{1, 1}));

    ASSERT_EQ(map.shape_provider_.var_data_->offsets_.size(), 2);
    arrow::Int32Builder builder;
    ASSERT_OK(builder.AppendValues({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}));
    ASSERT_OK_AND_ASSIGN(auto offsets1, builder.Finish());
    ASSERT_TRUE(offsets1->Equals(map.shape_provider_.var_data_->offsets_[0]));
    builder.Reset();
    ASSERT_OK(builder.AppendValues({0, 2, 3, 5, 7, 8, 10, 12, 13}));
    ASSERT_OK_AND_ASSIGN(auto offsets2, builder.Finish());
    ASSERT_TRUE(offsets2->Equals(map.shape_provider_.var_data_->offsets_[1]));

    for(auto [r, rit]=std::tuple{0, map.RangeBegin()}; rit != map.RangeEnd(); ++rit, ++r) {
      auto rid = static_cast<ssize_t>(row_ids[r]);
      ASSERT_EQ(rit.GetRowSlicer(), Slicer(IPos({rid}), IPos({rid}), Slicer::endIsLast));
      ASSERT_EQ(rit.GetSectionSlicer().length(),
                IPos({1, map.shape_provider_.var_data_->row_shapes_[r][1]}));
      data_column.getColumnRange(rit.GetRowSlicer(), rit.GetSectionSlicer());
    }
  }

  {
    // Select some rows a channel and a correlation in the VAR_DATA column
    auto data_column = GetArrayColumn<CasaComplex>(proxy.table(), "VAR_DATA");
    auto row_ids = arcae::RowIds{0, 1, 2, 3, 6, 7, 8, 9};
    auto selection = arcae::ColumnSelection{row_ids, {0}, {0}};
    ASSERT_OK_AND_ASSIGN(auto map, ColumnMapping::Make(data_column, selection));

    ASSERT_TRUE(map.shape_provider_.IsVarying());
    ASSERT_FALSE(map.shape_provider_.IsDefinitelyFixed());
    ASSERT_TRUE(map.shape_provider_.IsActuallyFixed());
    ASSERT_EQ(map.shape_provider_.nDim(), 3);
    ASSERT_EQ(map.ranges_.size(), 3);
    ASSERT_EQ(map.ranges_[2].size(), 2);
    ASSERT_EQ(map.nRanges(), 2);
    ASSERT_EQ(map.nElements(), 8);
    ASSERT_EQ(map.shape_provider_.var_data_->row_shapes_.size(), 8);
    EXPECT_THAT(map.shape_provider_.var_data_->row_shapes_,
                ::testing::ElementsAre(IPos{1, 1}, IPos{1, 1}, IPos{1, 1}, IPos{1, 1},
                                       IPos{1, 1}, IPos{1, 1}, IPos{1, 1}, IPos{1, 1}));

    ASSERT_EQ(map.shape_provider_.var_data_->offsets_.size(), 2);
    arrow::Int32Builder builder;
    ASSERT_OK(builder.AppendValues({0, 1, 2, 3, 4, 5, 6, 7, 8}));
    ASSERT_OK_AND_ASSIGN(auto offsets1, builder.Finish());
    ASSERT_TRUE(offsets1->Equals(map.shape_provider_.var_data_->offsets_[0]));
    builder.Reset();
    ASSERT_OK(builder.AppendValues({0, 1, 2, 3, 4, 5, 6, 7, 8}));
    ASSERT_OK_AND_ASSIGN(auto offsets2, builder.Finish());
    ASSERT_TRUE(offsets2->Equals(map.shape_provider_.var_data_->offsets_[1]));

    auto rit = map.RangeBegin();
    ASSERT_EQ(rit.GetRowSlicer(), Slicer(IPos({0}), IPos({3}), Slicer::endIsLast));
    ASSERT_EQ(rit.GetSectionSlicer().length(), IPos({1, 1}));
    data_column.getColumnRange(rit.GetRowSlicer(), rit.GetSectionSlicer());
    ++rit;
    ASSERT_EQ(rit.GetRowSlicer(), Slicer(IPos({6}), IPos({9}), Slicer::endIsLast));
    ASSERT_EQ(rit.GetSectionSlicer().length(), IPos({1, 1}));
    data_column.getColumnRange(rit.GetRowSlicer(), rit.GetSectionSlicer());
    ++rit;
    ASSERT_EQ(rit, map.RangeEnd());
  }


  {
    // Select all data in the VAR_FIXED_DATA column
    // Even though VAR_FIXED_DATA is defined as variable the data
    // is treated as fixed
    auto data_column = GetArrayColumn<CasaComplex>(proxy.table(), "VAR_FIXED_DATA");
    ASSERT_OK_AND_ASSIGN(auto map, ColumnMapping::Make(data_column, arcae::ColumnSelection{{}}));

    ASSERT_TRUE(map.shape_provider_.IsVarying());
    ASSERT_FALSE(map.shape_provider_.IsDefinitelyFixed());
    ASSERT_TRUE(map.shape_provider_.IsActuallyFixed());
    ASSERT_EQ(map.shape_provider_.nDim(), 3);
    ASSERT_EQ(map.ranges_.size(), 3);
    ASSERT_EQ(map.nRanges(), 1);
    ASSERT_EQ(map.nElements(), kncorr*knchan*knrow);
    ASSERT_EQ(map.shape_provider_.var_data_->row_shapes_.size(), knrow);
    EXPECT_THAT(map.shape_provider_.var_data_->row_shapes_,
                ::testing::Contains(IPos{kncorr, knchan}).Times(knrow));

    auto rit = map.RangeBegin();
    ASSERT_EQ(rit.GetRowSlicer(), Slicer(IPos({0}), IPos({knrow - 1}), Slicer::endIsLast));
    ASSERT_EQ(rit.GetSectionSlicer(), Slicer(IPos({0, 0}), IPos({kncorr - 1, knchan - 1}), Slicer::endIsLast));
    ++rit;
    ASSERT_EQ(rit, map.RangeEnd());
  }

  {
    // Select all data in a MODEL_DATA, which is defined as fixed
    auto data_column = GetArrayColumn<CasaComplex>(proxy.table(), MS::MODEL_DATA);
    ASSERT_OK_AND_ASSIGN(auto map, ColumnMapping::Make(data_column, arcae::ColumnSelection{{}}));

    ASSERT_FALSE(map.shape_provider_.IsVarying());
    ASSERT_TRUE(map.shape_provider_.IsDefinitelyFixed());
    ASSERT_TRUE(map.shape_provider_.IsActuallyFixed());
    ASSERT_EQ(map.shape_provider_.nDim(), 3);
    ASSERT_EQ(map.ranges_.size(), 3);
    ASSERT_EQ(map.nRanges(), 1);
    ASSERT_EQ(map.nElements(), kncorr*knchan*knrow);
    ASSERT_EQ(map.shape_provider_.var_data_, nullptr);

    auto rit = map.RangeBegin();
    ASSERT_EQ(rit.GetRowSlicer(), Slicer(IPos({0}), IPos({knrow - 1}), Slicer::endIsLast));
    ASSERT_EQ(rit.GetSectionSlicer(), Slicer(IPos({0, 0}), IPos({kncorr - 1, knchan - 1}), Slicer::endIsLast));
    ++rit;
    ASSERT_EQ(rit, map.RangeEnd());
  }
}
