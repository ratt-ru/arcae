#include "arcae/column_write_map.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <functional>
#include <memory>
#include <optional>
#include <sys/types.h>
#include <vector>

#include <arrow/util/logging.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/array/array_nested.h>
#include <arrow/scalar.h>
#include <arrow/type.h>

#include <casacore/casa/aipsxtype.h>
#include <casacore/casa/Utilities/DataType.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/tables/Tables/ArrayColumnBase.h>
#include <casacore/tables/Tables/TableColumn.h>


namespace arcae {

namespace {

// Reconcile Data Shape with selection indices to decide
// the shape of the column row.
// Selection indices might refer to indices that are greater
// than the data shape. This implies that the on-disk array
// is larger than the selected shape.
// Adjust shapes appropriately
void
ReconcileDataAndSelectionShape(const ColumnSelection & selection,
                               casacore::IPosition & shape) {
  // There's no selection, or only a row selection
  // so there's no need to reconcile shapes
  if(selection.size() <= 1) {
    return;
  }

  for(std::size_t dim=0; dim < shape.size(); ++dim) {
    auto sdim = SelectDim(dim, selection.size(), shape.size() + 1);
    if(sdim >= 0 && selection[sdim].size() > 0) {
      for(auto i: selection[sdim]) {
        shape[dim] = std::max(shape[dim], ssize_t(i) + 1);
      }
    }
  }
}

// Make ranges for each dimension
arrow::Result<ColumnRanges>
RangeFactory(const ArrowShapeProvider & shape_prov, const ColumnMaps & maps) {
  if(shape_prov.IsDataFixed()) {
    return FixedRangeFactory(shape_prov, maps);
  }

  return VariableRangeFactory(shape_prov, maps);
}


arrow::Status
SetVariableRowShapes(casacore::ArrayColumnBase & column,
                     const ColumnSelection & selection,
                     const std::shared_ptr<arrow::Array> & data,
                     const ArrowShapeProvider & shape_prov) {

  assert(!column.columnDesc().isFixedShape());
  // Row dimension is last in FORTRAN order
  auto select_row_dim = selection.size() - 1;

  // No selection
  if(selection.size() == 0 || selection[select_row_dim].size() == 0) {
    for(auto r = 0; r < data->length(); ++r) {
      if(!column.isDefined(r)) {
        auto shape = casacore::IPosition(shape_prov.nDim() - 1, 0);
        for(std::size_t d=0; d < shape.size(); ++d) shape[d] = shape_prov.RowDimSize(r, d);
        column.setShape(r, shape);
      }
    }
  } else {
    const auto & row_ids = selection[select_row_dim];

    for(std::size_t rid = 0; rid < row_ids.size(); ++rid) {
      auto r = row_ids[rid];
      if(!column.isDefined(r)) {
        auto shape = casacore::IPosition(shape_prov.nDim() - 1, 0);
        for(std::size_t d=0; d < shape.size(); ++d) shape[d] = shape_prov.RowDimSize(rid, d);
        ReconcileDataAndSelectionShape(selection, shape);
        column.setShape(r, shape);
      }
    }
  }

  return arrow::Status::OK();
}

} // namespace

arrow::Result<ArrowShapeProvider>
ArrowShapeProvider::Make(const casacore::TableColumn & column,
                         const ColumnSelection & selection,
                         const std::shared_ptr<arrow::Array> & data) {
  ARROW_ASSIGN_OR_RAISE(auto properties, GetArrayProperties(column, selection, data));
  return ArrowShapeProvider{std::cref(column),
                            std::cref(selection),
                            data,
                            std::move(properties.data_type),
                            std::move(properties.shape),
                            properties.ndim,
                            properties.is_complex};
}

// Returns the dimension size of the data
arrow::Result<std::size_t>
ArrowShapeProvider::DimSize(std::size_t dim) const {
  assert(dim < nDim());
  if(IsDataFixed()) {
    return shape_.value()[dim];
  } else if(dim == RowDim()) {
    return data_->length();
  }

  return arrow::Status::Invalid("Unable to obtain dimension size for "
                                "dim ", dim, ". Use RowDimSize instead");
}

// Returns the dimension size of the data for the given row
std::size_t ArrowShapeProvider::RowDimSize(casacore::rownr_t row, std::size_t dim) const {
  assert(dim < RowDim());
  auto cdim = std::ptrdiff_t(nDim()) - std::ptrdiff_t(dim) - 1 /* account for row */ - 1;

  auto tmp_data = data_;
  auto start = std::int64_t(row);
  auto end = start + 1;

  auto AdvanceAndGetDimSize = [&](auto list) -> std::size_t{
    if(list->length() == 0) {
      return 0;
    }

    // Derive dimension size from the first offset
    auto dim_size = list->value_length(start);

    // Assert that other offset diffs match
    for(auto i = start + 1; i < end; ++i) {
      assert(dim_size == list->value_length(i));
    }

    // Advance start and end
    start = list->value_offset(start);
    end = list->value_offset(end);

    return dim_size;
  };

  using ItType = std::tuple<bool, std::ptrdiff_t>;

  for(auto [done, current_dim]=ItType{false, 0}; !done; ++current_dim) {
    switch(tmp_data->type_id()) {
      case arrow::Type::LIST:
        {
          auto list = std::dynamic_pointer_cast<arrow::ListArray>(tmp_data);
          auto dim_size = AdvanceAndGetDimSize(list);
          if(current_dim == cdim) return dim_size;
          tmp_data = list->values();
          break;
        }
      case arrow::Type::LARGE_LIST:
        {
          auto list = std::dynamic_pointer_cast<arrow::LargeListArray>(tmp_data);
          auto dim_size = AdvanceAndGetDimSize(list);
          if(current_dim == cdim) return dim_size;
          tmp_data = list->values();
          break;
        }
      case arrow::Type::FIXED_SIZE_LIST:
        {
          auto list = std::dynamic_pointer_cast<arrow::FixedSizeListArray>(tmp_data);
          auto dim_size = AdvanceAndGetDimSize(list);
          if(current_dim == cdim) return dim_size;
          tmp_data = list->values();
          break;
        }
      default:
        done = true;
        break;
    }
  }

  /// TODO(sjperkins)
  /// If we've reached this point, we've interpreted Arrow's nested arrays
  /// incorrectly.
  ARROW_LOG(FATAL) << "Logical error in ArrowShapeProvider::RowDimSize";
  return -1;
}


std::size_t ColumnWriteMap::FlatOffset(const std::vector<std::size_t> & index) const {
  if(shape_provider_.shape_) {
    // Fixed shape output, easy case
    const auto & shape = shape_provider_.shape_.value();
    auto result = std::size_t{0};
    auto product = std::size_t{1};

    for(std::size_t dim = 0; dim < RowDim(); ++dim) {
      result += index[dim] * product;
      product *= shape[dim];
    }

    return result + product * index[RowDim()];
  }

  std::size_t row = index[RowDim()];
  std::size_t result = 0;
  std::size_t product = 1;

  // Sum shape products until the row of interest
  for(std::size_t r=0; r < row; ++r) {
    std::size_t p = 1;
    for(std::size_t d=0; d < RowDim(); ++d) p *= RowDimSize(r, d);
    result += p;
  }

  for(std::size_t dim=0; dim < RowDim(); ++dim) {
    result += product*index[dim];
    product *= RowDimSize(row, dim);
  }

  return result;
}


// Factory method for making a ColumnWriteMap object
arrow::Result<ColumnWriteMap>
ColumnWriteMap::Make(
    casacore::TableColumn & column,
    ColumnSelection selection,
    const std::shared_ptr<arrow::Array> & data,
    MapOrder order) {

  // Convert to FORTRAN ordering, used by casacore internals
  if(order == MapOrder::C_ORDER) {
    std::reverse(std::begin(selection), std::end(selection));
  }

  ARROW_ASSIGN_OR_RAISE(auto shape_prov, ArrowShapeProvider::Make(column, selection, data));
  auto maps = MapFactory(shape_prov, selection);

  if(shape_prov.IsColumnFixed()) {
    // If the column has a fixed shape, check up front that
    // the selection indices that we're writing to exist
    auto colshape = column.columnDesc().shape();
    colshape.append(casacore::IPosition({ssize_t(column.nrow())}));
    ARROW_RETURN_NOT_OK(CheckSelectionAgainstShape(colshape, selection));
  } else {
    // Otherwise we may be able to set the shape in the case
    // of variably shaped columns
    auto array_base = casacore::ArrayColumnBase(column);
    ARROW_RETURN_NOT_OK(SetVariableRowShapes(array_base, selection, data, shape_prov));
  }

  ARROW_ASSIGN_OR_RAISE(auto ranges, RangeFactory(shape_prov, maps));

  if(ranges.size() == 0) {
    return arrow::Status::ExecutionError("Zero ranges generated for column ",
                                          column.columnDesc().name());
  }

  return ColumnWriteMap{{std::cref(column), std::move(maps), std::move(ranges)},
                        data, std::move(shape_prov)};
}


} // namespace arcae
