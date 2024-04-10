#include "arcae/column_read_map.h"

#include <cassert>
#include <cstddef>
#include <iterator>
#include <functional>
#include <memory>
#include <optional>
#include <sys/types.h>
#include <vector>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include <casacore/casa/aipsxtype.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/tables/Tables/TableColumn.h>

#include "arcae/array_util.h"
#include "arcae/base_column_map.h"

namespace arcae {

namespace {

// Clip supplied shape based on the column selection
// The shape should not include the row dimension
arrow::Result<casacore::IPosition> ClipShape(
                                    const casacore::IPosition & shape,
                                    const ColumnSelection & selection) {
  // There's no selection, or only a row selection
  // so there's no need to clip the shapes
  if(selection.size() <= 1) {
    return shape;
  }

  auto clipped = shape;
  auto selection_size = std::ptrdiff_t(selection.size());

  for(std::size_t dim=0; dim < shape.size(); ++dim) {
    auto sdim = SelectDim(dim, selection.size(), shape.size() + 1);
    if(sdim >= 0 && sdim < selection_size && selection[sdim].size() > 0) {
      for(auto i: selection[sdim]) {
        if(i >= clipped[dim]) {
          return arrow::Status::Invalid("Selection index ", i,
                                        " exceeds dimension ", dim,
                                        " of shape ", clipped);
        }
      }

      clipped[dim] = selection[sdim].size();
    }
  }

  return clipped;
}

// Derive an output shape from the selection ranges
// This may not be possible for variably shaped columns
std::optional<casacore::IPosition>
MaybeMakeOutputShape(const ColumnRanges & ranges) {
  auto ndim = ranges.size();
  assert(ndim > 0);
  auto shape = casacore::IPosition(ndim, 0);

  for(auto dim=std::size_t{0}; dim < ndim; ++dim) {
    auto size = std::size_t{0};
    for(const auto & range: ranges[dim]) {
      switch(range.type) {
        case Range::FREE:
        case Range::MAP:
        case Range::EMPTY:
          assert(range.IsValid());
          size += range.Size();
          break;
        case Range::VARYING:
          return std::nullopt;
        default:
          assert(false && "Unhandled Range::Type enum");
      }
    }
    shape[dim] = size;
  }

  return shape;
}

// Make ranges for each dimension
arrow::Result<ColumnRanges>
RangeFactory(const ShapeProvider & shape_prov, const ColumnMaps & maps) {
  if(shape_prov.IsActuallyFixed()) {
    return FixedRangeFactory(shape_prov, maps);
  }

  return VariableRangeFactory(shape_prov, maps);
}

arrow::Result<casacore::IPosition> GetColumnRowShape(
  const casacore::TableColumn & column,
  const std::optional<ArrayProperties> & result_props,
  RowId row) {
    if(row < 0) {
      if(!result_props.has_value()) {
        return arrow::Status::Invalid(
          "Negative selection indices may only be present "
          "when a result array is provided");
      }
      const auto & rprops = result_props.value();
      if(!rprops.shape.has_value()) {
        return arrow::Status::NotImplemented(
          "Reconciliation of variably shaped indices");
      }
      const auto & shape = rprops.shape.value();
      // Return secondary dimensions
      return shape.getFirst(shape.size() - 1);
    }

    if(row >= RowId(column.nrow())) {
      return arrow::Status::IndexError(
        "Row ", row, " in column ",
        column.columnDesc().name(),
        " is out of bounds");
    }
    if(!column.isDefined(row)) {
      return arrow::Status::IndexError(
        "Row ", row, " in column ",
        column.columnDesc().name(),
        " is undefined");
    }
    return column.shape(row);
}

// Create offset arrays for arrow::ListArrays from casacore row shapes
arrow::Result<std::vector<std::shared_ptr<arrow::Int32Array>>>
MakeOffsets(const decltype(VariableShapeData::row_shapes_) & row_shapes) {
  auto nrow = row_shapes.size();
  // Number of dimensions without row
  auto ndim = std::begin(row_shapes)->size();
  auto builders = std::vector<arrow::Int32Builder>(ndim);
  auto offsets = std::vector<std::shared_ptr<arrow::Int32Array>>(ndim);

  for(std::size_t dim=0; dim < ndim; ++dim) {
    ARROW_RETURN_NOT_OK(builders[dim].Reserve(nrow + 1));
    ARROW_RETURN_NOT_OK(builders[dim].Append(0));
  }

  auto running_offsets = std::vector<std::size_t>(ndim, 0);

  // Compute number of elements in each row by making
  // a product over each dimension
  for(std::size_t row=0; row < nrow; ++row) {
    using ItType = std::tuple<std::ptrdiff_t, std::size_t>;
    for(auto [dim, product]=ItType{ndim - 1, 1}; dim >= 0; --dim) {
      auto dim_size = row_shapes[row][dim];
      for(std::size_t p=0; p < product; ++p) {
        running_offsets[dim] += dim_size;
        ARROW_RETURN_NOT_OK(builders[dim].Append(running_offsets[dim]));
      }
      product *= dim_size;
    }
  }

  // Build the offset arrays
  for(std::size_t dim=0; dim < ndim; ++dim) {
    ARROW_RETURN_NOT_OK(builders[dim].Finish(&offsets[dim]));
  }

  return offsets;
}


arrow::Result<std::vector<std::shared_ptr<arrow::Int32Array>>>
MakeFixedOffsets(const casacore::rownr_t nrow, const casacore::IPosition & shape) {
  auto ndim = shape.size();

  auto builders = std::vector<arrow::Int32Builder>(ndim);
  auto offsets = std::vector<std::shared_ptr<arrow::Int32Array>>(ndim);

  for(std::size_t dim=0; dim < ndim; ++dim) {
    ARROW_RETURN_NOT_OK(builders[dim].Reserve(nrow + 1));
    ARROW_RETURN_NOT_OK(builders[dim].Append(0));
  }

  auto running_offsets = std::vector<std::size_t>(ndim, 0);

  // Compute number of elements in each row by making
  // a product over each dimension
  for(casacore::rownr_t row=0; row < nrow; ++row) {
    using ItType = std::tuple<std::ptrdiff_t, std::size_t>;
    for(auto [dim, product]=ItType{ndim - 1, 1}; dim >= 0; --dim) {
      auto dim_size = shape[dim];
      for(std::size_t p=0; p < product; ++p) {
        running_offsets[dim] += dim_size;
        ARROW_RETURN_NOT_OK(builders[dim].Append(running_offsets[dim]));
      }
      product *= dim_size;
    }
  }

  // Build the offset arrays
  for(std::size_t dim=0; dim < ndim; ++dim) {
    ARROW_RETURN_NOT_OK(builders[dim].Finish(&offsets[dim]));
  }

  return offsets;

}

} // namespace

// Factory method for creating Variably Shape Data from column
arrow::Result<std::unique_ptr<VariableShapeData>>
VariableShapeData::Make(const casacore::TableColumn & column,
                        const ColumnSelection & selection,
                        const std::optional<ArrayProperties> & result_props) {
  assert(!column.columnDesc().isFixedShape());
  auto row_shapes = decltype(VariableShapeData::row_shapes_){};
  bool shapes_equal = true;
  // Row dimension is last in FORTRAN ordering
  auto row_dim = selection.size() - 1;
  using ItType = std::tuple<casacore::rownr_t, bool>;

  // Helper lambda for keeping track of whether row shapes are equal
  auto AreShapesEqual = [&](bool first) -> arrow::Result<bool> {
    if(first) return shapes_equal;
    if(row_shapes.front().size() != row_shapes.back().size()) {
      return arrow::Status::NotImplemented("Column ", column.columnDesc().name(),
                                           " dimensions vary per row.");
    }
    return shapes_equal && (row_shapes.back() == row_shapes.front());
  };

  // No selection
  // Create row shape data from column.nrow()
  if(selection.size() == 0 || selection[row_dim].size() == 0) {
    row_shapes.reserve(column.nrow());

    for(auto [r, first] = ItType{0, true}; r < column.nrow(); ++r, first=false) {
      ARROW_ASSIGN_OR_RAISE(auto shape, GetColumnRowShape(column, result_props, r))
      ARROW_ASSIGN_OR_RAISE(shape, ClipShape(shape, selection));
      row_shapes.emplace_back(std::move(shape));
      ARROW_ASSIGN_OR_RAISE(shapes_equal, AreShapesEqual(first));
    }
  } else {
    // Create row shape data from row id selection
    const auto & row_ids = selection[row_dim];
    row_shapes.reserve(row_ids.size());

    for(auto [r, first] = ItType{0, true}; r < row_ids.size(); ++r, first=false) {
      ARROW_ASSIGN_OR_RAISE(auto shape, GetColumnRowShape(column, result_props, row_ids[r]));
      ARROW_ASSIGN_OR_RAISE(shape, ClipShape(shape, selection));
      row_shapes.emplace_back(std::move(shape));
      ARROW_ASSIGN_OR_RAISE(shapes_equal, AreShapesEqual(first));
    }
  }

  if(row_shapes.size() == 0) {
    return arrow::Status::Invalid("No row shapes were found!");
  }

  // We may have a fixed shape in practice
  auto shape = shapes_equal ? std::make_optional(row_shapes.front())
                            : std::nullopt;

  ARROW_ASSIGN_OR_RAISE(auto offsets, MakeOffsets(row_shapes));

  auto ndim = row_shapes.front().size();

  return std::unique_ptr<VariableShapeData>(
    new VariableShapeData{std::move(row_shapes),
                          std::move(offsets),
                          ndim,
                          std::move(shape)});
}

bool VariableShapeData::IsActuallyFixed() const {
  return shape_.has_value();
}

std::size_t VariableShapeData::nDim() const {
  return ndim_;
}

arrow::Result<ShapeProvider>
ShapeProvider::Make(const casacore::TableColumn & column,
                    const ColumnSelection & selection,
                    const std::optional<ArrayProperties> & result_props) {

  if(column.columnDesc().isFixedShape()) {
    auto shape = column.columnDesc().shape();
    shape.append(casacore::IPosition({ssize_t(column.nrow())}));
    ARROW_RETURN_NOT_OK(CheckSelectionAgainstShape(shape, selection));
    return ShapeProvider{std::cref(column), selection};
  }

  ARROW_ASSIGN_OR_RAISE(auto var_data, VariableShapeData::Make(column, selection, result_props));
  return ShapeProvider{std::cref(column), std::cref(selection), std::move(var_data)};
}

// Returns the dimension size of this column
arrow::Result<std::size_t>
ShapeProvider::DimSize(std::size_t dim) const {
  const auto & sel = selection_.get();
  auto sdim = SelectDim(dim, sel.size(), nDim());
  // If we have a selection of row id's,
  // derive the dimension size from these
  if(sdim >= 0 && sel.size() > 0 && sel[sdim].size() > 0) {
    return sel[sdim].size();
  }

  assert(dim < nDim());

  // There's no selection for this dimension
  // so we must derive the dimension size
  // from the column shape information
  if(dim == RowDim()) {
    // First dimension is just row
    return column_.get().nrow();
  } else if(IsDefinitelyFixed()) {
    // Fixed shape column, we have the size information
    return column_.get().shapeColumn()[dim];
  } else {
    const auto & shape = var_data_->shape_;

    if(!shape) {
      return arrow::Status::IndexError("Dimension ", dim, " in  column ",
                                        column_.get().columnDesc().name(),
                                        " is not fixed.");
    }

    // Even though the column is marked as variable
    // the individual row shapes are the same
    return shape.value()[dim];
  }
}

// Returns the dimension size of the colum for the given row
std::size_t ShapeProvider::RowDimSize(casacore::rownr_t row, std::size_t dim) const {
  assert(IsVarying());
  assert(row < var_data_->row_shapes_.size());
  assert(dim < RowDim());
  return var_data_->row_shapes_[row][dim];
}


arrow::Result<std::vector<std::shared_ptr<arrow::Int32Array>>>
ColumnReadMap::GetOffsets() const {
  if(shape_provider_.IsVarying()) {
    return shape_provider_.var_data_->offsets_;
  }

  auto column_desc = column_.get().columnDesc();
  return MakeFixedOffsets(column_.get().nrow(), column_desc.shape());
}


std::size_t ColumnReadMap::FlatOffset(const std::vector<std::size_t> & index) const {
  if(output_shape_) {
    // Fixed shape output, easy case
    const auto & shape = output_shape_.value();
    auto result = std::size_t{0};
    auto product = std::size_t{1};

    for(std::size_t dim = 0; dim < RowDim(); ++dim) {
      result += index[dim] * product;
      product *= shape[dim];
    }

    return result + product * index[RowDim()];
  }
  // Variably shaped output
  const auto & shapes = shape_provider_.var_data_->row_shapes_;
  // Offset into the output starts at the row offset
  std::size_t row = index[RowDim()];
  std::size_t result = 0;
  std::size_t product = 1;

  // Sum shape products until the row of interest
  for(std::size_t r=0; r < row; ++r) {
    result += shapes[r].product();
  }

  // Then add in the offsets of the secondary dimensions
  for(std::size_t dim = 0; dim < RowDim(); ++dim) {
    result += product*index[dim];
    product *= shapes[row][dim];
  }

  return result;
}


// Factory method for making a ColumnReadMap object
arrow::Result<ColumnReadMap>
ColumnReadMap::Make(
    const casacore::TableColumn & column,
    ColumnSelection selection,
    const std::shared_ptr<arrow::Array> & result,
    MapOrder order) {

  // Convert to FORTRAN ordering used by casacore internals
  if(order == MapOrder::C_ORDER) {
    std::reverse(std::begin(selection), std::end(selection));
  }

  std::optional<ArrayProperties> result_props = std::nullopt;
  if(result) {
    ARROW_ASSIGN_OR_RAISE(result_props, GetArrayProperties(column, result));
  }

  ARROW_ASSIGN_OR_RAISE(auto shape_prov, ShapeProvider::Make(column, selection, result_props));
  auto maps = MapFactory(shape_prov, selection);
  ARROW_ASSIGN_OR_RAISE(auto ranges, RangeFactory(shape_prov, maps));

  if(ranges.size() == 0) {
    return arrow::Status::IndexError("Zero ranges generated for column ",
                                     column.columnDesc().name());
  }

  auto shape = MaybeMakeOutputShape(ranges);

  return ColumnReadMap{{std::cref(column), std::move(maps), std::move(ranges)},
                        std::move(shape_prov), std::move(shape), result};
}

} // namespace arcae
