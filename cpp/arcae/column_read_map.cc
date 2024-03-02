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

  for(std::size_t dim=0; dim < shape.size(); ++dim) {
    auto sdim = SelectDim(dim, selection.size(), shape.size() + 1);
    if(sdim >= 0 && sdim < selection.size() && selection[sdim].size() > 0) {
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
  auto row_dim = std::ptrdiff_t(ndim) - 1;
  auto shape = casacore::IPosition(ndim, 0);

  for(auto dim=std::size_t{0}; dim < ndim; ++dim) {
    auto size = std::size_t{0};
    for(const auto & range: ranges[dim]) {
      switch(range.type) {
        case Range::FREE:
        case Range::MAP:
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
  casacore::rownr_t row) {
    if(row >= column.nrow()) {
      return arrow::Status::IndexError("Row ", row, " in column ",
                                       column.columnDesc().name(),
                                       " is out of bounds");
    }
    if(column.isDefined(row)) return column.shape(row);
    return arrow::Status::IndexError("Row ", row, " in column ",
                                    column.columnDesc().name(),
                                   " is undefined");
}

// Create offset arrays for arrow::ListArrays from casacore row shapes
arrow::Result<std::vector<std::shared_ptr<arrow::Int32Array>>>
MakeOffsets(const decltype(VariableShapeData::row_shapes_) & row_shapes) {
  auto nrow = row_shapes.size();
  // Number of dimensions without row
  auto ndim = std::begin(row_shapes)->size();
  auto builders = std::vector<arrow::Int32Builder>(ndim);
  auto offsets = std::vector<std::shared_ptr<arrow::Int32Array>>(ndim);

  for(auto dim=0; dim < ndim; ++dim) {
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

} // namespace

// Factory method for creating Variably Shape Data from column
arrow::Result<std::unique_ptr<VariableShapeData>>
VariableShapeData::Make(const casacore::TableColumn & column,
                        const ColumnSelection & selection) {
  assert(!column.columnDesc().isFixedShape());
  auto row_shapes = decltype(VariableShapeData::row_shapes_){};
  bool fixed_shape = true;
  bool fixed_dims = true;
  // Row dimension is last in FORTRAN ordering
  auto row_dim = selection.size() - 1;
  using ItType = std::tuple<casacore::rownr_t, bool>;

  // No selection
  // Create row shape data from column.nrow()
  if(selection.size() == 0 || selection[row_dim].size() == 0) {
    row_shapes.reserve(column.nrow());

    for(auto [r, first] = ItType{0, true}; r < column.nrow(); ++r) {
      ARROW_ASSIGN_OR_RAISE(auto shape, GetColumnRowShape(column, r))
      ARROW_ASSIGN_OR_RAISE(shape, ClipShape(shape, selection));
      row_shapes.push_back(std::move(shape));
      if(first) { first = false; continue; }
      fixed_shape = fixed_shape && *std::rbegin(row_shapes) == *std::begin(row_shapes);
      fixed_dims = fixed_dims && std::rbegin(row_shapes)->size() == std::begin(row_shapes)->size();
    }
  } else {
    // Create row shape data from row id selection
    const auto & row_ids = selection[row_dim];
    row_shapes.reserve(row_ids.size());

    for(auto [r, first] = ItType{0, true}; r < row_ids.size(); ++r) {
      ARROW_ASSIGN_OR_RAISE(auto shape, GetColumnRowShape(column, row_ids[r]));
      ARROW_ASSIGN_OR_RAISE(shape, ClipShape(shape, selection));
      row_shapes.push_back(std::move(shape));
      if(first) { first = false; continue; }
      fixed_shape = fixed_shape && *std::rbegin(row_shapes) == *std::begin(row_shapes);
      fixed_dims = fixed_dims && std::rbegin(row_shapes)->size() == std::begin(row_shapes)->size();
    }
  }

  // Arrow can't handle differing dimensions per row, so we quit here.
  if(!fixed_dims) {
    return arrow::Status::NotImplemented("Column ", column.columnDesc().name(),
                                          " dimensions vary per row.");
  }

  // We may have a fixed shape in practice
  auto shape = fixed_shape ? std::make_optional(*std::begin(row_shapes))
                            : std::nullopt;

  ARROW_ASSIGN_OR_RAISE(auto offsets, MakeOffsets(row_shapes));

  return std::unique_ptr<VariableShapeData>(
    new VariableShapeData{std::move(row_shapes),
                          std::move(offsets),
                          std::begin(row_shapes)->size(),
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
                    const ColumnSelection & selection) {

  if(column.columnDesc().isFixedShape()) {
    auto shape = column.columnDesc().shape();
    shape.append(casacore::IPosition({ssize_t(column.nrow())}));
    ARROW_RETURN_NOT_OK(CheckSelectionAgainstShape(shape, selection));
    return ShapeProvider{std::cref(column), selection};
  }

  ARROW_ASSIGN_OR_RAISE(auto var_data, VariableShapeData::Make(column, selection));
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
  return arrow::Status::Invalid("Unable to retrieve varying offsets for "
                                "fixed column ", column_.get().columnDesc().name());
}


std::size_t ColumnReadMap::FlatOffset(const std::vector<std::size_t> & index) const {
  if(output_shape_) {
    // Fixed shape output, easy case
    const auto & shape = output_shape_.value();
    auto result = std::size_t{0};
    auto product = std::size_t{1};

    for(auto dim = 0; dim < RowDim(); ++dim) {
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
    MapOrder order) {

  // Convert to FORTRAN ordering, which the casacore internals use
  if(order == MapOrder::C_ORDER) {
    std::reverse(std::begin(selection), std::end(selection));
  }

  ARROW_ASSIGN_OR_RAISE(auto shape_prov, ShapeProvider::Make(column, selection));
  auto maps = MapFactory(shape_prov, selection);
  ARROW_ASSIGN_OR_RAISE(auto ranges, RangeFactory(shape_prov, maps));

  if(ranges.size() == 0) {
    return arrow::Status::ExecutionError("Zero ranges generated for column ",
                                          column.columnDesc().name());
  }

  auto shape = MaybeMakeOutputShape(ranges);

  return ColumnReadMap{column, std::move(maps), std::move(ranges),
                        std::move(shape_prov), std::move(shape)};
}

} // namespace arcae
