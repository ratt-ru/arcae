#include "arcae/column_read_map.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <functional>
#include <memory>
#include <optional>
#include <sys/types.h>
#include <vector>

#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/array/array_nested.h>
#include <arrow/scalar.h>
#include <arrow/type.h>

#include <casacore/casa/aipsxtype.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/Arrays/Slicer.h>
#include <casacore/tables/Tables/ArrayColumnBase.h>
#include <casacore/tables/Tables/TableColumn.h>

namespace arcae {

namespace {

// Return a selection dimension given
//
// 1. FORTRAN ordered dim
// 2. Number of selection dimensions
// 3. Number of column dimensions
//
// A return of < 0 indicates a non-existent selection
std::ptrdiff_t SelectDim(std::size_t dim, std::size_t sdims, std::size_t ndims) {
  return std::ptrdiff_t(dim) + std::ptrdiff_t(sdims) - std::ptrdiff_t(ndims);
}


// Clip supplied shape based on the column selection
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
    if(sdim >= 0 && selection[sdim].size() > 0) {
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



// Create a Column Map from a selection of row id's in different dimensions
ColumnMaps MapFactory(const ShapeProvider & shape_prov, const ColumnSelection & selection) {
  ColumnMaps column_maps;
  auto ndim = shape_prov.nDim();
  column_maps.reserve(ndim);

  for(auto dim=std::size_t{0}; dim < ndim; ++dim) {
      // Dimension needs to be adjusted for
      // 1. We may not have selections matching all dimensions
      // 2. Selections are FORTRAN ordered
      auto sdim = SelectDim(dim, selection.size(), ndim);

      if(sdim < 0 || selection.size() == 0 || selection[sdim].size() == 0) {
        column_maps.emplace_back(ColumnMap{});
        continue;
      }

      const auto & dim_ids = selection[sdim];
      ColumnMap column_map;
      column_map.reserve(dim_ids.size());

      for(auto [disk_it, mem] = std::tuple{std::begin(dim_ids), casacore::rownr_t{0}};
          disk_it != std::end(dim_ids); ++mem, ++disk_it) {
            column_map.push_back({*disk_it, mem});
      }

      std::sort(std::begin(column_map), std::end(column_map),
                [](const auto & lhs, const auto & rhs) {
                  return lhs.disk < rhs.disk; });

      column_maps.emplace_back(std::move(column_map));
  }

  return column_maps;
}

// Make ranges for fixed shape columns
// In this case, each row has the same shape
// so we can make ranges that span multiple rows
arrow::Result<ColumnRanges>
FixedRangeFactory(const ShapeProvider & shape_prov, const ColumnMaps & maps) {
  assert(shape_prov.IsActuallyFixed());
  auto ndim = shape_prov.nDim();
  ColumnRanges column_ranges;
  column_ranges.reserve(ndim);

  for(std::size_t dim=0; dim < ndim; ++dim) {
    // If no mapping exists for this dimension, create a range
    // from the column shape
    if(dim >= maps.size() || maps[dim].size() == 0) {
      ARROW_ASSIGN_OR_RAISE(auto dim_size, shape_prov.DimSize(dim));
      column_ranges.emplace_back(ColumnRange{Range{0, dim_size, Range::FREE}});
      continue;
    }

    // A mapping exists for this dimension, create ranges
    // from contiguous segments
    const auto & column_map = maps[dim];
    auto column_range = ColumnRange{};
    auto current = Range{0, 1, Range::MAP};

    for(auto [i, prev, next] = std::tuple{
            casacore::rownr_t{1},
            std::begin(column_map),
            std::next(std::begin(column_map))};
        next != std::end(column_map); ++i, ++prev, ++next) {

      if(next->disk - prev->disk == 1) {
        current.end += 1;
      } else {
        column_range.push_back(current);
        current = Range{i, i + 1, Range::MAP};
      }
    }

    column_range.emplace_back(std::move(current));
    column_ranges.emplace_back(std::move(column_range));
  }

  assert(ndim == column_ranges.size());
  return column_ranges;
}

// Make ranges for variably shaped columns
// In this case, each row may have a different shape
// so we create a separate range for each row and VARYING
// ranges for other dimensions whose size cannot be determined.
arrow::Result<ColumnRanges>
VariableRangeFactory(const ShapeProvider & shape_prov, const ColumnMaps & maps) {
  assert(!shape_prov.IsActuallyFixed());
  auto ndim = shape_prov.nDim();
  auto row_dim = ndim - 1;
  ColumnRanges column_ranges;
  column_ranges.reserve(ndim);


  // Handle non-row dimensions first
  for(std::size_t dim=0; dim < row_dim; ++dim) {
    // If no mapping exists for this dimension
    // create a single VARYING range
    if(dim >= maps.size() || maps[dim].size() == 0) {
      column_ranges.emplace_back(ColumnRange{Range{0, 0, Range::VARYING}});
      continue;
    }

    // A mapping exists for this dimension, create ranges
    // from contiguous segments
    const auto & column_map = maps[dim];
    auto column_range = ColumnRange{};
    auto current = Range{0, 1, Range::MAP};

    for(auto [i, prev, next] = std::tuple{
            casacore::rownr_t{1},
            std::begin(column_map),
            std::next(std::begin(column_map))};
        next != std::end(column_map); ++i, ++prev, ++next) {

      if(next->disk - prev->disk == 1) {
        current.end += 1;
      } else {
        column_range.push_back(current);
        current = Range{i, i + 1, Range::MAP};
      }
    }

    column_range.emplace_back(std::move(current));
    column_ranges.emplace_back(std::move(column_range));
  }

  // Lastly, the row dimension
  auto row_range = ColumnRange{};

  // Split the row dimension into ranges of exactly one row
  if(maps.size() == 0 || maps[row_dim].size() == 0) {
    // No maps provided, derive from shape
    ARROW_ASSIGN_OR_RAISE(auto dim_size, shape_prov.DimSize(row_dim));
    row_range.reserve(dim_size);
    for(std::size_t r=0; r < dim_size; ++r) {
      row_range.emplace_back(Range{r, r + 1, Range::FREE});
    }
  } else {
    // Derive from mapping
    const auto & row_maps = maps[row_dim];
    row_range.reserve(row_maps.size());
    for(std::size_t r=0; r < row_maps.size(); ++r) {
      row_range.emplace_back(Range{r, r + 1, Range::MAP});
    }
  }

  column_ranges.emplace_back(std::move(row_range));


  assert(ndim == column_ranges.size());
  return column_ranges;
}

// Make ranges for each dimension
arrow::Result<ColumnRanges>
RangeFactory(const ShapeProvider & shape_prov, const ColumnMaps & maps) {
  if(shape_prov.IsActuallyFixed()) {
    return FixedRangeFactory(shape_prov, maps);
  }

  return VariableRangeFactory(shape_prov, maps);
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
          size += range.nRows();
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

} // namespace

arrow::Result<casacore::IPosition> GetColumnRowShape(
  const casacore::TableColumn & column,
  casacore::rownr_t row) {
    if(column.isDefined(row)) return column.shape(row);
    return arrow::Status::IndexError("Row ", row, " in column ",
                                    column.columnDesc().name(),
                                   " is undefined");
}


arrow::Result<casacore::IPosition> GetDataRowShape(
  const std::shared_ptr<arrow::Array> & data,
  const ColumnSelection & selection,
  casacore::rownr_t row) {

  if(!data->IsValid(row)) {
    return arrow::Status::IndexError("Row ", row, " in data ",
                                     data->ToString(),
                                     " is undefined");
  }

  // Select the cell at a particular row
  ARROW_ASSIGN_OR_RAISE(auto cell, data->GetScalar(row));
  auto cell_data = std::shared_ptr<arrow::Array>{nullptr};
  auto shape = std::vector<std::int64_t>{};

  // Find out the shape of the first dimension of this cell
  switch(cell->type->id()) {
    case arrow::Type::LARGE_LIST:
    {
      // List types give us a dimension size
      auto list_scalar = std::dynamic_pointer_cast<arrow::BaseListScalar>(cell);
      assert(list_scalar);
      cell_data = std::dynamic_pointer_cast<arrow::LargeListArray>(list_scalar->value);
      assert(cell_data);
      shape.emplace_back(cell_data->length());
      break;
    }

    case arrow::Type::LIST:
    {
      // List types give us a dimension size
      auto list_scalar = std::dynamic_pointer_cast<arrow::BaseListScalar>(cell);
      assert(list_scalar);
      cell_data = std::dynamic_pointer_cast<arrow::ListArray>(list_scalar->value);
      assert(cell_data);
      shape.emplace_back(cell_data->length());
      break;
    }
    case arrow::Type::FIXED_SIZE_LIST:
    {
      auto list_scalar = std::dynamic_pointer_cast<arrow::BaseListScalar>(cell);
      assert(list_scalar);
      cell_data = std::dynamic_pointer_cast<arrow::FixedSizeListArray>(list_scalar->value);
      assert(cell_data);
      shape.emplace_back(cell_data->length());
      break;
    }
    case arrow::Type::BOOL:
    case arrow::Type::UINT8:
    case arrow::Type::UINT16:
    case arrow::Type::UINT32:
    case arrow::Type::UINT64:
    case arrow::Type::INT8:
    case arrow::Type::INT16:
    case arrow::Type::INT32:
    case arrow::Type::INT64:
    case arrow::Type::FLOAT:
    case arrow::Type::DOUBLE:
    case arrow::Type::STRING:
        // If we have basic data arrays
        // the shape is trivial
        return casacore::IPosition({1});
    default:
        return arrow::Status::TypeError(
            "Shape derivation of ",
            cell->type->ToString(),
            " is not supported");
  }

  // The other dimensions are handled as arrays
  for(auto done=false; !done;) {
    switch(cell_data->type_id()) {
      case arrow::Type::LARGE_LIST:
      {
        // If we have homogenous lengths in this list, then
        // a valid dimension size can be derived
        auto base_list = std::dynamic_pointer_cast<arrow::LargeListArray>(cell_data);
        assert(base_list);
        std::int64_t dim_size = base_list->value_length(0);
        for(std::int64_t i=1; i < base_list->length(); ++i) {
          if(dim_size != base_list->value_length(i)) {
            return arrow::Status::Invalid("Heterogenous dimension size ",
                                          dim_size, " ", base_list->value_length(i),
                                          " in row ", row, " of input data");
          }
        }
        shape.emplace_back(dim_size);
        cell_data = base_list->values();
        break;
      }
      case arrow::Type::LIST:
      {
        // If we have homogenous lengths in this list, then
        // a valid dimension size can be derived
        auto base_list = std::dynamic_pointer_cast<arrow::ListArray>(cell_data);
        assert(base_list);
        std::int64_t dim_size = base_list->value_length(0);
        for(std::int64_t i=1; i < base_list->length(); ++i) {
          if(dim_size != base_list->value_length(i)) {
            return arrow::Status::Invalid("Heterogenous dimension size ",
                                          dim_size, " ", base_list->value_length(i),
                                          " in row ", row, " of input data");
          }
        }
        shape.emplace_back(dim_size);
        cell_data = base_list->values();
        break;
      }
      case arrow::Type::FIXED_SIZE_LIST:
      {
        auto base_list = std::dynamic_pointer_cast<arrow::FixedSizeListArray>(cell_data);
        assert(base_list);
        shape.emplace_back(base_list->value_length(0));
        cell_data = base_list->values();
        break;
      }
      case arrow::Type::BOOL:
      case arrow::Type::UINT8:
      case arrow::Type::UINT16:
      case arrow::Type::UINT32:
      case arrow::Type::UINT64:
      case arrow::Type::INT8:
      case arrow::Type::INT16:
      case arrow::Type::INT32:
      case arrow::Type::INT64:
      case arrow::Type::FLOAT:
      case arrow::Type::DOUBLE:
      case arrow::Type::STRING:
          done = true;
          break;
      default:
          return arrow::Status::TypeError(
              "Shape derivation of ",
              cell_data->type()->ToString(),
              " is not supported");
    }
  }

  // C-ORDER to FORTRAN-ORDER
  auto result = casacore::IPosition(shape.size(), 0);
  for(std::size_t dim=0; dim < shape.size(); ++dim) {
    auto fdim = shape.size() - dim - 1;
    result[fdim] = shape[dim];

    // Check that the selection indices don't exceed the data shape
    if(auto sdim = SelectDim(fdim, selection.size(), shape.size() + 1); sdim >= 0) {
      if(selection[sdim].size() > shape[dim]) {
        return arrow::Status::IndexError(
          "Number of selections ", selection[sdim].size(),
          " is greater than the dimension ", shape[dim],
          " of the input data");
      }
    }
  }

  return result;

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

// Set the casacore row shape of a variably shaped casacore column row
arrow::Status SetRowShape(casacore::ArrayColumnBase & column,
                 const ColumnSelection & selection,
                 casacore::rownr_t row,
                 const casacore::IPosition & shape)
{
  auto new_shape = shape;

  for(std::size_t dim=0; dim < shape.size(); ++dim) {
    casacore::rownr_t dim_size = shape[dim];
    if(auto sdim = SelectDim(dim, selection.size(), shape.size() + 1); sdim >= 0) {
      for(auto & index: selection[sdim]) {
        dim_size = std::max(dim_size, index + 1);
      }
    }
    new_shape[dim] = dim_size;
  }

  // Avoid set the shape if possible
  if(column.isDefined(row)) {
    auto column_shape = column.shape(row);
    if(column_shape.size() != new_shape.size() || new_shape > column_shape) {
      return arrow::Status::Invalid("Unable to set row ", row, " to shape ", new_shape,
                                     "column ", column.columnDesc().name(),
                                     ". The existing row shape is ", column.shape(row),
                                     " and changing this would destroy existing data");
    }

    return arrow::Status::OK();
  }

  column.setShape(row, new_shape);
  return arrow::Status::OK();
}

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
      row_shapes.emplace_back(std::move(shape));
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
      row_shapes.emplace_back(std::move(shape));
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
