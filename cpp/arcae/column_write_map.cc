#include "arcae/column_write_map.h"
#include "arcae/map_iterator.h"

#include <casacore/casa/Utilities/DataType.h>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <functional>
#include <memory>
#include <numeric>
#include <optional>
#include <sys/types.h>
#include <type_traits>
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
ColumnMaps MapFactory(const ArrowShapeProvider & shape_prov, const ColumnSelection & selection) {
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

// Make ranges for fixed shape data
// In this case, each row has the same shape
// so we can make ranges that span multiple rows
arrow::Result<ColumnRanges>
FixedRangeFactory(const ArrowShapeProvider & shape_prov, const ColumnMaps & maps) {
  assert(shape_prov.IsDataFixed());
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

// Make ranges for variably shaped data
// In this case, each row may have a different shape
// so we create a separate range for each row and VARYING
// ranges for other dimensions whose size cannot be determined.
arrow::Result<ColumnRanges>
VariableRangeFactory(const ArrowShapeProvider & shape_prov, const ColumnMaps & maps) {
  assert(shape_prov.IsDataVarying());
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
RangeFactory(const ArrowShapeProvider & shape_prov, const ColumnMaps & maps) {
  if(shape_prov.IsDataFixed()) {
    return FixedRangeFactory(shape_prov, maps);
  }

  return VariableRangeFactory(shape_prov, maps);
}

arrow::Result<casacore::IPosition> GetColumnRowShape(
  const casacore::TableColumn & column,
  casacore::rownr_t row) {
    if(column.isDefined(row)) return column.shape(row);
    return arrow::Status::IndexError("Row ", row, " in column ",
                                    column.columnDesc().name(),
                                   " is undefined");
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

  // Avoid setting the shape if possible
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


} // namespace


// Get the shape and Number of dimensions
// from the input data
arrow::Result<std::tuple<std::optional<casacore::IPosition>, std::size_t>> GetShapeAndNdim(
  const casacore::TableColumn & column,
  const ColumnSelection & selection,
  const std::shared_ptr<arrow::Array> & data)
{
  auto fixed_shape = true;
  auto shape = std::vector<std::int64_t>{};
  auto tmp_data = data;
  auto ndim = std::size_t{0};

  auto MaybeUpdateShapeAndNdim = [&](auto list) -> arrow::Result<std::shared_ptr<arrow::Array>> {
    ++ndim;
    using ListType = std::decay<decltype(list)>;

    assert(list->null_count() == 0);

    if(!fixed_shape || list->length() == 0) {
      fixed_shape = false;
      return list->values();
    }

    auto dim_size = list->value_length(0);

    if constexpr(!std::is_same_v<ListType, std::shared_ptr<arrow::FixedSizeListArray>>) {
      for(std::int64_t i=0; i < list->length(); ++i) {
        if(dim_size != list->value_length(i)) {
          fixed_shape = false;
          return list->values();
        }
      }
    }

    shape.emplace_back(dim_size);
    return list->values();
  };

  arrow::Type::type type_id;

  for(auto done=false; !done;) {
    switch(tmp_data->type_id()) {
      case arrow::Type::LARGE_LIST:
      {
        auto base_list = std::dynamic_pointer_cast<arrow::LargeListArray>(tmp_data);
        assert(base_list);
        ARROW_ASSIGN_OR_RAISE(tmp_data, MaybeUpdateShapeAndNdim(base_list));
        break;
      }
      case arrow::Type::LIST:
      {
        auto base_list = std::dynamic_pointer_cast<arrow::ListArray>(tmp_data);
        assert(base_list);
        ARROW_ASSIGN_OR_RAISE(tmp_data, MaybeUpdateShapeAndNdim(base_list));
        break;
      }
      case arrow::Type::FIXED_SIZE_LIST:
      {
        auto base_list = std::dynamic_pointer_cast<arrow::FixedSizeListArray>(tmp_data);
        assert(base_list);
        ARROW_ASSIGN_OR_RAISE(tmp_data, MaybeUpdateShapeAndNdim(base_list));
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
        type_id = tmp_data->type_id();
        done = true;
        break;
      default:
        return arrow::Status::TypeError(
            "Shape derivation of ",
            tmp_data->type()->ToString(),
            " is not supported");
    }
  }

  switch(column.columnDesc().dataType()) {
    case casacore::DataType::TpComplex:
      if(type_id != arrow::Type::FLOAT) {
        return arrow::TypeError()
      }
    case casacore::DataType::TpDComplex:
    default:
      break;
  };

  if(!fixed_shape) {
    return std::tuple(std::nullopt, ndim);
  }

  assert(shape.size() == ndim);
  // C-ORDER to FORTRAN-ORDER
  auto casa_shape = casacore::IPosition(shape.size(), 0);
  for(std::size_t dim=0; dim < shape.size(); ++dim) {
    auto fdim = shape.size() - dim - 1;
    casa_shape[fdim] = shape[dim];

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

  return std::tuple(std::make_optional(casa_shape), ndim);
}


// Factory method for creating Variably Shape Data from input data
// arrow::Result<std::unique_ptr<VariableShapeData>>
// VariableShapeData::MakeFromData(const casacore::TableColumn & column,
//                                 const std::shared_ptr<arrow::Array> & data,
//                                 const ColumnSelection & selection) {
//   auto row_shapes = decltype(VariableShapeData::row_shapes_){};
//   auto fixed_shape = true;
//   auto fixed_dims = true;
//   auto row_dim = selection.size() - 1;
//   using ItType = std::tuple<casacore::rownr_t, bool>;
//   auto array_column = casacore::ArrayColumnBase(column);

//   if(selection.size() == 0 || selection[row_dim].size() == 0) {
//     for(auto [r, first] = ItType{0, true}; r < data->length(); ++r) {
//       ARROW_ASSIGN_OR_RAISE(auto shape, GetDataRowShape(data, selection, r));
//       row_shapes.emplace_back(std::move(shape));
//       ARROW_RETURN_NOT_OK(SetRowShape(array_column, selection, r, shape));
//       if(first) { first = false; continue; }
//       fixed_shape = fixed_shape && *std::rbegin(row_shapes) == *std::begin(row_shapes);
//       fixed_dims = fixed_dims && std::rbegin(row_shapes)->size() == std::begin(row_shapes)->size();
//     }
//   } else {
//     const auto & row_ids = selection[row_dim];
//     row_shapes.reserve(row_ids.size());
//     for(auto [r, first] = ItType{0, true}; r < row_ids.size(); ++r) {
//       ARROW_ASSIGN_OR_RAISE(auto shape, GetDataRowShape(data, selection, r))
//       ARROW_RETURN_NOT_OK(SetRowShape(array_column, selection, row_ids[r], shape));
//       row_shapes.emplace_back(std::move(shape));
//       if(first) { first = false; continue; }
//       fixed_shape = fixed_shape && *std::rbegin(row_shapes) == *std::begin(row_shapes);
//       fixed_dims = fixed_dims && std::rbegin(row_shapes)->size() == std::begin(row_shapes)->size();
//     }
//   }

//   // Arrow can't handle differing dimensions per row, so we quit here.
//   if(!fixed_dims) {
//     return arrow::Status::NotImplemented("Array ", data->ToString(),
//                                          " dimensions vary per row.");
//   }

//   // We may have a fixed shape in practice
//   auto shape = fixed_shape ? std::make_optional(*std::begin(row_shapes))
//                             : std::nullopt;

//   ARROW_ASSIGN_OR_RAISE(auto offsets, MakeOffsets(row_shapes));

//   return std::unique_ptr<VariableShapeData>(
//     new VariableShapeData{std::move(row_shapes),
//                           std::move(offsets),
//                           std::begin(row_shapes)->size(),
//                           std::move(shape)});
// }

// Factory method for creating Variably Shape Data from column
// arrow::Result<std::unique_ptr<VariableShapeData>>
// VariableShapeData::MakeFromColumn(const casacore::TableColumn & column,
//                                   const ColumnSelection & selection) {
//   assert(!column.columnDesc().isFixedShape());
//   auto row_shapes = decltype(VariableShapeData::row_shapes_){};
//   bool fixed_shape = true;
//   bool fixed_dims = true;
//   // Row dimension is last in FORTRAN ordering
//   auto row_dim = selection.size() - 1;
//   using ItType = std::tuple<casacore::rownr_t, bool>;

//   // No selection
//   // Create row shape data from column.nrow()
//   if(selection.size() == 0 || selection[row_dim].size() == 0) {
//     row_shapes.reserve(column.nrow());

//     for(auto [r, first] = ItType{0, true}; r < column.nrow(); ++r) {
//       ARROW_ASSIGN_OR_RAISE(auto shape, GetColumnRowShape(column, r))
//       ARROW_ASSIGN_OR_RAISE(shape, ClipShape(shape, selection));
//       row_shapes.emplace_back(std::move(shape));
//       if(first) { first = false; continue; }
//       fixed_shape = fixed_shape && *std::rbegin(row_shapes) == *std::begin(row_shapes);
//       fixed_dims = fixed_dims && std::rbegin(row_shapes)->size() == std::begin(row_shapes)->size();
//     }
//   } else {
//     // Create row shape data from row id selection
//     const auto & row_ids = selection[row_dim];
//     row_shapes.reserve(row_ids.size());

//     for(auto [r, first] = ItType{0, true}; r < row_ids.size(); ++r) {
//       ARROW_ASSIGN_OR_RAISE(auto shape, GetColumnRowShape(column, row_ids[r]));
//       ARROW_ASSIGN_OR_RAISE(shape, ClipShape(shape, selection));
//       row_shapes.emplace_back(std::move(shape));
//       if(first) { first = false; continue; }
//       fixed_shape = fixed_shape && *std::rbegin(row_shapes) == *std::begin(row_shapes);
//       fixed_dims = fixed_dims && std::rbegin(row_shapes)->size() == std::begin(row_shapes)->size();
//     }
//   }

//   // Arrow can't handle differing dimensions per row, so we quit here.
//   if(!fixed_dims) {
//     return arrow::Status::NotImplemented("Column ", column.columnDesc().name(),
//                                           " dimensions vary per row.");
//   }

//   // We may have a fixed shape in practice
//   auto shape = fixed_shape ? std::make_optional(*std::begin(row_shapes))
//                             : std::nullopt;

//   ARROW_ASSIGN_OR_RAISE(auto offsets, MakeOffsets(row_shapes));

//   return std::unique_ptr<VariableShapeData>(
//     new VariableShapeData{std::move(row_shapes),
//                           std::move(offsets),
//                           std::begin(row_shapes)->size(),
//                           std::move(shape)});
// }

// bool VariableShapeData::IsActuallyFixed() const {
//   return shape_.has_value();
// }

arrow::Result<ArrowShapeProvider>
ArrowShapeProvider::Make(const casacore::TableColumn & column,
                    const ColumnSelection & selection,
                    const std::shared_ptr<arrow::Array> & data) {


  ARROW_ASSIGN_OR_RAISE(auto shape_and_ndim, GetShapeAndNdim(column, selection, data));
  auto &[shape, ndim] = shape_and_ndim;
  return ArrowShapeProvider{std::cref(column), selection, data, std::move(shape), ndim};
}

// Returns the dimension size of the data
arrow::Result<std::size_t>
ArrowShapeProvider::DimSize(std::size_t dim) const {
  assert(IsDataFixed());
  assert(shape_.has_value());
  assert(dim < nDim());
  return shape_.value()[dim];
}

// Returns the dimension size of the data for the given row
std::size_t ArrowShapeProvider::RowDimSize(casacore::rownr_t row, std::size_t dim) const {
  assert(IsDataVarying());
  assert(dim < RowDim());
  // auto sdim = SelectDim(dim, nDim(), nDim());

  auto tmp_data = data_;
  auto start = std::int64_t(row);
  auto end = start + 1;

  auto GetDimSize = [&](auto list) -> std::size_t{
    // No dimension size can be derived if there is 1 or 0 offsets
    if(end - start <= 1) {
      return 0;
    }

    // Derive dimension size from the first two offsets
    auto dim_size = list->value_offset(start + 1) - list->value_offset(start);

    // Assert that other offset diffs match
    for(auto i = start + 2; i < end + 1; ++i) {
      assert(dim_size == list->value_offset(i) - list->value_offset(i - 1));
    }

    // Advance start and end
    start = list->value_offset(start);
    end = list->value_offset(end);

    return dim_size;
  };

  using ItType = std::tuple<bool, std::size_t>;

  for(auto [done, current_dim]=ItType{false, 0}; !done; ++current_dim) {
    switch(tmp_data->type_id()) {
      case arrow::Type::LIST:
        {
          auto list = std::dynamic_pointer_cast<arrow::ListArray>(tmp_data);
          if(current_dim == dim) return GetDimSize(list);
          tmp_data = list->values();
          break;
        }
      case arrow::Type::LARGE_LIST:
        {
          auto list = std::dynamic_pointer_cast<arrow::LargeListArray>(tmp_data);
          if(current_dim == dim) return GetDimSize(list);
          tmp_data = list->values();
          break;
        }
      case arrow::Type::FIXED_SIZE_LIST:
        {
          auto list = std::dynamic_pointer_cast<arrow::FixedSizeListArray>(tmp_data);
          if(current_dim == dim) return GetDimSize(list);
          tmp_data = list->values();
          break;
        }
      default:
        done = true;
        break;
    }
  }

  assert(false);
}


std::size_t ColumnWriteMap::FlatOffset(const std::vector<std::size_t> & index) const {
  if(shape_provider_.shape_) {
    // Fixed shape output, easy case
    const auto & shape = shape_provider_.shape_.value();
    auto result = std::size_t{0};
    auto product = std::size_t{1};

    for(auto dim = 0; dim < RowDim(); ++dim) {
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
    for(std::size_t d=0; d < RowDim(); ++d) {
      p *= RowDimSize(r, d);
    }
    result += p;
  }

}


// Factory method for making a ColumnWriteMap object
arrow::Result<ColumnWriteMap>
ColumnWriteMap::Make(
    const casacore::TableColumn & column,
    ColumnSelection selection,
    const std::shared_ptr<arrow::Array> & data,
    InputOrder order) {

  // Convert to FORTRAN ordering, which the casacore internals use
  if(order == InputOrder::C_ORDER) {
    std::reverse(std::begin(selection), std::end(selection));
  }

  ARROW_ASSIGN_OR_RAISE(auto shape_prov, ArrowShapeProvider::Make(column, selection, data));
  auto maps = MapFactory(shape_prov, selection);
  ARROW_ASSIGN_OR_RAISE(auto ranges, RangeFactory(shape_prov, maps));

  if(ranges.size() == 0) {
    return arrow::Status::ExecutionError("Zero ranges generated for column ",
                                          column.columnDesc().name());
  }

  return ColumnWriteMap{column, std::move(maps), std::move(ranges),
                        std::move(shape_prov)};
}

// Number of disjoint ranges in this map
std::size_t
ColumnWriteMap::nRanges() const {
  return std::accumulate(std::begin(ranges_), std::end(ranges_), std::size_t{1},
                        [](const auto init, const auto & range)
                          { return init * range.size(); });
}

// Returns true if this is a simple map or, a map that only contains
// a single range and thereby removes the need to read separate ranges of
// data and copy those into a final buffer.
bool
ColumnWriteMap::IsSimple() const {
  for(std::size_t dim=0; dim < nDim(); ++dim) {
    const auto & column_map = DimMaps(dim);
    const auto & column_range = DimRanges(dim);

    // More than one range of row ids in a dimension
    if(column_range.size() > 1) {
      return false;
    }

    for(auto & range: column_range) {
      switch(range.type) {
        // These are trivially contiguous
        case Range::FREE:
        case Range::VARYING:
          break;
        case Range::MAP:
          for(std::size_t i = range.start + 1; i < range.end; ++i) {
            if(column_map[i].mem - column_map[i-1].mem != 1) {
              return false;
            }
            if(column_map[i].disk - column_map[i-1].disk != 1) {
              return false;
            }
          }
          break;
      }
    }
  }

  return true;
}

// Find the total number of elements formed
// by the disjoint ranges in this map
std::size_t
ColumnWriteMap::nElements() const {
  assert(ranges_.size() > 0);
  const auto & row_ranges = DimRanges(RowDim());
  auto elements = std::size_t{0};

  for(std::size_t rr_id=0; rr_id < row_ranges.size(); ++rr_id) {
    const auto & row_range = row_ranges[rr_id];
    auto row_elements = std::size_t{row_range.nRows()};
    for(std::size_t dim = 0; dim < RowDim(); ++dim) {
      const auto & dim_range = DimRanges(dim);
      auto dim_elements = std::size_t{0};
      for(const auto & range: dim_range) {
        switch(range.type) {
          case Range::VARYING:
            assert(row_range.IsSingleRow());
            dim_elements += shape_provider_.RowDimSize(rr_id, dim);
            break;
          case Range::FREE:
          case Range::MAP:
            assert(range.IsValid());
            dim_elements += range.nRows();
            break;
          default:
            assert(false && "Unhandled Range::Type enum");
        }
      }
      row_elements *= dim_elements;
    }
    elements += row_elements;
  }

  return elements;
}


} // namespace arcae
