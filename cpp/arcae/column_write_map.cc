#include "arcae/column_write_map.h"

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
#include <arrow/util/logging.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/array/array_nested.h>
#include <arrow/scalar.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/type.h>

#include <casacore/casa/aipsxtype.h>
#include <casacore/casa/Utilities/DataType.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/Arrays/Slicer.h>
#include <casacore/tables/Tables/ArrayColumnBase.h>
#include <casacore/tables/Tables/TableColumn.h>

#include "arcae/map_iterator.h"

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

struct DataProperties {
  std::optional<casacore::IPosition> shape;
  std::size_t ndim;
  std::shared_ptr<arrow::DataType> data_type;
  bool is_complex;
};

// Get the properties of the input data
arrow::Result<DataProperties> GetDataProperties(
  const casacore::TableColumn & column,
  const ColumnSelection & selection,
  const std::shared_ptr<arrow::Array> & data)
{
  auto fixed_shape = true;
  auto shape = std::vector<std::int64_t>{data->length()};
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

  std::shared_ptr<arrow::DataType> data_type;

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
        ++ndim;
        data_type = tmp_data->type();
        done = true;
        break;
      default:
        return arrow::Status::TypeError(
            "Shape derivation of ",
            tmp_data->type()->ToString(),
            " is not supported");
    }
  }

  // If we're writing to Complex Data columns, the
  // Data array must contain a nested list of paired values
  // at it's root.
  // Modify ndim and shape to reflect the CASA shape
  const auto & casa_type = column.columnDesc().dataType();
  auto is_complex = casa_type == casacore::TpComplex ||
                           casa_type == casacore::TpDComplex;

  if(is_complex) {
    if(ndim == 0) {
      return arrow::Status::Invalid(
        "A list array of paired numbers must be supplied when writing "
        "to complex typed column ", column.columnDesc().name());
    }

    --ndim;

    if(fixed_shape) {
      if(shape.back() != 2) {
        return arrow::Status::Invalid(
          "A list array of paired numbers must be supplied when writing "
          "to complex typed column ", column.columnDesc().name());
      }

      shape.pop_back();
    }
  }

  // Variably shaped data
  if(!fixed_shape) {
    return DataProperties{std::nullopt, ndim, std::move(data_type), is_complex};
  }

  // C-ORDER to FORTRAN-ORDER
  assert(ndim == shape.size());
  auto casa_shape = casacore::IPosition(ndim, 0);
  for(std::size_t dim=0; dim < ndim; ++dim) {
    auto fdim = ndim - dim - 1;
    casa_shape[fdim] = shape[dim];

    // Check that the selection indices don't exceed the data shape
    if(auto sdim = SelectDim(fdim, selection.size(), ndim); sdim >= 0) {
      if(selection[sdim].size() > shape[dim]) {
        return arrow::Status::IndexError(
          "Number of selections ", selection[sdim].size(),
          " is greater than the dimension ", shape[dim],
          " of the input data");
      }
    }
  }

  // Fixed shape data
  return DataProperties{std::make_optional(casa_shape), ndim, std::move(data_type), is_complex};
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
        for(auto d=0; d < shape.size(); ++d) shape[d] = shape_prov.RowDimSize(r, d);
        column.setShape(r, shape);
      }
    }
  } else {
    const auto & row_ids = selection[select_row_dim];

    for(auto rid = 0; rid < row_ids.size(); ++rid) {
      auto r = row_ids[rid];
      if(!column.isDefined(r)) {
        auto shape = casacore::IPosition(shape_prov.nDim() - 1, 0);
        for(auto d=0; d < shape.size(); ++d) shape[d] = shape_prov.RowDimSize(rid, d);
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

  ARROW_ASSIGN_OR_RAISE(auto properties, GetDataProperties(column, selection, data));
  auto &[shape, ndim, dtype, is_complex] = properties;
  return ArrowShapeProvider{std::cref(column),
                            selection, data,
                            std::move(dtype),
                            std::move(shape),
                            ndim, is_complex};
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

  using ItType = std::tuple<bool, std::size_t>;

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
  ARROW_LOG(INFO) << "Logical error in ArrowShapeProvider::RowDimSize";
  assert(false);
  std::exit(1);
  return -1;
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
    InputOrder order) {

  // Convert to FORTRAN ordering, which the casacore internals use
  if(order == InputOrder::C_ORDER) {
    std::reverse(std::begin(selection), std::end(selection));
  }

  ARROW_ASSIGN_OR_RAISE(auto shape_prov, ArrowShapeProvider::Make(column, selection, data));
  auto maps = MapFactory(shape_prov, selection);
  if(shape_prov.IsColumnVarying()) {
    auto array_base = casacore::ArrayColumnBase(column);
    ARROW_RETURN_NOT_OK(SetVariableRowShapes(array_base, selection, data, shape_prov));
  }
  ARROW_ASSIGN_OR_RAISE(auto ranges, RangeFactory(shape_prov, maps));

  if(ranges.size() == 0) {
    return arrow::Status::ExecutionError("Zero ranges generated for column ",
                                          column.columnDesc().name());
  }

  return ColumnWriteMap{column, data,
                        std::move(maps), std::move(ranges),
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
