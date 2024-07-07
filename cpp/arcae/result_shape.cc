
#include <algorithm>
#include <cstddef>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <sys/types.h>

#include <casacore/casa/aipsxtype.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/tables/Tables/ColumnDesc.h>
#include <casacore/tables/Tables/TableColumn.h>
#include <casacore/casa/Utilities/DataType.h>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>

#include "arcae/selection.h"
#include "arcae/result_shape.h"

using ::arcae::detail::IndexType;
using RowShapes = ::arcae::detail::RowShapes;

using ::casacore::ColumnDesc;
using ::casacore::DataType;
using ::casacore::IPosition;
using ::casacore::TableColumn;

namespace arcae {
namespace detail {
namespace {

//----------------------------------------------------------------------------
// Anonymous Read Functions
//----------------------------------------------------------------------------

// Get the shape for a particular row (row is excluded from the shape)
// If, the supplied row is negative (-1) and a result_shape is supplied
// the shape is derived from this source, otherwise the CASA shape is
// used as the reference value
arrow::Result<IPosition> GetRowShape(
    const TableColumn & column,
    const std::optional<IPosition> & result_shape,
    IndexType r) {

  if(r < 0) {
    if(!result_shape.has_value()) {
      return arrow::Status::IndexError(
        "Negative selection indices may only be present "
        "when a fixed shape result array is provided");
    }
    const auto & shape = result_shape.value();
    // Return secondary dimensions
    return shape.getFirst(shape.size() - 1);
  } else if(r >= IndexType(column.nrow())) {
    return arrow::Status::IndexError(
      "Row ", r, " in column ",
      column.columnDesc().name(),
      " is out of bounds");
  } else if(column.isDefined(r)) {
    return column.shape(r);
  }
  return arrow::Status::IndexError(
    "Row ", r, " in column ",
    column.columnDesc().name(),
    " is not defined");
}

// Clips the shape against the selection
arrow::Status ClipShape(
    const casacore::ColumnDesc & column_desc,
    IPosition & shape,
    const Selection & selection) {

  if(selection.Size() <= 1) return arrow::Status::OK();
  for(std::size_t dim=0; dim < shape.size(); ++dim) {
    if(auto result = selection.FSpan(dim, shape.size() + 1); result.ok()) {
      auto span = result.ValueOrDie();
      for(auto i: span) {
        if(i >= shape[dim]) {
          return arrow::Status::IndexError(
            "Selection index ", i,
            " exceeds dimension ", dim,
            " of shape ", shape,
            " in column ", column_desc.name());
        }
      }
      shape[dim] = span.size();
    }
  }
  return arrow::Status::OK();
}

// Create variably shaped row data
arrow::Result<RowShapes>
MakeRowData(const TableColumn &column,
            const Selection &selection,
            const std::optional<IPosition> & result_shape) {

  RowShapes shapes;
  const auto & column_desc = column.columnDesc();

  // Get the row selection if provided
  if(selection.HasRowSpan()) {
    auto span = selection.GetRowSpan();
    shapes.reserve(span.size());
    for(std::size_t r=0; r < span.size(); ++r) {
      ARROW_ASSIGN_OR_RAISE(auto shape, GetRowShape(column, result_shape, span[r]));
      ARROW_RETURN_NOT_OK(ClipShape(column_desc, shape, selection));
      shapes.emplace_back(std::move(shape));
    }
  // otherwise, the entire column
  } else {
    shapes.reserve(column.nrow());
    for(std::size_t r=0; r < column.nrow(); ++r) {
      ARROW_ASSIGN_OR_RAISE(auto shape, GetRowShape(column, result_shape, r));
      ARROW_RETURN_NOT_OK(ClipShape(column_desc, shape, selection));
      shapes.emplace_back(std::move(shape));
    }
  }

  return shapes;
}

// Check that the column shape matches the
// shape of the result array
arrow::Status CheckShapeMatchesResult(
    const std::string & column_name,
    const IPosition & shape,
    const std::optional<IPosition> & result_shape) {
  if(result_shape.has_value()) {
    if(shape != result_shape.value()) {
      return arrow::Status::Invalid(
        "Result shape ", result_shape.value(),
        " does not match the selection shape ", shape,
        " in column ", column_name);
    }
  }
  return arrow::Status::OK();
}

//----------------------------------------------------------------------------
// Anonymous Write Functions
//----------------------------------------------------------------------------

// Get the row shapes of variably shaped list arrays
arrow::Result<RowShapes>
GetRowShapes(const std::shared_ptr<arrow::Array> & data, std::size_t ndim) {
  auto result = RowShapes(data->length(), IPosition(ndim - 1));

  auto SetRowShape = [&](
      auto list,
      std::int64_t r,
      std::int64_t & start,
      std::int64_t & end,
      std::size_t & nd
  ) -> arrow::Result<std::shared_ptr<arrow::Array>> {
    if (list->null_count() > 0) return arrow::Status::NotImplemented("nulls");
    auto dim_size = list->value_length(start);
    if constexpr (!std::is_same_v<decltype(list), std::shared_ptr<arrow::FixedSizeListArray>>) {
      for (std::int64_t i=start + 1; i < end; ++i) {
        if(dim_size != list->value_length(i)) {
          return arrow::Status::Invalid("Offsets in row ", r, " don't match");
        }
      }
    }
    result[r][nd++ - 1] = dim_size;
    start = list->value_offset(start);
    end = list->value_offset(end);
    return list->values();
  };

  for(std::int64_t r=0; r < data->length(); ++r) {
    std::size_t ndim = 1;
    auto tmp = data;
    auto start = r;
    auto end = r + 1;
    for(bool done=false; !done; ) {
      switch(tmp->type_id()) {
        case arrow::Type::LIST: {
          auto list = std::dynamic_pointer_cast<arrow::ListArray>(tmp);
          ARROW_ASSIGN_OR_RAISE(tmp, SetRowShape(list, r, start, end, ndim))
          continue;
        }
        case arrow::Type::LARGE_LIST: {
          auto list = std::dynamic_pointer_cast<arrow::LargeListArray>(tmp);
          ARROW_ASSIGN_OR_RAISE(tmp, SetRowShape(list, r, start, end, ndim))
          continue;
        }
        case arrow::Type::FIXED_SIZE_LIST: {
          auto list = std::dynamic_pointer_cast<arrow::FixedSizeListArray>(tmp);
          ARROW_ASSIGN_OR_RAISE(tmp, SetRowShape(list, r, start, end, ndim))
          continue;
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
          return arrow::Status::NotImplemented(
            "Shape derivation of ",
            tmp->type()->ToString(),
            " is not supported");
      }
    }
  }

  return result;
}

// Get shape information for the supplied array
arrow::Result<ResultShapeData>
GetArrowResultShapeData(
    const ColumnDesc & column_desc,
    const std::shared_ptr<arrow::Array> & data) {

  if (!data) return arrow::Status::Invalid("data is null");
  auto fixed_shape = true;
  auto shape = std::vector<std::int64_t>{data->length()};
  auto ndim = std::size_t{1};
  auto tmp = data;
  std::shared_ptr<arrow::DataType> dtype;

  auto MaybeUpdateShapeAndNdim = [&](auto list) -> arrow::Result<std::shared_ptr<arrow::Array>> {
    if(list->null_count() > 0) return arrow::Status::NotImplemented("null handling");
    ++ndim;
    if(!fixed_shape) return list->values();
    auto dim_size = list->value_length(0);
    if constexpr (!std::is_same_v<decltype(list), std::shared_ptr<arrow::FixedSizeListArray>>) {
      // Check for variable dimension sizes in the non-fixed size list case
      for (std::int64_t i = 0; i < list->length(); ++i) {
        if(dim_size != list->value_length(i)) {
          fixed_shape = false;
          return list->values();
        }
      }
    }
    shape.emplace_back(std::size_t(dim_size));
    return list->values();
  };

  for (auto done=false; !done;) {
    switch (tmp->type_id()) {
      case arrow::Type::LARGE_LIST: {
        auto lla = std::dynamic_pointer_cast<arrow::LargeListArray>(tmp);
        ARROW_ASSIGN_OR_RAISE(tmp, MaybeUpdateShapeAndNdim(lla));
        break;
      }
      case arrow::Type::LIST: {
        auto la = std::dynamic_pointer_cast<arrow::ListArray>(tmp);
        ARROW_ASSIGN_OR_RAISE(tmp, MaybeUpdateShapeAndNdim(la));
        break;
      }
      case arrow::Type::FIXED_SIZE_LIST: {
        auto fsla = std::dynamic_pointer_cast<arrow::FixedSizeListArray>(tmp);
        ARROW_ASSIGN_OR_RAISE(tmp, MaybeUpdateShapeAndNdim(fsla));
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
        dtype = tmp->type();
        done = true;
        break;
      default:
        return arrow::Status::NotImplemented(
          "Shape derivation of ",
          tmp->type()->ToString(),
          " is not supported");
    }
  }

  if (fixed_shape) {
    // C-ORDER to FORTRAN-ORDER
    auto casa_shape = IPosition(ndim, 0);
    for(std::size_t dim=0; dim < ndim; ++dim) {
      casa_shape[ndim - dim - 1] = shape[dim];
    }

    return ResultShapeData{
      column_desc.name(),
      std::make_optional(casa_shape),
      ndim,
      column_desc.dataType(),
      std::nullopt
    };
  }

  ARROW_ASSIGN_OR_RAISE(auto row_shapes, GetRowShapes(data, ndim));
  return ResultShapeData{
    column_desc.name(),
    std::nullopt,
    ndim,
    column_desc.dataType(),
    std::move(row_shapes)
  };
}

// Gets basic shape information from GetArrowResultShapeData.
// However, Arrow doesn't have a complex data type,
// We have chosen to represent complex values
// as lists of real-imaginary pairs.
// This function massages Arrow shape data
// into CASA shape data, by removing the
// associated dimension of 2 values from the shape data.
arrow::Result<ResultShapeData>
GetResultShapeData(
    const ColumnDesc & column_desc,
    const std::shared_ptr<arrow::Array>& data) {

  ARROW_ASSIGN_OR_RAISE(auto shape_data, GetArrowResultShapeData(column_desc, data));
  // No conversion needed, just move
  if(shape_data.GetDataType() != DataType::TpComplex &&
      shape_data.GetDataType() != DataType::TpDComplex) {
    return std::move(shape_data);
  }

  // Convert the fixed shape
  if(shape_data.IsFixed()) {
    auto shape = shape_data.GetShape();
    if(shape.size() == 0 || shape[0] != 2) {
      return arrow::Status::Invalid(
        "Arrow result data must supply pairs of values ",
        "for complex valued column ", shape_data.GetName());
    }

    shape = shape.getLast(shape.size() - 1);
    auto ndim = shape.size();
    return ResultShapeData{
      std::move(shape_data.column_name_),
      std::move(shape),
      ndim,
      shape_data.dtype_,
      std::nullopt
    };
  }

  // Modify the row shapes
  auto & shapes = shape_data.row_shapes_.value();
  for(std::size_t r=0; r < shapes.size(); ++r) {
    if(shapes[r].size() == 0 || shapes[r][0] != 2) {
      return arrow::Status::Invalid(
        "Arrow result data must supply pairs of values ",
        "for complex valued column ", shape_data.column_name_);
    }
    shapes[r] = shapes[r].getLast(shapes[r].size() - 1);
  }

  return ResultShapeData{
    std::move(shape_data.column_name_),
    std::nullopt,
    shape_data.ndim_ - 1,
    shape_data.dtype_,
    std::move(shape_data.row_shapes_)
  };
}

}  // namespace

// Maximum dimension size
std::size_t
ResultShapeData::MaxDimensionSize() const noexcept {
  if(IsFixed()) return *std::max_element(shape_->begin(), shape_->end());
  std::size_t max_size = nRows();
  for(std::size_t r=0; r < nRows(); ++r) {
    const auto & shape = GetRowShape(r);
    auto shape_max = *std::max_element(shape.begin(), shape.end());
    max_size = std::max<std::size_t>(max_size, shape_max);
  }
  return max_size;
}

std::size_t
ResultShapeData::FlatOffset(const std::vector<IndexType> & index) const noexcept {
  auto ndim = nDim();
  std::size_t offset = 0;
  IndexType product = 1;
  assert(index.size() == ndim);

  // Fixed case
  if(IsFixed()) {
    const auto & shape = GetShape();
    for(std::size_t d=0; d < index.size() && d < ndim; ++d) {
      offset += index[d] * product;
      product *= shape[d];
    }
    return offset;
  };

  // Variable case
  auto row = index[ndim-1];
  assert(row < IndexType(nRows()));
  const auto shape = GetRowShape(row);

  for(std::size_t d=0; d < shape.size() && d < ndim - 1; ++d) {
    offset += index[d] * product;
    product *= shape[d];
  }

  return std::accumulate(
    row_shapes_->begin(),
    row_shapes_->begin() + row + 1,
    offset,
    [](auto i, auto s) { return i + s.product(); });
}

// Number of elements in the result
std::size_t
ResultShapeData::nElements() const noexcept {
  if(IsFixed()) return shape_.value().product();
  return std::accumulate(
    row_shapes_->begin(),
    row_shapes_->end(),
    std::size_t{0},
    [](auto i, auto s) { return s.product() + i; });
}

// Get ListArray offsets arrays
arrow::Result<std::vector<std::shared_ptr<arrow::Int32Array>>>
ResultShapeData::GetOffsets() const noexcept {
  auto nrow = nRows();
  // Don't build offsets for the row dimension (last in FORTRAN order)
  auto ndim = nDim() - 1;
  auto builders = std::vector<arrow::Int32Builder>(ndim);
  auto offsets = std::vector<std::shared_ptr<arrow::Int32Array>>(ndim);
  auto running_offsets = std::vector<std::size_t>(ndim, 0);

  // Initialise offsets
  for(std::size_t dim=0; dim < ndim; ++dim) {
    ARROW_RETURN_NOT_OK(builders[dim].Reserve(nrow + 1));
    ARROW_RETURN_NOT_OK(builders[dim].Append(0));
  }

  // Compute number of elements in each row by creating
  // a product over each dimension
  auto BuildFn = [&](auto && GetShapeFn) -> arrow::Status {
    using ItType = std::tuple<std::ptrdiff_t, std::size_t>;
    for(std::size_t row=0; row < nrow; ++row) {
      for(auto [dim, product]=ItType{ndim - 1, 1}; dim >= 0; --dim) {
        auto dim_size = GetShapeFn(row, dim);
        for(std::size_t p=0; p < product; ++p) {
          running_offsets[dim] += dim_size;
          ARROW_RETURN_NOT_OK(builders[dim].Append(running_offsets[dim]));
        }
        product *= dim_size;
      }
    }
    return arrow::Status::OK();
  };

  // Build the offset arrays
  if(!IsFixed()) {
    ARROW_RETURN_NOT_OK(BuildFn([&](auto r, auto d) { return GetRowShape(r)[d]; }));
  } else {
    ARROW_RETURN_NOT_OK(BuildFn([&](auto r, auto d) { return (*shape_)[d]; }));
  }
  // Finish the offset arrays
  for(std::size_t dim=0; dim < ndim; ++dim) {
    ARROW_RETURN_NOT_OK(builders[dim].Finish(&offsets[dim]));
  }
  return offsets;
}


arrow::Result<ResultShapeData>
ResultShapeData::MakeRead(
    const TableColumn &column,
    const Selection &selection,
    const std::shared_ptr<arrow::Array> & result) {
  auto column_desc = column.columnDesc();
  auto column_name = column_desc.name();
  auto dtype = column_desc.dataType();
  auto nselrow = selection.HasRowSpan() ? ssize_t(selection.GetRowSpan().size())
                                        : ssize_t(column.nrow());

  // Get fixed shape of the result array, if available
  std::optional<IPosition> result_shape = std::nullopt;
  if (result) {
    ARROW_ASSIGN_OR_RAISE(auto result_data, GetResultShapeData(column_desc, result));
    if(result_data.IsFixed()) result_shape = result_data.GetShape();
  }

  // The number of dimensions varies per row
  // This case is not handled
  if (column_desc.ndim() == -1) {
    return arrow::Status::NotImplemented(
      "Column ", column_name,
      " has varying dimensions");
  }

  // Fixed shape, easy case
  if (column_desc.isFixedShape()) {
    auto shape = column_desc.shape();
    ARROW_RETURN_NOT_OK(ClipShape(column_desc, shape, selection));
    ARROW_RETURN_NOT_OK(CheckShapeMatchesResult(column_name, shape, result_shape));
    shape.append(IPosition({nselrow}));
    std::size_t ndim = shape.size();
    return ResultShapeData{
      std::move(column_name),
      std::move(shape),
      ndim,
      std::move(dtype),
      std::nullopt};
  }

  // Get shapes of each row in the selection
  ARROW_ASSIGN_OR_RAISE(auto shapes, MakeRowData(column, selection, result_shape));
  auto shape = std::optional<IPosition>{};
  int ndim = -1;
  bool first = true;
  bool shapes_equal = true;

  // Identify fixed shapes and varying dimensionality
  for (auto it = shapes.begin(); it != shapes.end(); ++it) {
    if(first) {
      shape = *it;
      ndim = it->size();
      first = false;
    } else {
      if (shapes.front().size() != it->size()) {
        ndim = -1;
        shape.reset();
        shapes_equal = false;
        break;
      }
      shapes_equal = shapes_equal && (shapes.front() == *it);
    }
  }

  // Should have been caught above, but check again
  // The number of dimensions varies per row
  // This case is not handled
  if (ndim == -1) {
    return arrow::Status::NotImplemented(
      "Column ", column_name,
      " has varying dimensions");
  }

  // Even though the column varys
  // the resultant shape after selection is fixed
  // There's no need to clip the shape as this
  // will have been done in MakeRowData
  if (shapes_equal) {
    assert(shape.has_value());
    shape->append(IPosition({nselrow}));
    ARROW_RETURN_NOT_OK(CheckShapeMatchesResult(column_name, *shape, result_shape));
    ndim = shape->size();
    return ResultShapeData{
      column_name,
      std::move(shape),
      std::size_t(ndim),
      std::move(dtype),
      std::nullopt};
  }

  // Shapes vary per row
  return ResultShapeData{
    column_name,
    std::nullopt,
    std::size_t(ndim + 1),
    std::move(dtype),
    std::move(shapes)};
}

arrow::Result<ResultShapeData>
ResultShapeData::MakeWrite(
    const TableColumn &column,
    const std::shared_ptr<arrow::Array> & data,
    const Selection &selection) {

  if (!data) return arrow::Status::Invalid("data array is null");
  auto column_desc = column.columnDesc();

  // Varying dimensions in each row are not supported
  if (column_desc.ndim() < 0) {
    return arrow::Status::NotImplemented(
      "Writing to unconstrained dimension column ",
      column_desc.name());
  }

  ARROW_ASSIGN_OR_RAISE(auto shape_data, GetResultShapeData(column_desc, data));
  if (shape_data.nDim() != std::size_t(column_desc.ndim() + 1)) {
    return arrow::Status::Invalid(
      "Number of data dimensions ", shape_data.nDim(),
      " does not match number of column dimensions ",
      column_desc.ndim() + 1);
  }

  // Check the row dimension against the selection
  if(selection.HasRowSpan() && shape_data.nRows() != selection.GetRowSpan().size()) {
    return arrow::Status::IndexError(
      "Row selection size ", selection.GetRowSpan().size(),
      " doesn't match the number of rows",
      " in the result shape ", shape_data.nRows()
    );
  }

  // Check secondary dimensions against the selection
  auto other_dims = std::ptrdiff_t(shape_data.nDim() - 1);
  auto sel_size = std::ptrdiff_t(selection.Size());

  if(shape_data.IsFixed()) {
    for(std::ptrdiff_t dim=0; dim < other_dims && dim < sel_size; ++dim) {
      if(shape_data.GetShape()[dim] != ssize_t(selection[dim].size())) {
        return arrow::Status::IndexError(
          "Selection size ", selection[dim].size(),
          " doesn't match dimension ", dim,
          " of shape", shape_data.GetShape());
      }
    }
  } else {
    for(std::size_t r=0; r < shape_data.nRows(); ++r) {
      const auto & row_shape = shape_data.GetRowShape(r);
      for(std::ptrdiff_t dim=0; dim < other_dims && dim < sel_size; ++dim) {
        if(row_shape[dim] != ssize_t(selection[dim].size())) {
          return arrow::Status::IndexError(
            "Selection size", selection[dim].size(),
            " doesn't match dimension ", dim,
            " of shape", shape_data.GetRowShape(r),
            " in row ", r);
        }
      }
    }
  }

  return shape_data;
}

} // namespace detail
} // namespace arcae