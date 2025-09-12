
#include <sys/types.h>
#include <algorithm>
#include <cstddef>
#include <iterator>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <vector>

#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/Utilities/DataType.h>
#include <casacore/casa/aipsxtype.h>
#include <casacore/tables/Tables/ArrayColumnBase.h>
#include <casacore/tables/Tables/ColumnDesc.h>
#include <casacore/tables/Tables/TableColumn.h>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>

#include "arcae/result_shape.h"
#include "arcae/selection.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/buffer_builder.h"

using ::arrow::Result;
using ::arrow::Status;

using ::arcae::detail::IndexType;
using RowShapes = ::arcae::detail::RowShapes;

using ::casacore::ArrayColumnBase;
using ::casacore::ColumnDesc;
using ::casacore::DataType;
using ::casacore::IPosition;
using ::casacore::rownr_t;
using ::casacore::TableColumn;

namespace arcae {
namespace detail {
namespace {

//----------------------------------------------------------------------------
// Anonymous Read Functions
//----------------------------------------------------------------------------

bool IsDegenerateShape(const IPosition& shape) { return shape.size() == 0; }

// Clips the shape against the selection
Status ClipShape(const ColumnDesc& column_desc, IPosition& shape,
                 const Selection& selection) {
  if (selection.Size() <= 1) return Status::OK();
  for (std::size_t dim = 0; dim < shape.size(); ++dim) {
    if (auto result = selection.FSpan(dim, shape.size() + 1); result.ok()) {
      auto span = result.ValueOrDie();
      for (auto i : span) {
        if (i >= shape[dim]) {
          return Status::IndexError("Selection index ", i, " exceeds dimension ", dim,
                                    " of shape ", shape, " in column ",
                                    column_desc.name());
        }
      }
      shape[dim] = span.size();
    }
  }
  return Status::OK();
}

// Create variably shaped row data
Result<RowShapes> MakeRowData(const TableColumn& column, const Selection& selection,
                              bool allow_missing_rows) {
  RowShapes shapes;
  const auto& column_desc = column.columnDesc();

  // Lambda which gets the shape from the column's row if 0 <= row < nrow
  // If row < 0 or the row is missing and a result shape is present, that
  // shape will be used
  auto GetClippedRowShape = [&](auto r) -> Result<IPosition> {
    if (r >= decltype(r)(column.nrow())) {
      return Status::IndexError("Requested row ", r, " in column ", column_desc.name(),
                                " >= column.nrow() ", column.nrow());
    }

    // Get the shape if the row is positive and defined
    // and clip against the selection
    if (r >= 0) {
      if (column.isDefined(r)) {
        auto shape = column.shape(r);
        ARROW_RETURN_NOT_OK(ClipShape(column_desc, shape, selection));
        return shape;
      }
      if (allow_missing_rows) return IPosition();
      return Status::Invalid("Row ", r, " is not defined for column ", column_desc.name(),
                             " and missing rows ", "are not configured");
    }
    if (allow_missing_rows) return IPosition();
    return Status::IndexError(
        "Unable to derive a row shape for negative "
        "row index ",
        r,
        ". MakeRowData has likely been "
        "misused");
  };

  // Get the row selection if provided
  if (selection.HasRowSpan()) {
    auto span = selection.GetRowSpan();
    shapes.reserve(span.size());
    for (std::size_t r = 0; r < span.size(); ++r) {
      ARROW_ASSIGN_OR_RAISE(auto shape, GetClippedRowShape(span[r]));
      shapes.emplace_back(std::move(shape));
    }
    // otherwise, the entire column
  } else {
    shapes.reserve(column.nrow());
    for (casacore::rownr_t r = 0; r < column.nrow(); ++r) {
      ARROW_ASSIGN_OR_RAISE(auto shape, GetClippedRowShape(r));
      shapes.emplace_back(std::move(shape));
    }
  }

  return shapes;
}

//----------------------------------------------------------------------------
// Anonymous Write Functions
//----------------------------------------------------------------------------

// Get the row shapes of variably shaped list arrays
Result<RowShapes> GetRowShapes(const std::shared_ptr<arrow::Array>& data, int ndim) {
  auto result = RowShapes(data->length(), IPosition(ndim - 1));

  auto SetRowShape = [&](auto list, std::int64_t r, std::int64_t& start,
                         std::int64_t& end,
                         int& nd) -> Result<std::shared_ptr<arrow::Array>> {
    if (list->null_count() > 0) return Status::NotImplemented("nulls");
    auto dim_size = list->value_length(start);
    if constexpr (!std::is_same_v<decltype(list),
                                  std::shared_ptr<arrow::FixedSizeListArray>>) {
      for (std::int64_t i = start + 1; i < end; ++i) {
        if (dim_size != list->value_length(i)) {
          return Status::Invalid("Offsets in row ", r, " don't match");
        }
      }
    }
    result[r][nd++ - 1] = dim_size;
    start = list->value_offset(start);
    end = list->value_offset(end);
    return list->values();
  };

  for (std::int64_t r = 0; r < data->length(); ++r) {
    int ndim = 1;
    auto tmp = data;
    auto start = r;
    auto end = r + 1;
    for (bool done = false; !done;) {
      switch (tmp->type_id()) {
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
          return Status::NotImplemented("Shape derivation of ", tmp->type()->ToString(),
                                        " is not supported");
      }
    }
  }

  return result;
}

// Get shape information for the supplied array
Result<ResultShapeData> GetArrowResultShapeData(
    const ColumnDesc& column_desc, const std::shared_ptr<arrow::Array>& data) {
  if (!data) return Status::Invalid("data is null");
  auto fixed_shape = true;
  auto shape = std::vector<std::int64_t>{data->length()};
  int ndim = 1;
  auto tmp = data;
  std::shared_ptr<arrow::DataType> dtype;

  auto MaybeUpdateShapeAndNdim = [&](auto list) -> Result<std::shared_ptr<arrow::Array>> {
    if (list->null_count() > 0) return Status::NotImplemented("null handling");
    ++ndim;
    if (!fixed_shape) return list->values();
    auto dim_size = list->value_length(0);
    if constexpr (!std::is_same_v<decltype(list),
                                  std::shared_ptr<arrow::FixedSizeListArray>>) {
      // Check for variable dimension sizes in the non-fixed size list case
      for (std::int64_t i = 0; i < list->length(); ++i) {
        if (dim_size != list->value_length(i)) {
          fixed_shape = false;
          return list->values();
        }
      }
    }
    shape.emplace_back(std::size_t(dim_size));
    return list->values();
  };

  for (auto done = false; !done;) {
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
        return Status::NotImplemented("Shape derivation of ", tmp->type()->ToString(),
                                      " is not supported");
    }
  }

  if (fixed_shape) {
    // C-ORDER to FORTRAN-ORDER
    auto casa_shape = IPosition(ndim, 0);
    for (int dim = 0; dim < ndim; ++dim) {
      casa_shape[ndim - dim - 1] = shape[dim];
    }

    return ResultShapeData{column_desc.name(), std::make_optional(casa_shape), ndim,
                           column_desc.dataType(), std::nullopt};
  }

  ARROW_ASSIGN_OR_RAISE(auto row_shapes, GetRowShapes(data, ndim));
  return ResultShapeData{column_desc.name(), std::nullopt, ndim, column_desc.dataType(),
                         std::move(row_shapes)};
}

// Gets basic shape information from GetArrowResultShapeData.
// However, Arrow doesn't have a complex data type,
// We have chosen to represent complex values
// as lists of real-imaginary pairs.
// This function massages Arrow shape data
// into CASA shape data, by removing the
// associated dimension of 2 values from the shape data.
Result<ResultShapeData> GetResultShapeData(const ColumnDesc& column_desc,
                                           const std::shared_ptr<arrow::Array>& data) {
  ARROW_ASSIGN_OR_RAISE(auto shape_data, GetArrowResultShapeData(column_desc, data));
  // No conversion needed, just move
  if (shape_data.GetDataType() != DataType::TpComplex &&
      shape_data.GetDataType() != DataType::TpDComplex) {
    return shape_data;
  }

  // Convert the fixed shape
  if (shape_data.IsFixed()) {
    auto shape = shape_data.GetShape();
    if (IsDegenerateShape(shape) || shape[0] != 2) {
      return Status::Invalid("Arrow result data must supply pairs of values ",
                             "for complex valued column ", shape_data.GetName());
    }

    shape = shape.getLast(shape.size() - 1);
    auto ndim = int(shape.size());
    return ResultShapeData{std::move(shape_data.column_name_), std::move(shape), ndim,
                           shape_data.dtype_, std::nullopt};
  }

  // Modify the row shapes
  auto& shapes = shape_data.row_shapes_.value();
  for (std::size_t r = 0; r < shapes.size(); ++r) {
    if (IsDegenerateShape(shapes[r]) || shapes[r][0] != 2) {
      return Status::Invalid("Arrow result data must supply pairs of values ",
                             "for complex valued column ", shape_data.column_name_);
    }
    shapes[r] = shapes[r].getLast(shapes[r].size() - 1);
  }

  ARROW_ASSIGN_OR_RAISE(auto ndim, shape_data.ConsistentNDim());
  return ResultShapeData{std::move(shape_data.column_name_), std::nullopt, ndim - 1,
                         shape_data.dtype_, std::move(shape_data.row_shapes_)};
}

// Check that the number of rows in the table
// fit into the IndexType used by arcae
Status CheckRowNumberLimit(const std::string& column, rownr_t nrows) {
  if (nrows <= std::numeric_limits<IndexType>::max()) return Status::OK();
  return Status::IndexError("Number of rows ", nrows, " in column ", column,
                            " is too large for arcae's IndexType");
}

}  // namespace

// Maximum dimension size
std::size_t ResultShapeData::MaxDimensionSize() const noexcept {
  if (IsFixed()) return *std::max_element(shape_->begin(), shape_->end());
  std::size_t max_size = nRows();
  for (std::size_t r = 0; r < nRows(); ++r) {
    const auto& shape = GetRowShape(r);
    auto shape_max = *std::max_element(shape.begin(), shape.end());
    max_size = std::max<std::size_t>(max_size, shape_max);
  }
  return max_size;
}

Result<std::size_t> ResultShapeData::FlatOffset(
    const absl::Span<const IndexType>& index) const noexcept {
  ARROW_ASSIGN_OR_RAISE(auto ndim, ConsistentNDim());
  assert(index.size() == ndim);
  std::size_t offset = 0;

  // Fixed case
  if (IsFixed()) {
    const auto& shape = GetShape();
    for (int d = 0, product = 1; d < int(index.size()) && d < ndim; ++d) {
      offset += index[d] * product;
      product *= shape[d];
    }
    return offset;
  };

  // Variable case
  auto row = index[ndim - 1];
  assert(row < IndexType(nRows()));
  const auto shape = GetRowShape(row);

  for (int d = 0, product = 1; d < int(shape.size()) && d < ndim - 1; ++d) {
    offset += index[d] * product;
    product *= shape[d];
  }

  return std::accumulate(row_shapes_->begin(), row_shapes_->begin() + row, offset,
                         [](auto i, auto s) { return i + s.product(); });
}

// Number of elements in the result
std::size_t ResultShapeData::nElements() const noexcept {
  if (IsFixed()) return shape_.value().product();
  return std::accumulate(row_shapes_->begin(), row_shapes_->end(), std::size_t{0},
                         [](auto i, auto s) { return s.product() + i; });
}

Result<std::shared_ptr<arrow::Array>> ResultShapeData::GetShapeArray() const noexcept {
  ARROW_ASSIGN_OR_RAISE(auto ndim, ConsistentNDim());
  --ndim;  // without row
  auto nrow = nRows();

  if (ndim == 0) return std::make_shared<arrow::NullArray>(std::int64_t(nrow));

  auto builders = std::vector<arrow::Int32Builder>(ndim);
  auto shape_data_builder = arrow::Int32Builder();

  ARROW_RETURN_NOT_OK(shape_data_builder.Reserve(nrow * ndim));

  if (IsFixed()) {
    auto shape = GetShape();
    for (std::size_t row = 0; row < nrow; ++row) {
      for (int dim = ndim - 1; dim >= 0; --dim) {
        ARROW_RETURN_NOT_OK(shape_data_builder.Append(shape[dim]));
      }
    }
    ARROW_ASSIGN_OR_RAISE(auto shape_values, shape_data_builder.Finish());
    return arrow::FixedSizeListArray::FromArrays(shape_values, ndim);
  } else {
    auto null_nitmap_builder = arrow::TypedBufferBuilder<bool>();
    ARROW_RETURN_NOT_OK(null_nitmap_builder.Reserve(nrow * ndim));
    for (std::size_t row = 0; row < nrow; ++row) {
      auto shape = GetRowShape(row);
      ARROW_RETURN_NOT_OK(null_nitmap_builder.Append(shape.size() != 0));
      if (IsDegenerateShape(shape)) shape = IPosition(ndim, 0);
      for (int dim = ndim - 1; dim >= 0; --dim) {
        ARROW_RETURN_NOT_OK(shape_data_builder.Append(shape[dim]));
      }
    }
    ARROW_ASSIGN_OR_RAISE(auto nulls, null_nitmap_builder.Finish());
    ARROW_ASSIGN_OR_RAISE(auto shape_values, shape_data_builder.Finish());
    return arrow::FixedSizeListArray::FromArrays(shape_values, ndim, nulls);
  }

  return nullptr;
}

// Get ListArray offsets arrays
Result<std::vector<std::shared_ptr<arrow::Int32Array>>> ResultShapeData::GetOffsets()
    const noexcept {
  auto nrow = nRows();
  ARROW_ASSIGN_OR_RAISE(auto ndim, ConsistentNDim());
  // Don't build offsets for the row dimension (last in FORTRAN order)
  --ndim;

  if (ndim < 0) {
    return Status::NotImplemented(
        "Building offset arrays for scalar or variable rank arrays");
  }

  auto builders = std::vector<arrow::Int32Builder>(ndim);
  auto offsets = std::vector<std::shared_ptr<arrow::Int32Array>>(ndim);
  auto running_offsets = std::vector<std::size_t>(ndim, 0);

  // Initialise offsets
  for (int dim = 0; dim < ndim; ++dim) {
    ARROW_RETURN_NOT_OK(builders[dim].Reserve(nrow + 1));
    ARROW_RETURN_NOT_OK(builders[dim].Append(0));
  }

  // Compute number of elements in each row by creating
  // a product over each dimension
  auto BuildFn = [&](auto&& GetShapeFn) -> Status {
    using ItType = std::tuple<std::ptrdiff_t, std::size_t>;
    for (std::size_t row = 0; row < nrow; ++row) {
      for (auto [dim, product] = ItType{ndim - 1, 1}; dim >= 0; --dim) {
        auto dim_size = GetShapeFn(row, dim);
        for (std::size_t p = 0; p < product; ++p) {
          running_offsets[dim] += dim_size;
          ARROW_RETURN_NOT_OK(builders[dim].Append(running_offsets[dim]));
        }
        product *= dim_size;
      }
    }
    return Status::OK();
  };

  // Build the offset arrays
  if (!IsFixed()) {
    ARROW_RETURN_NOT_OK(BuildFn([&](auto r, auto d) {
      const auto& row_shape = GetRowShape(r);
      return IsDegenerateShape(row_shape) ? 0 : row_shape[d];
    }));
  } else {
    ARROW_RETURN_NOT_OK(BuildFn([&](auto r, auto d) { return (*shape_)[d]; }));
  }
  // Finish the offset arrays
  for (int dim = 0; dim < ndim; ++dim) {
    ARROW_RETURN_NOT_OK(builders[dim].Finish(&offsets[dim]));
  }
  return offsets;
}

Result<Selection> ResultShapeData::NegateMissingSelectedRows(const TableColumn& column,
                                                             const Selection& selection) {
  if (!IsFixed())
    return Status::NotImplemented(
        "Checking selections against non fixed-shape result data");

  bool has_span = selection.HasRowSpan();
  auto nrow = IndexType(column.nrow());
  bool all_rows_present = true;

  if (has_span) {
    for (auto r : selection.GetRowSpan()) {
      if (r >= 0 && r < nrow && !column.isDefined(r)) {
        all_rows_present = false;
        break;
      };
    }
  } else {
    for (IndexType r = 0; r < nrow; ++r) {
      if (!column.isDefined(r)) {
        all_rows_present = false;
        break;
      }
    }
  }

  if (all_rows_present) return selection;

  // Redefine the selection for the missing rows
  Index rows;

  if (has_span) {
    // We've been given a row span, check that
    auto span = selection.GetRowSpan();
    rows.resize(span.size());
    std::copy(std::begin(span), std::end(span), std::begin(rows));
  } else {
    rows.resize(column.nrow());
    std::iota(std::begin(rows), std::end(rows), 0);
  }

  if (rows.size() != nRows())
    return Status::IndexError("Number of rows in the result shape ", nRows(),
                              " does not match the number of rows ", rows.size(),
                              " in the ", has_span ? "selection." : "column.");

  for (auto it = std::begin(rows); it != std::end(rows); ++it) {
    if (*it >= nrow || (*it >= 0 && !column.isDefined(*it))) *it = -1;
  }

  // Add the modified rows
  auto builder = SelectionBuilder();
  builder.Order('C');
  builder.Add(std::move(rows));

  // Add secondary selection dimensions
  for (std::size_t d = 1; d < selection.Size(); ++d) {
    if (auto res = selection.CSpan(d); res.ok()) {
      auto span = res.ValueOrDie();
      builder.Add(Index(std::begin(span), std::end(span)));
    }
  }

  auto new_selection = builder.Build();
  return new_selection;
}

Result<ResultShapeData> ResultShapeData::FromArray(
    const TableColumn& column, const std::shared_ptr<arrow::Array>& array) {
  return GetResultShapeData(column.columnDesc(), array);
}

Result<ResultShapeData> ResultShapeData::MakeRead(const TableColumn& column,
                                                  const Selection& selection,
                                                  bool allow_missing_rows) {
  auto column_desc = column.columnDesc();
  auto column_name = column_desc.name();

  ARROW_RETURN_NOT_OK(CheckRowNumberLimit(column_name, column.nrow()));
  auto dtype = column_desc.dataType();
  auto nselrow = selection.HasRowSpan() ? ssize_t(selection.GetRowSpan().size())
                                        : ssize_t(column.nrow());

  // Fixed shape, easy case
  if (column_desc.isFixedShape()) {
    auto shape = column_desc.shape();
    ARROW_RETURN_NOT_OK(ClipShape(column_desc, shape, selection));
    shape.append(IPosition({nselrow}));
    auto ndim = int(shape.size());
    return ResultShapeData{std::move(column_name), std::move(shape), ndim,
                           std::move(dtype), std::nullopt};
  }

  // Get shapes of each row in the selection
  ARROW_ASSIGN_OR_RAISE(auto shapes, MakeRowData(column, selection, allow_missing_rows));

  auto missing_rows =
      std::accumulate(std::begin(shapes), std::end(shapes), std::size_t(0),
                      [&](auto i, auto s) { return i + int(IsDegenerateShape(s)); });

  if (!allow_missing_rows && missing_rows > 0) {
    return Status::Invalid(missing_rows, " missing rows in column ", column_name,
                           " for the given row selection");
  }

  auto fixed_shape = std::optional<IPosition>{std::nullopt};
  int ndim = -1;
  bool shapes_equal = false;

  // Identify fixed shapes and varying dimensionality
  for (auto it = shapes.begin(); it != shapes.end(); ++it) {
    if (IsDegenerateShape(*it)) continue;
    if (!fixed_shape) {
      fixed_shape = *it;
      ndim = it->size() + 1;
      shapes_equal = true;
    } else {
      if (fixed_shape->size() != it->size()) {
        ndim = -1;
        fixed_shape.reset();
        shapes_equal = false;
        break;
      }
      shapes_equal = shapes_equal && (*fixed_shape == *it);
    }
  }

  // Even though the column varies
  // the resultant shape after selection is fixed
  // There's no need to clip the shape as MakeRowData
  // has already done this
  if (shapes_equal && fixed_shape && missing_rows == 0) {
    fixed_shape->append(IPosition({nselrow}));
    auto ndim = int(fixed_shape->size());
    return ResultShapeData{column_name, std::move(fixed_shape), ndim, std::move(dtype),
                           std::nullopt};
  }

  // Shapes and their rank may vary per row and rows may be missing
  return ResultShapeData{column_name, std::nullopt, ndim, std::move(dtype),
                         std::move(shapes)};
}

Result<ResultShapeData> ResultShapeData::MakeWrite(
    const TableColumn& column, const std::shared_ptr<arrow::Array>& data,
    const Selection& selection) {
  if (!data) return Status::Invalid("data array is null");
  auto column_desc = column.columnDesc();
  auto column_name = column_desc.name();
  auto column_ndim = column_desc.ndim();
  ARROW_RETURN_NOT_OK(CheckRowNumberLimit(column_name, column.nrow()));

  ARROW_ASSIGN_OR_RAISE(auto shape_data, GetResultShapeData(column_desc, data));
  ARROW_ASSIGN_OR_RAISE(auto shape_ndim, shape_data.ConsistentNDim());

  if (column_ndim != -1 && shape_ndim != column_ndim + 1) {
    return Status::Invalid("Number of data dimensions ", shape_ndim,
                           " does not match number of column dimensions ",
                           column_desc.ndim() + 1);
  }

  // Check the row dimension against the selection
  if (selection.HasRowSpan()) {
    const auto& row_span = selection.GetRowSpan();
    if (row_span.size() > column.nrow()) {
      return Status::IndexError("Row selection size ", row_span.size(),
                                " exceeds the number of rows", " in the result shape ",
                                column.nrow());
    }

    // Check the row selection if valid
    for (std::size_t r = 0; r < row_span.size(); ++r) {
      if (row_span[r] >= IndexType(column.nrow())) {
        return Status::IndexError("Row selection ", row_span[r],
                                  " exceeds the number of rows ", column.nrow(),
                                  " in column ", column_desc.name());
      }
    }
  }

  // Check secondary dimensions against the selection
  ARROW_ASSIGN_OR_RAISE(std::ptrdiff_t row_dim, shape_data.ConsistentNDim());
  --row_dim;
  // No secondary dimensions, exit early
  if (row_dim <= 0) return shape_data;

  auto CheckSelectionAgainstShape = [&](const IndexSpan& span, const IPosition& shape,
                                        std::size_t dim) -> arrow::Status {
    if (ssize_t(span.size()) > shape[dim]) {
      return Status::IndexError("Selection size ", span.size(),
                                " exceeds the dimension size ", shape[dim],
                                " of dimension ", dim, " in column ", column_desc.name());
    }
    for (std::size_t i = 0; i < span.size(); ++i) {
      if (span[i] >= shape[dim]) {
        return Status::IndexError("Selection ", span[i], " exceeds the dimension size ",
                                  shape[dim], " of dimension ", dim, " in column ",
                                  column_desc.name());
      }
    }
    return Status::OK();
  };

  if (column_desc.isFixedShape()) {
    const auto& shape = column.shapeColumn();
    ARROW_ASSIGN_OR_RAISE(auto ndim, shape_data.ConsistentNDim());
    for (std::ptrdiff_t dim = 0; dim < row_dim; ++dim) {
      if (auto res = selection.FSpan(dim, ndim); res.ok()) {
        auto span = res.ValueOrDie();
        ARROW_RETURN_NOT_OK(CheckSelectionAgainstShape(span, shape, dim));
      }
    }
  } else {
    auto array_column = ArrayColumnBase(column);
    auto has_row_span = selection.HasRowSpan();
    auto row_span = has_row_span ? selection.GetRowSpan() : IndexSpan{};
    IndexType nrows = has_row_span ? row_span.size() : column.nrow();
    ARROW_ASSIGN_OR_RAISE(auto shape_ndim, shape_data.ConsistentNDim());

    for (IndexType r = 0; r < nrows; ++r) {
      IndexType row = has_row_span ? row_span[r] : r;
      if (row < 0) continue;  // Don't check negative row indices
      // If the row is defined, check the selection
      // against the shape of the row
      if (column.isDefined(row)) {
        auto row_shape = column.shape(row);
        for (std::ptrdiff_t dim = 0; dim < row_dim; ++dim) {
          if (auto res = selection.FSpan(dim, shape_ndim); res.ok()) {
            auto span = res.ValueOrDie();
            ARROW_RETURN_NOT_OK(CheckSelectionAgainstShape(span, row_shape, dim));
          }
        }
      } else {
        // The row is undefined
        // Set the row shape from the shape of the result,
        // taking any maximum selection into account
        auto row_shape = [&]() -> casacore::IPosition {
          if (!shape_data.IsFixed()) return shape_data.GetRowShape(row);
          auto shape = shape_data.GetShape();
          return shape.getFirst(shape.size() - 1);
        }();

        for (std::ptrdiff_t dim = 0; dim < row_dim; ++dim) {
          if (auto res = selection.FSpan(dim, shape_ndim); res.ok()) {
            auto span = res.ValueOrDie();
            auto span_max = *std::max_element(std::begin(span), std::end(span));
            row_shape[dim] = std::max<ssize_t>(span_max + 1, row_shape[dim]);
          }
        }
        array_column.setShape(row, row_shape);
      }
    }
  }

  return shape_data;
}

}  // namespace detail
}  // namespace arcae
