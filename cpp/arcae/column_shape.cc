#include "arcae/column_shape.h"

#include <memory>

#include <casacore/casa/aipsxtype.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/tables/Tables/ColumnDesc.h>
#include <casacore/tables/Tables/TableColumn.h>

#include <arrow/result.h>
#include <arrow/status.h>

#include "arcae/selection.h"

using ::arcae::detail::IndexType;

namespace arcae {
namespace detail {
namespace {

arrow::Result<casacore::IPosition> GetRowShape(
    const casacore::TableColumn & column,
    IndexType r) {

  if(column.isDefined(r)) return column.shape(r);
  return arrow::Status::IndexError("Row ", r, " in column ",
                                    column.columnDesc().name(),
                                    " is not defined");
}

arrow::Status ClipShape(
    const casacore::ColumnDesc & column_desc,
    casacore::IPosition & shape,
    const Selection & selection) {

  if(selection.Size() <= 1) return arrow::Status::OK();
  for(std::size_t dim=0; dim < shape.size(); ++dim) {
    if(auto result = selection.CSpan(dim, shape.size() + 1); result.ok()) {
      auto span = result.ValueOrDie();
      for(auto i: span) {
        if(i >= shape[dim]) {
          return arrow::Status::IndexError("Selection index ", i,
                                           " exceeds dimension ", dim,
                                           " of shape ", shape,
                                           " in column ",
                                           column_desc.name());
        }
      }
      shape[dim] = span.size();
    }
  }
  return arrow::Status::OK();
}

arrow::Result<std::unique_ptr<ColumnShapeData::RowShapes>>
MakeRowData(const casacore::TableColumn &column,
            const Selection &selection) {

  ColumnShapeData::RowShapes shapes;
  const auto & column_desc = column.columnDesc();

  if(selection.Size() > 0) {
    auto span = selection.GetRowSpan();
    shapes.reserve(span.size());
    for(std::size_t r=0; r < span.size(); ++r) {
      ARROW_ASSIGN_OR_RAISE(auto shape, GetRowShape(column, span[r]));
      ARROW_RETURN_NOT_OK(ClipShape(column_desc, shape, selection));
      shapes.push_back(std::move(shape));
    }
  } else {
    shapes.reserve(column.nrow());
    for(casacore::rownr_t r=0; r < column.nrow(); ++r) {
      ARROW_ASSIGN_OR_RAISE(auto shape, GetRowShape(column, r));
      ARROW_RETURN_NOT_OK(ClipShape(column_desc, shape, selection));
      shapes.push_back(std::move(shape));
    }
  }

  return std::make_unique<ColumnShapeData::RowShapes>(std::move(shapes));
}

}  // namespace

arrow::Result<ColumnShapeData>
ColumnShapeData::Make(
    const casacore::TableColumn &column,
    const Selection &selection) {
  auto column_desc = column.columnDesc();
  auto column_name = column_desc.name();
  auto dtype = column_desc.dataType();
  auto nselrow = selection.HasRowSpan() ? ssize_t(selection.GetRowSpan().size())
                                        : ssize_t(column.nrow());

  // The number of dimensions varies per row
  // This case is not handled
  if(column_desc.ndim() == -1) {
    return arrow::Status::NotImplemented(
      "Column ", column_name,
      " has varying dimensions");
  }

  // Fixed shape, easy case
  if(column_desc.isFixedShape()) {
    auto shape = column_desc.shape();
    ARROW_RETURN_NOT_OK(ClipShape(column_desc, shape, selection));
    shape.append(casacore::IPosition({nselrow}));
    std::size_t ndim = shape.size();
    return ColumnShapeData{
      std::move(column_name),
      std::move(shape),
      ndim,
      std::move(dtype)};
  }

  // Get shapes of each row in the selection
  ARROW_ASSIGN_OR_RAISE(auto shapes, MakeRowData(column, selection));
  auto shape = std::optional<casacore::IPosition>{};
  int ndim = -1;
  bool first = true;
  bool shapes_equal = true;

  // Identify fixed shapes and varying dimensionality
  for(auto it = shapes->begin(); it != shapes->end(); ++it) {
    if(first) {
      shape = *it;
      ndim = it->size();
      first = false;
    } else {
      if(shapes->front().size() != it->size()) {
        ndim = -1;
        shape.reset();
        shapes_equal = false;
        break;
      }
      shapes_equal = shapes_equal && (shapes->front() == *it);
    }
  }

  // The number of dimensions varies per row
  // This case is not handled
  if(ndim == -1) {
    return arrow::Status::NotImplemented(
      "Column ", column_name,
      " has varying dimensions");
  }

  // Even though the column varys
  // the resultant shape after selection is fixed
  if(shapes_equal) {
    shape->append(casacore::IPosition({nselrow}));
    ndim = shape->size();
    return ColumnShapeData{
      column_name,
      std::move(shape),
      std::size_t(ndim),
      std::move(dtype)};
  }

  // Shapes vary per row
  return ColumnShapeData{
    column_name,
    std::nullopt,
    std::size_t(ndim + 1),
    std::move(dtype),
    std::move(shapes)};
}

} // namespace detail
} // namespace arcae