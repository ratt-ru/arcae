#ifndef ARCAE_COLUMN_SHAPE_H
#define ARCAE_COLUMN_SHAPE_H

#include <cassert>
#include <memory>
#include <optional>
#include <vector>

#include <arrow/result.h>

#include <casacore/tables/Tables/TableColumn.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/Utilities/DataType.h>

#include "arcae/selection.h"

namespace arcae {
namespace detail {

struct ColumnShapeData {
  using RowShapes = std::vector<casacore::IPosition>;

  std::string column_name_;
  std::optional<casacore::IPosition> shape_;
  std::size_t ndim_;
  casacore::DataType dtype_;
  std::unique_ptr<RowShapes> row_shapes_;

  // Return the Column Name
  const std::string & GetName() const { return column_name_; }
  // Return the Number of Dimensions in the Column
  std::size_t nDim() const { return ndim_; }
  // Does the Column have a fixed shape?
  bool IsFixed() const { return shape_.has_value(); }

  // Return the column shape if the column is fixed
  // Should only be called if IsFixed() == true
  casacore::IPosition GetShape() const {
    assert(shape_.has_value());
    return shape_.value();
  }

  // Return the shape of the row, if the column is not fixed
  casacore::IPosition GetRowShape(std::size_t row) const {
    assert(!IsFixed());
    assert(row_shapes_);
    assert(row < row_shapes_->size());
    return row_shapes_->operator[](row);
  }

  casacore::DataType GetDataType() const { return dtype_; }

  // Create a ColumnShapeData instance
  // from a TableColumn and a column Selection
  static arrow::Result<ColumnShapeData> Make(
      const casacore::TableColumn & column,
      const Selection & selection=Selection());
};

} // namespace detail
} // namespace arcae

#endif // ARCAE_COLUMN_SHAPE_H