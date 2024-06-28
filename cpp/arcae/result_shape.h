#ifndef ARCAE_RESULT_SHAPE_H
#define ARCAE_RESULT_SHAPE_H

#include <cassert>
#include <memory>
#include <optional>
#include <vector>

#include <arrow/api.h>
#include <arrow/result.h>

#include <casacore/tables/Tables/TableColumn.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/Utilities/DataType.h>

#include "arcae/selection.h"

namespace arcae {
namespace detail {

// Holds information about the shape of:
//
// 1. A read from a column
// 2. A write to a column
//
// In the case of reads, this information is either derived
// from a synthesis of:
//
// 1. A selection of column data.
// 2. A result array
//
// In the case of writes, this data is derived from
//
// 1. The result array
//
// This shape may be fixed, in which case `IsFixed` returns true
// and the shape can obtained by calling `GetShape`.
//
// Otherwise the shape varies per row, in which case `IsFixed` return false
// and each individual row shape can be obtained by calling `GetRowShape`.
struct ResultShapeData {
  using RowShapes = std::vector<casacore::IPosition>;

  std::string column_name_;
  std::optional<casacore::IPosition> shape_;
  std::size_t ndim_;
  casacore::DataType dtype_;
  std::optional<RowShapes> row_shapes_;

  // Return the Column Name
  const std::string & GetName() const { return column_name_; }

  // Return the Number of Dimensions in the Column
  std::size_t nDim() const { return ndim_; }

  // Number of Rows in the Shape
  std::size_t nRows() const {
    return IsFixed() ? shape_->last() : row_shapes_->size();
  }

  // Is the result shape fixed?
  bool IsFixed() const { return shape_.has_value(); }

  // Return the shape if it is fixed
  // Requires IsFixed() == true.
  casacore::IPosition GetShape() const {
    assert(IsFixed());
    return shape_.value();
  }

  // Return the shape of the row
  // Requires IsFixed() == false.
  const casacore::IPosition & GetRowShape(std::size_t row) const {
    assert(!IsFixed());
    assert(row_shapes_);
    assert(row < row_shapes_->size());
    return row_shapes_->operator[](row);
  }

  // Get the underlying CASA Data Type
  casacore::DataType GetDataType() const { return dtype_; }

  // Get the number of elements in the result
  std::size_t nElements() const;

  // Create a ResultShapeData instance suitable for reading
  static arrow::Result<ResultShapeData> MakeRead(
      const casacore::TableColumn & column,
      const Selection & selection=Selection(),
      const std::shared_ptr<arrow::Array> & result=nullptr);

  // Create a ResultShapeData instance suitable for writing
  static arrow::Result<ResultShapeData> MakeWrite(
    const casacore::TableColumn & column,
    const std::shared_ptr<arrow::Array> & data,
    const Selection & selection=Selection());
};

} // namespace detail
} // namespace arcae

#endif // ARCAE_RESULT_SHAPE_H