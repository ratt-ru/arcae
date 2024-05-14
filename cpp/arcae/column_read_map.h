#ifndef ARCAE_COLUMN_READ_MAP_H
#define ARCAE_COLUMN_READ_MAP_H

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <casacore/casa/aipsxtype.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/tables/Tables/TableColumn.h>

#include "arcae/base_column_map.h"

namespace arcae {

// Holds variable shape data for a column
struct VariableShapeData {
  // Factory method for creating Variable Shape Data
  static arrow::Result<std::unique_ptr<VariableShapeData>>
  Make(const casacore::TableColumn & column, const ColumnSelection & selection);
  // Returns true if the data shapes are fixed in practice
  bool IsActuallyFixed() const;
  // Number of dimensions, excluding row
  std::size_t nDim() const;

  std::vector<casacore::IPosition> row_shapes_;
  std::vector<std::shared_ptr<arrow::Int32Array>> offsets_;
  std::size_t ndim_;
  std::optional<casacore::IPosition> shape_;
};


// Provides Shape information for this column,
// primarily for determining dimension sizes which
// are used to establish ranges for dimensions with no selection.
// This easy in the case of Fixed Shape columns.
// This may not be possible in the Variable column case.
struct ShapeProvider {
  std::reference_wrapper<const casacore::TableColumn> column_;
  std::reference_wrapper<const ColumnSelection> selection_;
  std::unique_ptr<VariableShapeData> var_data_;
  std::size_t ndim_;

  static arrow::Result<ShapeProvider> Make(const casacore::TableColumn & column,
                                           const ColumnSelection & selection);

  // Returns true if the column is defined as having a fixed shape
  bool IsDefinitelyFixed() const {
    return var_data_ == nullptr;
  }

  // Return true if the column is defined as having a varying shape
  bool IsVarying() const {
    return !IsDefinitelyFixed();
  }

  // Return true if the column has a fixed shape in practice
  bool IsActuallyFixed() const {
    return IsDefinitelyFixed() || var_data_->IsActuallyFixed();
  }

  // Returns the number of dimensions, including row
  std::size_t nDim() const {
    return ndim_;
  }

  std::size_t RowDim() const { return nDim() - 1; }

  // Returns the dimension size of this column
  arrow::Result<std::size_t> DimSize(std::size_t dim) const;

  // Returns the dimension size of the colum for the given row
  std::size_t RowDimSize(casacore::rownr_t row, std::size_t dim) const;
};


struct ColumnReadMap : public BaseColumnMap<ColumnReadMap> {
  ShapeProvider shape_provider_;
  std::optional<casacore::IPosition> output_shape_;

  std::size_t nDim() const {
    return shape_provider_.nDim();
  }

  std::size_t RowDimSize(casacore::rownr_t row, std::size_t dim) const {
    return shape_provider_.RowDimSize(row, dim);
  }

  arrow::Result<std::vector<std::shared_ptr<arrow::Int32Array>>> GetOffsets() const;

  // Flattened offset in the output buffer
  std::size_t FlatOffset(const std::vector<std::size_t> & index) const;

  // Get the output shape, returns Status::Invalid if undefined
  arrow::Result<casacore::IPosition> GetOutputShape() const {
    if(output_shape_) return output_shape_.value();
    return arrow::Status::Invalid("Column ", column_.get().columnDesc().name(),
                                  " does not have a fixed shape");
  }

  // Is this a Fixed Shape case
  bool IsFixedShape() const {
    return shape_provider_.IsActuallyFixed();
  }

  // Factory method for making a ColumnReadMap object
  static arrow::Result<ColumnReadMap> Make(
      const casacore::TableColumn & column,
      ColumnSelection selection,
      MapOrder order=MapOrder::C_ORDER);
};


} // namespace arcae

#endif // ARCAE_COLUMN_READ_MAP_H