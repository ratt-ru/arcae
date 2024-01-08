#ifndef ARCAE_COLUMN_READ_MAP_H
#define ARCAE_COLUMN_READ_MAP_H

#include <cassert>
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
#include <casacore/casa/Arrays/Slicer.h>
#include <casacore/tables/Tables/TableColumn.h>

#include "arcae/map_iterator.h"

namespace arcae {

// Holds variable shape data for a column
struct VariableShapeData {
  // Factory method for creating Variable Shape Data
  static arrow::Result<std::unique_ptr<VariableShapeData>>
  MakeFromData(const casacore::TableColumn & column,
               const std::shared_ptr<arrow::Array> & data,
               const ColumnSelection & selection);

  static arrow::Result<std::unique_ptr<VariableShapeData>>
  MakeFromColumn(const casacore::TableColumn & column,
       const ColumnSelection & selection);
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

  static arrow::Result<ShapeProvider> Make(const casacore::TableColumn & column,
                                           const ColumnSelection & selection,
                                           const std::shared_ptr<arrow::Array> & data=nullptr);

  // Returns true if the column is defined as having a fixed shape
  inline bool IsDefinitelyFixed() const {
    return var_data_ == nullptr;
  }

  // Return true if the column is defined as having a varying shape
  inline bool IsVarying() const {
    return !IsDefinitelyFixed();
  }

  // Return true if the column has a fixed shape in practice
  inline bool IsActuallyFixed() const {
    return IsDefinitelyFixed() || var_data_->IsActuallyFixed();
  }

  // Returns the number of dimensions, including row
  std::size_t nDim() const {
    return (IsDefinitelyFixed() ? column_.get().columnDesc().ndim() : var_data_->nDim()) + 1;
  }

  inline std::size_t RowDim() const { return nDim() - 1; }

  // Returns the dimension size of this column
  arrow::Result<std::size_t> DimSize(std::size_t dim) const;

  // Returns the dimension size of the colum for the given row
  std::size_t RowDimSize(casacore::rownr_t row, std::size_t dim) const;
};


struct ColumnReadMap {
  enum InputOrder {C_ORDER=0, F_ORDER};

  std::reference_wrapper<const casacore::TableColumn> column_;
  ColumnMaps maps_;
  ColumnRanges ranges_;
  ShapeProvider shape_provider_;
  std::optional<casacore::IPosition> output_shape_;

  inline const ColumnMap & DimMaps(std::size_t dim) const {
    assert(dim < nDim());
    return maps_[dim];
  }

  inline const ColumnRange & DimRanges(std::size_t dim) const {
    assert(dim < nDim());
    return ranges_[dim];
  }

  inline std::size_t nDim() const {
    return shape_provider_.nDim();
  }

  inline std::size_t RowDim() const {
    assert(nDim() > 0);
    return nDim() - 1;
  }

  inline RangeIterator<ColumnReadMap> RangeBegin() const {
    return RangeIterator{const_cast<ColumnReadMap &>(*this), false};
  }

  inline RangeIterator<ColumnReadMap> RangeEnd() const {
    return RangeIterator{const_cast<ColumnReadMap &>(*this), true};
  }

  inline std::size_t RowDimSize(casacore::rownr_t row, std::size_t dim) const {
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
  inline bool IsFixedShape() const {
    return shape_provider_.IsActuallyFixed();
  }

  // Factory method for making a ColumnReadMap object
  static arrow::Result<ColumnReadMap> Make(
      const casacore::TableColumn & column,
      ColumnSelection selection,
      InputOrder order=InputOrder::C_ORDER,
      const std::shared_ptr<arrow::Array> & data=nullptr);

  // Number of disjoint ranges in this map
  std::size_t nRanges() const;

  // Returns true if this is a simple map or, a map that only contains
  // a single range and thereby removes the need to read separate ranges of
  // data and copy those into a final buffer.
  bool IsSimple() const;

  // Find the total number of elements formed
  // by the disjoint ranges in this map
  std::size_t nElements() const;
};


} // namespace arcae

#endif // ARCAE_COLUMN_READ_MAP_H