#ifndef ARCAE_COLUMN_WRITE_MAP_H
#define ARCAE_COLUMN_WRITE_MAP_H

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
#include "arrow/array/array_base.h"
#include "arrow/type.h"

namespace arcae {


// Provides Shape information from supplied input data,
// primarily for determining dimension sizes which
// are used to establish ranges for dimensions with no selection.
// This easy in the case of Fixed Shape columns.
// This may not be possible in the Variable column case.
struct ArrowShapeProvider {
  std::reference_wrapper<const casacore::TableColumn> column_;
  std::reference_wrapper<const ColumnSelection> selection_;
  std::shared_ptr<arrow::Array> data_;
  std::shared_ptr<arrow::DataType> data_type_;
  std::optional<casacore::IPosition> shape_;
  std::size_t ndim_;
  bool is_complex_;

  static arrow::Result<ArrowShapeProvider> Make(const casacore::TableColumn & column,
                                                const ColumnSelection & selection,
                                                const std::shared_ptr<arrow::Array> & data);

  // returns true if the column shape is fixed
  inline bool IsColumnFixed() const { return column_.get().columnDesc().isFixedShape(); }

  // return true if the column shape varys
  inline bool IsColumnVarying() const { return !IsColumnFixed(); }

  // Returns true if the data shape is fixed
  inline bool IsDataFixed() const { return shape_.has_value(); }

  // Return true if the data shape varys
  inline bool IsDataVarying() const { return !IsDataFixed(); }

  // Returns the number of dimensions, including row
  std::size_t nDim() const { return ndim_; }

  inline std::size_t RowDim() const { return nDim() - 1; }

  // Returns the dimension size of this column
  arrow::Result<std::size_t> DimSize(std::size_t dim) const;

  // Returns the dimension size of the colum for the given row
  std::size_t RowDimSize(casacore::rownr_t row, std::size_t dim) const;
};


struct ColumnWriteMap {
  enum InputOrder {C_ORDER=0, F_ORDER};

  std::reference_wrapper<const casacore::TableColumn> column_;
  ColumnMaps maps_;
  ColumnRanges ranges_;
  ArrowShapeProvider shape_provider_;

  inline const ColumnMap & DimMaps(std::size_t dim) const {
    assert(dim < nDim());
    return maps_[dim];
  }

  inline const ColumnRange & DimRanges(std::size_t dim) const {
    assert(dim < nDim());
    return ranges_[dim];
  }

  inline bool IsComplex() const {
    return shape_provider_.is_complex_;
  }

  inline std::size_t nDim() const {
    return shape_provider_.nDim();
  }

  inline std::size_t RowDim() const {
    assert(nDim() > 0);
    return nDim() - 1;
  }

  inline RangeIterator<ColumnWriteMap> RangeBegin() const {
    return RangeIterator{const_cast<ColumnWriteMap &>(*this), false};
  }

  inline RangeIterator<ColumnWriteMap> RangeEnd() const {
    return RangeIterator{const_cast<ColumnWriteMap &>(*this), true};
  }

  inline std::size_t RowDimSize(casacore::rownr_t row, std::size_t dim) const {
    return shape_provider_.RowDimSize(row, dim);
  }

  // Flattened offset in the output buffer
  std::size_t FlatOffset(const std::vector<std::size_t> & index) const;

  // Get the output shape, returns Status::Invalid if undefined
  arrow::Result<casacore::IPosition> GetOutputShape() const {
    if(shape_provider_.shape_) return shape_provider_.shape_.value();
    return arrow::Status::Invalid("Column ", column_.get().columnDesc().name(),
                                  " does not have a fixed shape");
  }

  // Is this a Fixed Shape case
  inline bool IsFixedShape() const {
    return shape_provider_.IsDataFixed();
  }

  // Factory method for making a ColumnWriteMap object
  static arrow::Result<ColumnWriteMap> Make(
      casacore::TableColumn & column,
      ColumnSelection selection,
      const std::shared_ptr<arrow::Array> & data,
      InputOrder order=InputOrder::C_ORDER);

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

#endif // ARCAE_COLUMN_WRITE_MAP_H