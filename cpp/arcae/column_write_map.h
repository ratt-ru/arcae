#ifndef ARCAE_COLUMN_WRITE_MAP_H
#define ARCAE_COLUMN_WRITE_MAP_H

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include <arrow/array.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include <casacore/casa/aipsxtype.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/tables/Tables/TableColumn.h>

#include "arcae/base_column_map.h"

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
  bool IsColumnFixed() const { return column_.get().columnDesc().isFixedShape(); }

  // return true if the column shape varys
  bool IsColumnVarying() const { return !IsColumnFixed(); }

  // Returns true if the data shape is fixed
  bool IsDataFixed() const { return shape_.has_value(); }

  // Return true if the data shape varys
  bool IsDataVarying() const { return !IsDataFixed(); }

  // Returns the number of dimensions, including row
  std::size_t nDim() const { return ndim_; }

  std::size_t RowDim() const { return nDim() - 1; }

  // Returns the dimension size of this column
  arrow::Result<std::size_t> DimSize(std::size_t dim) const;

  // Returns the dimension size of the colum for the given row
  std::size_t RowDimSize(casacore::rownr_t row, std::size_t dim) const;
};


struct ColumnWriteMap : public BaseColumnMap<ColumnWriteMap> {
  std::shared_ptr<arrow::Array> data_;
  ArrowShapeProvider shape_provider_;


  bool IsComplex() const {
    return shape_provider_.is_complex_;
  }

  std::size_t nDim() const {
    return shape_provider_.nDim();
  }

  std::size_t RowDimSize(casacore::rownr_t row, std::size_t dim) const {
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
  bool IsFixedShape() const {
    return shape_provider_.IsDataFixed();
  }

  // Factory method for making a ColumnWriteMap object
  static arrow::Result<ColumnWriteMap> Make(
      casacore::TableColumn & column,
      ColumnSelection selection,
      const std::shared_ptr<arrow::Array> & data,
      MapOrder order=MapOrder::C_ORDER);
};


} // namespace arcae

#endif // ARCAE_COLUMN_WRITE_MAP_H