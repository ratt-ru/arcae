#ifndef ARCAE_ARRAY_UTIL
#define ARCAE_ARRAY_UTIL

#include <memory>

#include <arrow/api.h>

#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/tables/Tables/TableColumn.h>

namespace arcae {

// Describes the properties of an Arrow Array
struct ArrayProperties {
  std::optional<casacore::IPosition> shape;
  std::size_t ndim;
  std::shared_ptr<arrow::DataType> data_type;
  bool is_complex;
};

// Validate the constructed array
arrow::Status ValidateArray(const std::shared_ptr<arrow::Array> & array);
// Get a flattened array
arrow::Result<std::shared_ptr<arrow::Array>> GetFlatArray(
  std::shared_ptr<arrow::Array> data,
  bool nulls=false);
arrow::Status CheckElements(std::size_t map_size, std::size_t data_size);
// Get the Array properties of an Arrow Array
arrow::Result<ArrayProperties> GetArrayProperties(
  const casacore::TableColumn & column,
  const std::shared_ptr<arrow::Array> & data);



} // namespace arcae

#endif // ARCAE_ARRAY_UTIL