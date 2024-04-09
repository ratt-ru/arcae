#ifndef ARCAE_ARRAY_UTIL
#define ARCAE_ARRAY_UTIL

#include <memory>

#include <arrow/api.h>

namespace arcae {

// Validate the constructed array
arrow::Status ValidateArray(const std::shared_ptr<arrow::Array> & array);
arrow::Result<std::shared_ptr<arrow::Array>> GetFlatArray(std::shared_ptr<arrow::Array> data, bool nulls=false);

} // namespace arcae

#endif // ARCAE_ARRAY_UTIL