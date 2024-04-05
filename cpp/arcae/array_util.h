#ifndef ARCAE_ARRAY_UTIL
#define ARCAE_ARRAY_UTIL

#include <memory>

#include <arrow/api.h>

namespace arcae {

// Validate the constructed array
arrow::Status ValidateArray(const std::shared_ptr<arrow::Array> & array);

} // namespace arcae

#endif // ARCAE_ARRAY_UTIL