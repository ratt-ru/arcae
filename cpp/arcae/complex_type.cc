// Complex Number Extension Type

#include <mutex>

#include "arcae/complex_type.h"

namespace arcae {

bool ComplexFloatType::ExtensionEquals(const ExtensionType& other) const {
  const auto& other_ext = static_cast<const ExtensionType&>(other);
  return other_ext.extension_name() == this->extension_name();
}

bool ComplexDoubleType::ExtensionEquals(const ExtensionType& other) const {
  const auto& other_ext = static_cast<const ExtensionType&>(other);
  return other_ext.extension_name() == this->extension_name();
}


std::shared_ptr<arrow::DataType> complex64() {
  return std::make_shared<ComplexFloatType>();
}

std::shared_ptr<arrow::DataType> complex128() {
  return std::make_shared<ComplexDoubleType>();
}

/// NOTE(sjperkins)
// Suggestions on how to improve this welcome!
std::once_flag complex_float_registered;
std::once_flag complex_double_registered;

bool register_complex_types()
{
  std::call_once(complex_float_registered,
                 arrow::RegisterExtensionType,
                 std::make_shared<ComplexFloatType>());

  std::call_once(complex_double_registered,
                 arrow::RegisterExtensionType,
                 std::make_shared<ComplexDoubleType>());

  return true;
}

static auto complex_types_registered = register_complex_types();

}
