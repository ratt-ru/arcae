#ifndef ARCAE_TYPE_TRAITS_H
#define ARCAE_TYPE_TRAITS_H

#include <type_traits>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/functional.h>
#include <arrow/util/future.h>

#include <casacore/casa/BasicSL/Complexfwd.h>
#include <casacore/casa/Utilities/DataType.h>
#include <casacore/casa/aipstype.h>
#include <casacore/casa/aipsxtype.h>

namespace arcae {
namespace detail {

// Helper class for use with static_assert
template <class...>
constexpr std::false_type always_false{};

// Coerce the return value of Fn(Args...) into an arrow::Result
template <typename Fn, typename... Args>
using ArrowResultType =
    typename arrow::EnsureResult<std::invoke_result_t<Fn, Args...>>::type;

// Coerce the return value of Fn(Args...) into an arrow::Future
template <typename Fn, typename... Args>
using ArrowFutureType =
    typename arrow::EnsureFuture<std::invoke_result_t<Fn, Args...>>::type;

// Get the size of a CASA Data Type
arrow::Result<std::size_t> CasaDataTypeSize(casacore::DataType data_type);

// Get the primitive arrow Data type associated with a CASA Data Type
arrow::Result<std::shared_ptr<arrow::DataType>> ArrowDataType(
    casacore::DataType data_type);

// From https://stackoverflow.com/a/35300172/1611416
// Supports initialisation of aggregate structures in std::make_shared<T>
//
//   struct Data { std::string a; int b; float c; };
//   std::make_shared<AggregateAdapter<Data>>{"hello", 1, 10.0f};
template <class T>
struct AggregateAdapter : public T {
  template <class... Args>
  AggregateAdapter(Args&&... args) : T{std::forward<Args>(args)...} {}
};

template <casacore::DataType CDT>
struct CasaDataTypeTraits {
  using ArrowType = std::false_type;
  using CasaType = std::false_type;
  static std::shared_ptr<arrow::DataType> ArrowDataType() { return nullptr; }
  static constexpr bool is_complex = false;
};

template <>
struct CasaDataTypeTraits<casacore::DataType::TpBool> {
  using ArrowType = arrow::UInt8Type;
  using CasaType = casacore::Bool;
  static std::shared_ptr<arrow::DataType> ArrowDataType() { return arrow::uint8(); }
  static constexpr bool is_complex = false;
};

template <>
struct CasaDataTypeTraits<casacore::DataType::TpChar> {
  using ArrowType = arrow::Int8Type;
  using CasaType = casacore::Char;
  static std::shared_ptr<arrow::DataType> ArrowDataType() { return arrow::int8(); }
  static constexpr bool is_complex = false;
};

template <>
struct CasaDataTypeTraits<casacore::DataType::TpUChar> {
  using ArrowType = arrow::UInt8Type;
  using CasaType = casacore::uChar;
  static std::shared_ptr<arrow::DataType> ArrowDataType() { return arrow::uint8(); }
  static constexpr bool is_complex = false;
};

template <>
struct CasaDataTypeTraits<casacore::DataType::TpShort> {
  using ArrowType = arrow::Int16Type;
  using CasaType = casacore::Short;
  static std::shared_ptr<arrow::DataType> ArrowDataType() { return arrow::int16(); }
  static constexpr bool is_complex = false;
};

template <>
struct CasaDataTypeTraits<casacore::DataType::TpUShort> {
  using ArrowType = arrow::UInt16Type;
  using CasaType = casacore::uShort;
  static std::shared_ptr<arrow::DataType> ArrowDataType() { return arrow::uint16(); }
  static constexpr bool is_complex = false;
};

template <>
struct CasaDataTypeTraits<casacore::DataType::TpInt> {
  using ArrowType = arrow::Int32Type;
  using CasaType = casacore::Int;
  static std::shared_ptr<arrow::DataType> ArrowDataType() { return arrow::int32(); }
  static constexpr bool is_complex = false;
};

template <>
struct CasaDataTypeTraits<casacore::DataType::TpUInt> {
  using ArrowType = arrow::UInt32Type;
  using CasaType = casacore::uInt;
  static std::shared_ptr<arrow::DataType> ArrowDataType() { return arrow::uint32(); }
  static constexpr bool is_complex = false;
};

template <>
struct CasaDataTypeTraits<casacore::DataType::TpInt64> {
  using ArrowType = arrow::Int64Type;
  using CasaType = casacore::Int64;
  static std::shared_ptr<arrow::DataType> ArrowDataType() { return arrow::int64(); }
  static constexpr bool is_complex = false;
};

template <>
struct CasaDataTypeTraits<casacore::DataType::TpFloat> {
  using ArrowType = arrow::FloatType;
  using CasaType = casacore::Float;
  static std::shared_ptr<arrow::DataType> ArrowDataType() { return arrow::float32(); }
  static constexpr bool is_complex = false;
};

template <>
struct CasaDataTypeTraits<casacore::DataType::TpDouble> {
  using ArrowType = arrow::DoubleType;
  using CasaType = casacore::Double;
  static std::shared_ptr<arrow::DataType> ArrowDataType() { return arrow::float64(); }
  static constexpr bool is_complex = false;
};

template <>
struct CasaDataTypeTraits<casacore::DataType::TpComplex> {
  using ArrowType = arrow::FloatType;
  using CasaType = casacore::Complex;
  static std::shared_ptr<arrow::DataType> ArrowDataType() { return arrow::float32(); }
  static constexpr bool is_complex = true;
};

template <>
struct CasaDataTypeTraits<casacore::DataType::TpDComplex> {
  using ArrowType = arrow::DoubleType;
  using CasaType = casacore::DComplex;
  static std::shared_ptr<arrow::DataType> ArrowDataType() { return arrow::float64(); }
  static constexpr bool is_complex = true;
};

template <>
struct CasaDataTypeTraits<casacore::DataType::TpString> {
  using ArrowType = arrow::StringType;
  using CasaType = casacore::String;
  static std::shared_ptr<arrow::DataType> ArrowDataType() { return arrow::utf8(); }
  static constexpr bool is_complex = false;
};

}  // namespace detail
}  // namespace arcae

#endif  // ARCAE_TYPE_TRAITS_H
