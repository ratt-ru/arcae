#ifndef ARCAE_TYPE_TRAITS_H
#define ARCAE_TYPE_TRAITS_H

#include <type_traits>

#include <arrow/util/functional.h>
#include <arrow/util/future.h>
#include <arrow/result.h>
#include <arrow/status.h>

namespace arcae {
namespace detail {

template <typename T>
struct is_arrow_result : std::false_type{};

template <typename T>
struct is_arrow_result<arrow::Result<T>> : std::true_type{};

template <typename T>
constexpr bool is_arrow_result_v = is_arrow_result<T>::value;

template <typename T>
struct is_arrow_status : std::false_type {};

template <>
struct is_arrow_status<arrow::Status> : std::true_type {};

template <typename T>
constexpr bool is_arrow_status_v = is_arrow_status<T>::value;

template <typename T>
struct is_arrow_future : std::false_type {};

template <typename T>
struct is_arrow_future<arrow::Future<T>> : std::true_type {};

template <typename T>
constexpr bool is_arrow_future_v = is_arrow_status<T>::value;

template <
  typename T,
  bool = is_arrow_result_v<T>,
  bool = is_arrow_status_v<T>>
struct ArrowResultTypeImpl { using type = T; };

template <typename T>
struct ArrowResultTypeImpl<T, false, true> { using type = arrow::Result<arrow::internal::Empty>; };

// template <typename T>
// struct ArrowResultTypeImpl<T, false, false> { using type = arrow::Future<T>; };

template <typename T>
struct ArrowResultTypeImpl<T, false, false> { using type = arrow::Result<T>; };

template <typename Fn, typename ... Args>
using ArrowResultType = typename ArrowResultTypeImpl<std::invoke_result_t<Fn, Args...>>::type;

}  // namespace detail
}  // namespace arcae

#endif // ARCAE_TYPE_TRAITS_H
