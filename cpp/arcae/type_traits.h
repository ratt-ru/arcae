#ifndef ARCAE_TYPE_TRAITS_H
#define ARCAE_TYPE_TRAITS_H

#include <type_traits>

#include <arrow/util/functional.h>
#include <arrow/util/future.h>
#include <arrow/result.h>
#include <arrow/status.h>

namespace arcae {
namespace detail {

// Template metaprogramming construct that checks
// whether the given type is an arrow::Result
template <typename T>
struct is_arrow_result : std::false_type{};

template <typename T>
struct is_arrow_result<arrow::Result<T>> : std::true_type{};

template <typename T>
constexpr bool is_arrow_result_v = is_arrow_result<T>::value;

template <typename T>
struct is_arrow_status : std::false_type {};

// Template metaprogramming construct that check
// whether the given type is an arrow::Status
template <>
struct is_arrow_status<arrow::Status> : std::true_type {};

template <typename T>
constexpr bool is_arrow_status_v = is_arrow_status<T>::value;

template <typename T>
struct is_arrow_future : std::false_type {};

// Template metaprogramming construct that checks
// whether the given type is an arrow::Future

template <typename T>
struct is_arrow_future<arrow::Future<T>> : std::true_type {};

template <typename T>
constexpr bool is_arrow_future_v = is_arrow_future<T>::value;

// Template metaprogramming construct that coerces function
// return types into an arrow::Result, or an arrow::Future
template <
  typename T,
  bool = is_arrow_result_v<T>,
  bool = is_arrow_status_v<T>,
  bool = is_arrow_future_v<T>>
struct ArrowResultTypeImpl { using type = T; };

template <typename T>
struct ArrowResultTypeImpl<T, false, false, false> { using type = arrow::Result<T>; };

template <typename T>
struct ArrowResultTypeImpl<T, false, true, false> { using type = arrow::Result<arrow::internal::Empty>; };

template <typename T>
struct ArrowResultTypeImpl<T, false, false, true> { using type = arrow::Future<T>; };

template <typename Fn, typename ... Args>
using ArrowResultType = typename ArrowResultTypeImpl<std::invoke_result_t<Fn, Args...>>::type;

// Template metaprogramming constructs that coerce function
// return types into an arrow::Future
template <
  typename T,
  bool = is_arrow_result_v<T>,
  bool = is_arrow_status_v<T>,
  bool = is_arrow_future_v<T>>
struct ArrowFutureTypeImpl { using type = T; };

template <typename T>
struct ArrowFutureTypeImpl<T, false, false, false> { using type = arrow::Future<T>; };

template <typename T>
struct ArrowFutureTypeImpl<T, false, true, false> { using type = arrow::Future<arrow::internal::Empty>; };

template <typename T>
struct ArrowFutureTypeImpl<T, false, false, true> { using type = T; };

template <typename Fn, typename ... Args>
using ArrowFutureType = typename ArrowFutureTypeImpl<std::invoke_result_t<Fn, Args...>>::type;

}  // namespace detail
}  // namespace arcae

#endif // ARCAE_TYPE_TRAITS_H
