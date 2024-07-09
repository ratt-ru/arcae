#ifndef ARCAE_TYPE_TRAITS_H
#define ARCAE_TYPE_TRAITS_H

#include <type_traits>

#include <arrow/util/functional.h>
#include <arrow/util/future.h>
#include <arrow/result.h>
#include <arrow/status.h>

namespace arcae {
namespace detail {

// Helper class for use with static_assert
template <class...> constexpr std::false_type always_false{};

// Coerce the return value of Fn(Args...) into an arrow::Result
template <typename Fn, typename ... Args>
using ArrowResultType = typename arrow::EnsureResult<std::invoke_result_t<Fn, Args...>>::type;

// Coerce the return value of Fn(Args...) into an arrow::Future
template <typename Fn, typename ... Args>
using ArrowFutureType = typename arrow::EnsureFuture<std::invoke_result_t<Fn, Args...>>::type;

}  // namespace detail
}  // namespace arcae

#endif // ARCAE_TYPE_TRAITS_H
