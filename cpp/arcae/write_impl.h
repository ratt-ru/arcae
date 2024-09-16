#ifndef ARCAE_WRITE_IMPL_H
#define ARCAE_WRITE_IMPL_H

#include <string>

#include <arrow/api.h>
#include <arrow/util/future.h>

#include "arcae/isolated_table_proxy.h"
#include "arcae/selection.h"

namespace arcae {
namespace detail {

arrow::Future<bool> WriteImpl(const std::shared_ptr<IsolatedTableProxy>& itp,
                              const std::string& column,
                              const std::shared_ptr<arrow::Array>& data,
                              const Selection& selection = Selection());

}  // namespace detail
}  // namespace arcae

#endif  // ARCAE_WRITE_IMPL_H
