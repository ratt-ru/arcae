#ifndef ARCAE_READ_IMPL_H
#define ARCAE_READ_IMPL_H

#include <memory>

#include <arrow/api.h>

#include "arcae/isolated_table_proxy.h"
#include "arcae/selection.h"

namespace arcae {
namespace detail {

arrow::Future<std::shared_ptr<arrow::Array>>
ReadImpl(const std::shared_ptr<IsolatedTableProxy> & itp,
         const std::string & column,
         const Selection & selection,
         const std::shared_ptr<arrow::Array> & result);

}  // namespace detail
}  // namespace arcae


#endif  // ARCAE_READ_IMPL_H