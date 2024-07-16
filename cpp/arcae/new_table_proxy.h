#ifndef ARCAE_NEW_TABLE_PROXY_H
#define ARCAE_NEW_TABLE_PROXY_H

#include <memory>

#include <arrow/util/thread_pool.h>
#include <arrow/result.h>

#include <casacore/tables/Tables/TableProxy.h>

#include "arcae/isolated_table_proxy.h"
#include "arcae/selection.h"
#include "arcae/type_traits.h"

namespace arcae {
namespace detail {

class NewTableProxy {
public:
  // Construct an NewTableProxy with the supplied function
  template <
    typename Fn,
    typename = std::enable_if<
                  std::is_same_v<
                    ArrowResultType<Fn>,
                    arrow::Result<std::shared_ptr<casacore::TableProxy>>>>>
  static arrow::Result<std::shared_ptr<NewTableProxy>> Make(
      Fn && functor,
      std::shared_ptr<arrow::internal::ThreadPool> io_pool=nullptr) {
    struct enable_make_shared_ntp : public NewTableProxy {};
    std::shared_ptr<NewTableProxy> ntp = std::make_shared<enable_make_shared_ntp>();
    ARROW_ASSIGN_OR_RAISE(ntp->itp_, IsolatedTableProxy::Make(std::move(functor), io_pool));
    return ntp;
  }

  arrow::Result<std::shared_ptr<arrow::Array>> GetColumn(
    const std::string & column,
    const Selection & selection=Selection(),
    const std::shared_ptr<arrow::Array> & result=nullptr) const;

private:
  std::shared_ptr<IsolatedTableProxy> itp_;
};

} // namespace detail
} // namespace arcae

#endif  // ARCAE_NEW_TABLE_PROXY_H