#include "arcae/isolated_table_proxy.h"

#include <memory>

#include <arrow/util/logging.h>

#include <casacore/tables/Tables/TableProxy.h>

using ::casacore::TableProxy;

namespace arcae {
namespace detail {

bool
IsolatedTableProxy::IsClosed() const {
  return is_closed_;
}

arrow::Result<bool>
IsolatedTableProxy::Close() {
  if(!is_closed_) {
    std::shared_ptr<void> defer_close(nullptr, [this](...) { this->is_closed_ = true; });
    return run_isolated([](TableProxy & tp) {
      tp.close();
      return true;
    });
  }
  return false;
}

IsolatedTableProxy::~IsolatedTableProxy() {
  auto result = Close();
  if(!result.ok()) {
      ARROW_LOG(WARNING) << "Error closing file " << result.status();
  }
}

}  // namespace detail
}  // namespace arcae