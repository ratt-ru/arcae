#include "arcae/isolated_table_proxy.h"

#include <cassert>
#include <cstddef>
#include <limits>
#include <memory>

#include <arrow/util/logging.h>
#include "arrow/status.h"
#include "arrow/util/functional.h"
#include "arrow/util/future.h"
#include "arrow/util/thread_pool.h"

#include <casacore/tables/Tables/TableProxy.h>

using ::arrow::Future;
using ::arrow::Result;
using ::arrow::Status;
using ::arrow::internal::ThreadPool;
using ::casacore::TableProxy;

namespace arcae {
namespace detail {

bool IsolatedTableProxy::IsClosed() const { return is_closed_; }

std::size_t IsolatedTableProxy::GetInstance() const {
  using NumTasksType = decltype(ProxyAndPool::io_pool_->GetNumTasks());
  std::size_t instance = 0;
  NumTasksType num_tasks = std::numeric_limits<NumTasksType>::max();
  assert(proxy_pools_.size() > 0);

  for (std::size_t i = 0; i < proxy_pools_.size(); ++i) {
    const auto& pool = proxy_pools_[i].io_pool_;
    const auto pool_tasks = pool->GetNumTasks();
    if (pool_tasks < num_tasks) {
      instance = i;
      num_tasks = pool_tasks;
    }
  }

  return instance;
}

const std::shared_ptr<TableProxy>& IsolatedTableProxy::GetProxy(
    std::size_t instance) const {
  assert(instance < proxy_pools_.size());
  return proxy_pools_[instance].table_proxy_;
}

const std::shared_ptr<ThreadPool>& IsolatedTableProxy::GetPool(
    std::size_t instance) const {
  assert(instance < proxy_pools_.size());
  return proxy_pools_[instance].io_pool_;
}

Status IsolatedTableProxy::CheckClosed() const {
  if (!is_closed_) return Status::OK();
  return Status::Invalid("TableProxy is closed");
}

Result<bool> IsolatedTableProxy::Close() {
  if (!is_closed_) {
    std::shared_ptr<void> defer_close(nullptr, [this](...) { this->is_closed_ = true; });
    std::vector<Future<bool>> results;
    results.reserve(proxy_pools_.size());
    for (auto& [proxy, pool] : proxy_pools_) {
      results.push_back(arrow::DeferNotOk(pool->Submit([tp = proxy]() {
        tp->close();
        return true;
      })));
    }
    auto all_done = arrow::All(results);
    all_done.Wait();
    return true;
  }
  return false;
}

IsolatedTableProxy::~IsolatedTableProxy() {
  auto result = Close();
  if (!result.ok()) {
    ARROW_LOG(WARNING) << "Error closing file " << result.status();
  }
}

}  // namespace detail
}  // namespace arcae
