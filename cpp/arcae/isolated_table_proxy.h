#ifndef ARCAE_ISOLATED_TABLE_PROXY_H
#define ARCAE_ISOLATED_TABLE_PROXY_H

#include <memory>
#include <type_traits>

#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/TableProxy.h>

#include <arrow/util/thread_pool.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include "arcae/type_traits.h"

namespace arcae {
namespace detail {

// Isolates access to a CASA Table to a single thread
class IsolatedTableProxy {
public:
  // Close the IsolatedTableProxy
  arrow::Result<bool> Close();
  // Is the IsolatedTableProxy closed?
  bool IsClosed() const;
  // Destroy the IsolatedTableProxy, attempting
  // to close the encapsulated TableProxy in the process
  virtual ~IsolatedTableProxy();

  // Runs functions with signature
  // ReturnType Function(const TableProxy &) on the isolation thread
  // If ReturnType is not an arrow::Result, it will be converted
  // to an arrow::Result<ReturnType>
  template <
    typename Fn,
    typename = std::enable_if_t<
                std::is_invocable_v<
                  Fn, const casacore::TableProxy &>>>
  ArrowResultType<Fn, const casacore::TableProxy &>
  run_isolated(Fn && functor) const {
    if(IsClosed()) return arrow::Status::Invalid("Table is closed");
    return run_in_pool([this, functor = std::forward<Fn>(functor)]() mutable {
      return std::invoke(std::forward<Fn>(functor), *this->table_proxy_);
    });
  }

  // Runs functions with signature
  // ReturnType Function(TableProxy &) on the isolation thread
  // If ReturnType is not an arrow::Result, it will be converted
  // to an arrow::Result<ReturnType>
  template <
    typename Fn,
    typename = std::enable_if_t<
                 std::is_invocable_v<
                   Fn, casacore::TableProxy &>>>
  ArrowResultType<Fn, casacore::TableProxy &>
  run_isolated(Fn && functor) {
    if(IsClosed()) return arrow::Status::Invalid("Table is closed");
    return run_in_pool([this, functor = std::forward<Fn>(functor)]() mutable {
      return std::invoke(std::forward<Fn>(functor), *this->table_proxy_);
    });
  }

  // Construct an IsolatedTableProxy with the supplied function
  template <
    typename Fn,
    typename = std::enable_if<
                  std::is_same_v<
                    std::invoke_result_t<Fn>,
                    arrow::Result<std::shared_ptr<IsolatedTableProxy>>>>>
  static arrow::Result<std::shared_ptr<IsolatedTableProxy>> Make(
      Fn && functor,
      std::shared_ptr<arrow::internal::ThreadPool> io_pool=nullptr) {
    struct enable_make_shared_itp : public IsolatedTableProxy {};
    auto proxy = std::make_shared<enable_make_shared_itp>();

    // Set up the io_pool
    if(io_pool) {
        if(io_pool->GetCapacity() != 1) {
          return arrow::Status::Invalid(
            "Number of threads in "
            "supplied thread pool != 1");
        }
    } else {
        ARROW_ASSIGN_OR_RAISE(io_pool, ::arrow::internal::ThreadPool::Make(1));
    }

    // Mark as closed so that if construction fails, we don't try to close it
    proxy->io_pool_ = io_pool;
    proxy->is_closed_ = true;
    ARROW_ASSIGN_OR_RAISE(proxy->table_proxy_, proxy->run_in_pool(std::move(functor)));
    proxy->is_closed_ = false;

    return proxy;
  }

protected:
  IsolatedTableProxy() = default;
  IsolatedTableProxy(const IsolatedTableProxy & rhs) = delete;
  IsolatedTableProxy(IsolatedTableProxy && rhs) = delete;
  IsolatedTableProxy& operator=(const IsolatedTableProxy & rhs) = delete;
  IsolatedTableProxy& operator=(IsolatedTableProxy && rhs) = delete;

  // Run the given functor in the I/O pool
  template <typename Fn>
  ArrowResultType<Fn>
  run_in_pool(Fn && functor) const {
    return arrow::DeferNotOk(io_pool_->Submit(std::forward<Fn>(functor))).MoveResult();
  }

  // Run the given functor in the I/O pool
  template <typename Fn>
  ArrowResultType<Fn>
  run_in_pool(Fn && functor) {
    return arrow::DeferNotOk(io_pool_->Submit(std::forward<Fn>(functor))).MoveResult();
  }

private:
  std::shared_ptr<casacore::TableProxy> table_proxy_;
  std::shared_ptr<arrow::internal::ThreadPool> io_pool_;
  bool is_closed_;
};

}  // namespace detail
}  // namespace arcae


#endif // ARCAE_ISOLATED_TABLE_PROXY_H
