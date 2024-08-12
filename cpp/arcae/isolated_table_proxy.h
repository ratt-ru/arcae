#ifndef ARCAE_ISOLATED_TABLE_PROXY_H
#define ARCAE_ISOLATED_TABLE_PROXY_H

#include <memory>
#include <type_traits>

#include <casacore/casa/Exceptions/Error.h>
#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/TableProxy.h>

#include <arrow/util/future.h>
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

  // Runs function with signature
  // ReturnType Function(const TableProxy &) on the isolation thread
  // returning an arrow::Future<ReturnType>
  template <
    typename Fn,
    typename = std::enable_if_t<
                std::is_invocable_v<
                  Fn, const casacore::TableProxy &>>>
  ArrowFutureType<Fn, const casacore::TableProxy &>
  RunAsync(Fn && functor) const {
    using ResultType = ArrowResultType<Fn, const casacore::TableProxy &>;
    if(IsClosed()) return arrow::Status::Invalid("Table is closed");
    return RunInPool(
      [this, functor = std::forward<Fn>(functor)]() mutable -> ResultType {
        try {
          return std::invoke(std::forward<Fn>(functor), *this->table_proxy_);
        } catch(casacore::AipsError & e) {
          return arrow::Status::Invalid("Unhandled casacore exception: ", e.what());
        }
      });
  }

  // Runs functions with signature
  // ReturnType Function(TableProxy &) on the isolation thread
  // returning an arrow::Future<ReturnType>
  template <
    typename Fn,
    typename = std::enable_if_t<
                 std::is_invocable_v<
                   Fn, casacore::TableProxy &>>>
  ArrowFutureType<Fn, casacore::TableProxy &>
  RunAsync(Fn && functor) {
    using ResultType = ArrowFutureType<Fn, casacore::TableProxy &>;
    if(IsClosed()) return arrow::Status::Invalid("Table is closed");
    return RunInPool(
      [this, functor = std::forward<Fn>(functor)]() mutable -> ResultType {
        try {
          return std::invoke(std::forward<Fn>(functor), *this->table_proxy_);
        } catch(casacore::AipsError & e) {
          return arrow::Status::Invalid("Unhandled casacore exception: ", e.what());
        }
      });
  }

  template <
    typename Fn,
    typename R,
    typename = std::enable_if_t<
                std::is_invocable_v<Fn, const R &, const casacore::TableProxy &>>>
  ArrowFutureType<Fn, const R &, const casacore::TableProxy &>
  Then(arrow::Future<R> & future, Fn && functor) const {
    using ResultType = ArrowFutureType<Fn, const R &, const casacore::TableProxy &>;
    if(IsClosed()) return arrow::Status::Invalid("Table is closed");
    return future.Then([this, fn = std::move(functor)](const R & result) mutable -> ResultType {
      try {
        return std::invoke(std::forward<Fn>(fn), result, *this->table_proxy_);
      } catch(casacore::AipsError & e) {
        return arrow::Status::Invalid("Unhandled casacore exception: ", e.what());
      }
    }, {}, arrow::CallbackOptions{arrow::ShouldSchedule::Always, io_pool_.get()});
  }

  template <
    typename Fn,
    typename R,
    typename = std::enable_if_t<
                std::is_invocable_v<Fn, const R &, casacore::TableProxy &>>>
  ArrowFutureType<Fn, const R &, casacore::TableProxy &>
  Then(arrow::Future<R> & future, Fn && functor) {
    using ResultType = ArrowFutureType<Fn, const R &, casacore::TableProxy &>;
    if(IsClosed()) return arrow::Status::Invalid("Table is closed");
    return future.Then([this, fn = std::move(functor)](const R & result) mutable -> ResultType {
      try {
        return std::invoke(std::forward<Fn>(fn), result, *this->table_proxy_);
      } catch(casacore::AipsError & e) {
        return arrow::Status::Invalid("Unhandled casacore exception: ", e.what());
      }
    }, {}, arrow::CallbackOptions{arrow::ShouldSchedule::Always, io_pool_.get()});
  }


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
  RunSync(Fn && functor) const {
    using ResultType = ArrowFutureType<Fn, const casacore::TableProxy &>;
    if(IsClosed()) return arrow::Status::Invalid("Table is closed");
    return RunInPoolSync(
      [this, functor = std::forward<Fn>(functor)]() mutable -> ResultType {
        try {
          return std::invoke(std::forward<Fn>(functor), *this->table_proxy_);
        } catch(casacore::AipsError & e) {
          return arrow::Status::Invalid("Unhandled casacore exception: ", e.what());
        }
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
  RunSync(Fn && functor) {
    using ResultType = ArrowResultType<Fn, casacore::TableProxy &>;
    if(IsClosed()) return arrow::Status::Invalid("Table is closed");
    return RunInPoolSync(
      [this, functor = std::forward<Fn>(functor)]() mutable -> ResultType {
        try {
          return std::invoke(std::forward<Fn>(functor), *this->table_proxy_);
        } catch(casacore::AipsError & e) {
          return arrow::Status::Invalid("Unhandled casacore exception: ", e.what());
        }
      });
  }

  // Construct an IsolatedTableProxy with the supplied function
  template <
    typename Fn,
    typename = std::enable_if<
                  std::is_same_v<
                    ArrowResultType<Fn>,
                    arrow::Result<std::shared_ptr<casacore::TableProxy>>>>>
  static arrow::Result<std::shared_ptr<IsolatedTableProxy>> Make(
      Fn && functor,
      std::shared_ptr<arrow::internal::ThreadPool> io_pool=nullptr) {
    struct enable_make_shared_itp : public IsolatedTableProxy {};
    std::shared_ptr<IsolatedTableProxy> proxy = std::make_shared<enable_make_shared_itp>();

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
    ARROW_ASSIGN_OR_RAISE(proxy->table_proxy_, proxy->RunInPoolSync(std::move(functor)));
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
  // and wait for the future's result
  template <typename Fn>
  ArrowResultType<Fn>
  RunInPoolSync(Fn && functor) const {
    return arrow::DeferNotOk(io_pool_->Submit(std::forward<Fn>(functor))).MoveResult();
  }

  // Run the given functor in the I/O pool
  // and wait for the future's result
  template <typename Fn>
  ArrowResultType<Fn>
  RunInPoolWait(Fn && functor) {
    return arrow::DeferNotOk(io_pool_->Submit(std::forward<Fn>(functor))).MoveResult();
  }

  // Run the given functor in the I/O pool
  // returning an future to the result
  template <typename Fn>
  ArrowFutureType<Fn>
  RunInPool(Fn && functor) const {
    return arrow::DeferNotOk(io_pool_->Submit(std::forward<Fn>(functor)));
  }

  // Run the given functor in the I/O pool
  // returning an future to the result
  template <typename Fn>
  ArrowFutureType<Fn>
  RunInPool(Fn && functor) {
    return arrow::DeferNotOk(io_pool_->Submit(std::forward<Fn>(functor)));
  }

private:
  std::shared_ptr<casacore::TableProxy> table_proxy_;
  std::shared_ptr<arrow::internal::ThreadPool> io_pool_;
  bool is_closed_;
};

}  // namespace detail
}  // namespace arcae


#endif // ARCAE_ISOLATED_TABLE_PROXY_H
