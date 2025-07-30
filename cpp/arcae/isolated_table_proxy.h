#ifndef ARCAE_ISOLATED_TABLE_PROXY_H
#define ARCAE_ISOLATED_TABLE_PROXY_H

#include <memory>
#include <type_traits>
#include <vector>

#include <casacore/casa/Exceptions/Error.h>
#include <casacore/casa/IO/FileLocker.h>
#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/TableProxy.h>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/util/future.h>
#include <arrow/util/logging.h>
#include <arrow/util/thread_pool.h>

#include "arcae/finally.h"
#include "arcae/type_traits.h"

namespace arcae {
namespace detail {

using CasaLockType = casacore::FileLocker::LockType;
using CasaTableProxy = casacore::TableProxy;
using ConstTableProxyRef = const casacore::TableProxy&;
using TableProxyRef = casacore::TableProxy&;

// Isolates access to a CASA Table to a single thread
class IsolatedTableProxy : public std::enable_shared_from_this<IsolatedTableProxy> {
 public:
  // Close the IsolatedTableProxy
  arrow::Result<bool> Close();
  // Is the IsolatedTableProxy closed?
  bool IsClosed() const;
  // Return a failed status code if the table is closed
  arrow::Status CheckClosed() const;
  // Destroy the IsolatedTableProxy, attempting
  // to close the encapsulated TableProxy in the process
  virtual ~IsolatedTableProxy();

  [[nodiscard]] static decltype(auto) MaybeLockAndFinalise(CasaTableProxy* proxy,
                                                           CasaLockType lock_type) {
    std::string_view lock_str = [&]() -> std::string_view {
      if (lock_type == CasaLockType::Write) return "write";
      if (lock_type == CasaLockType::Read) return "read";
      return "none";
    }();

    if (lock_type != CasaLockType::None) {
      ARROW_LOG(INFO) << "Locking a " << lock_str;
      proxy->lock(lock_type == CasaLockType::Write, 3);
    }
    return finally([=]() {
      if (lock_type != CasaLockType::None) {
        ARROW_LOG(INFO) << "Unlocking a " << lock_str;
        ;
        proxy->unlock();
      }
    });
  }

  // Runs function with signature
  // ReturnType Function(const TableProxy &) on the isolation thread
  // returning an arrow::Future<ReturnType>
  template <typename Fn,
            typename = std::enable_if_t<std::is_invocable_v<Fn, ConstTableProxyRef>>>
  ArrowFutureType<Fn, ConstTableProxyRef> RunAsync(
      Fn&& functor, CasaLockType lock_type = CasaLockType::Read) const {
    using ResultType = ArrowResultType<Fn, ConstTableProxyRef>;
    ARROW_RETURN_NOT_OK(CheckClosed());
    auto instance = GetInstance();
    return RunInPool([this, instance = instance, lock_type = lock_type,
                      functor = std::forward<Fn>(functor)]() mutable -> ResultType {
      auto proxy = this->GetProxy(instance);
      [[maybe_unused]] auto unlocker = this->MaybeLockAndFinalise(proxy.get(), lock_type);

      try {
        return std::invoke(functor, *proxy);
      } catch (casacore::AipsError& e) {
        return arrow::Status::Invalid("Unhandled casacore exception: ", e.what());
      }
    });
  }

  // Runs functions with signature
  // ReturnType Function(TableProxy &) on the isolation thread
  // returning an arrow::Future<ReturnType>
  template <typename Fn,
            typename = std::enable_if_t<std::is_invocable_v<Fn, TableProxyRef>>>
  ArrowFutureType<Fn, TableProxyRef> RunAsync(
      Fn&& functor, CasaLockType lock_type = CasaLockType::Read) {
    using ResultType = ArrowFutureType<Fn, TableProxyRef>;
    ARROW_RETURN_NOT_OK(CheckClosed());
    auto instance = GetInstance();
    return RunInPool(instance,
                     [this, instance = instance, lock_type = lock_type,
                      functor = std::forward<Fn>(functor)]() mutable -> ResultType {
                       auto proxy = this->GetProxy(instance);
                       [[maybe_unused]] auto unlocker =
                           this->MaybeLockAndFinalise(proxy.get(), lock_type);

                       try {
                         return std::invoke(functor, *this->GetProxy(instance));
                       } catch (casacore::AipsError& e) {
                         return arrow::Status::Invalid("Unhandled casacore exception: ",
                                                       e.what());
                       }
                     });
  }

  template <
      typename Fn, typename R,
      typename = std::enable_if_t<std::is_invocable_v<Fn, const R&, ConstTableProxyRef>>>
  ArrowFutureType<Fn, const R&, ConstTableProxyRef> Then(
      arrow::Future<R>& future, Fn&& functor,
      CasaLockType lock_type = CasaLockType::Read) const {
    using ResultType = ArrowFutureType<Fn, const R&, ConstTableProxyRef>;
    ARROW_RETURN_NOT_OK(CheckClosed());
    auto instance = GetInstance();
    return future.Then(
        [this, instance = instance, lock_type = lock_type,
         fn = std::forward<Fn>(functor)](const R& result) mutable -> ResultType {
          auto proxy = this->GetProxy(instance);
          [[maybe_unused]] auto unlocker =
              this->MaybeLockAndFinalise(proxy.get(), lock_type);
          try {
            return std::invoke(fn, result, *proxy);
          } catch (casacore::AipsError& e) {
            return arrow::Status::Invalid("Unhandled casacore exception: ", e.what());
          }
        },
        {},
        arrow::CallbackOptions{arrow::ShouldSchedule::Always,
                               this->GetPool(instance).get()});
  }

  template <typename Fn, typename R,
            typename = std::enable_if_t<std::is_invocable_v<Fn, const R&, TableProxyRef>>>
  ArrowFutureType<Fn, const R&, TableProxyRef> Then(
      arrow::Future<R>& future, Fn&& functor,
      CasaLockType lock_type = CasaLockType::Read) {
    using ResultType = ArrowFutureType<Fn, const R&, TableProxyRef>;
    ARROW_RETURN_NOT_OK(CheckClosed());
    auto instance = GetInstance();
    return future.Then(
        [this, instance = instance, lock_type = lock_type,
         fn = std::forward<Fn>(functor)](const R& result) mutable -> ResultType {
          auto proxy = this->GetProxy(instance);
          [[maybe_unused]] auto unlocker =
              this->MaybeLockAndFinalise(proxy.get(), lock_type);

          try {
            return std::invoke(fn, result, *proxy);
          } catch (casacore::AipsError& e) {
            return arrow::Status::Invalid("Unhandled casacore exception: ", e.what());
          }
        },
        {},
        arrow::CallbackOptions{arrow::ShouldSchedule::Always,
                               this->GetPool(instance).get()});
  }

  // Runs functions with signature
  // ReturnType Function(const TableProxy &) on the isolation thread
  // If ReturnType is not an arrow::Result, it will be converted
  // to an arrow::Result<ReturnType>
  template <typename Fn,
            typename = std::enable_if_t<std::is_invocable_v<Fn, ConstTableProxyRef>>>
  ArrowResultType<Fn, ConstTableProxyRef> RunSync(
      Fn&& functor, CasaLockType lock_type = CasaLockType::Read) const {
    using ResultType = ArrowFutureType<Fn, ConstTableProxyRef>;
    ARROW_RETURN_NOT_OK(CheckClosed());
    auto instance = GetInstance();
    return RunInPoolSync([this, instance = instance,
                          functor = std::forward<Fn>(functor)]() mutable -> ResultType {
      try {
        return std::invoke(functor, *this->GetProxy(instance));
      } catch (casacore::AipsError& e) {
        return arrow::Status::Invalid("Unhandled casacore exception: ", e.what());
      }
    });
  }

  // Runs functions with signature
  // ReturnType Function(TableProxy &) on the isolation thread
  // If ReturnType is not an arrow::Result, it will be converted
  // to an arrow::Result<ReturnType>
  template <typename Fn,
            typename = std::enable_if_t<std::is_invocable_v<Fn, TableProxyRef>>>
  ArrowResultType<Fn, TableProxyRef> RunSync(
      Fn&& functor, CasaLockType lock_type = CasaLockType::Read) {
    using ResultType = ArrowResultType<Fn, TableProxyRef>;
    ARROW_RETURN_NOT_OK(CheckClosed());
    auto instance = GetInstance();
    return RunInPoolSync(instance,
                         [this, instance = instance,
                          functor = std::forward<Fn>(functor)]() mutable -> ResultType {
                           try {
                             return std::invoke(functor, *this->GetProxy(instance));
                           } catch (casacore::AipsError& e) {
                             return arrow::Status::Invalid(
                                 "Unhandled casacore exception: ", e.what());
                           }
                         });
  }

  // Construct an IsolatedTableProxy with the supplied function
  template <typename Fn,
            typename = std::enable_if<std::is_same_v<
                ArrowResultType<Fn>, arrow::Result<std::shared_ptr<CasaTableProxy>>>>>
  static arrow::Result<std::shared_ptr<IsolatedTableProxy>> Make(
      Fn&& functor, std::size_t ninstances = 1) {
    if (ninstances < 1) {
      return arrow::Status::Invalid("Number of instances must at least be 1");
    }

    struct enable_make_shared_itp : public IsolatedTableProxy {};
    std::shared_ptr<IsolatedTableProxy> proxy =
        std::make_shared<enable_make_shared_itp>();
    proxy->proxy_pools_.reserve(ninstances);
    auto fwd_functor = std::forward<Fn>(functor);

    // Mark as closed so that if construction fails, we don't try to close it
    proxy->is_closed_ = true;

    // Create ninstances I/O pools
    for (std::size_t i = 0; i < ninstances; ++i) {
      ARROW_ASSIGN_OR_RAISE(auto io_pool, ::arrow::internal::ThreadPool::Make(1));
      auto table_fut = arrow::DeferNotOk(io_pool->Submit(fwd_functor));
      ARROW_ASSIGN_OR_RAISE(auto table_proxy, table_fut.MoveResult());
      proxy->proxy_pools_.push_back(
          ProxyAndPool{std::move(table_proxy), std::move(io_pool)});
    }

    proxy->is_closed_ = false;
    return proxy;
  }

  // Construct a new IsolatedTableProxy with the supplied function
  // which is dependent on this IsolatedTableProxy.
  // This generally exists to create Reference Tables through
  // for e.g. Taql queries
  template <typename Fn,
            typename = std::enable_if<
                std::is_invocable_v<Fn, ConstTableProxyRef> &&
                std::is_same_v<ArrowResultType<Fn, ConstTableProxyRef>,
                               arrow::Result<std::shared_ptr<CasaTableProxy>>>>>
  arrow::Result<std::shared_ptr<IsolatedTableProxy>> Spawn(Fn&& functor) {
    struct enable_make_shared_itp : public IsolatedTableProxy {};
    std::shared_ptr<IsolatedTableProxy> itp = std::make_shared<enable_make_shared_itp>();
    using ResultType = arrow::Result<std::shared_ptr<CasaTableProxy>>;

    // Mark as closed so that if construction fails, we don't try to close it
    itp->is_closed_ = true;
    auto fwd_functor = std::forward<Fn>(functor);

    for (std::size_t i = 0; i < proxy_pools_.size(); ++i) {
      auto future = arrow::DeferNotOk(
          GetPool(i)->Submit([this, i = i, fn = fwd_functor]() -> ResultType {
            return std::invoke(fn, *GetProxy(i));
          }));

      ARROW_ASSIGN_OR_RAISE(auto table_proxy, future.MoveResult());
      itp->proxy_pools_.emplace_back(ProxyAndPool{std::move(table_proxy), GetPool(i)});
    }

    itp->is_closed_ = false;
    // Add an explicit dependency on the ITP
    itp->dependencies_.push_back(shared_from_this());
    return itp;
  }

  // Spawns an IsolatedTableProxy encapsulating a single instance
  // from this ITP. Suitable for constraining writes to a single
  // thread and instance as concurrent writes issued from multiple
  // threads will produce race conditions in the underlying casacore layer
  std::shared_ptr<IsolatedTableProxy> SpawnWriter();

 protected:
  IsolatedTableProxy() = default;
  IsolatedTableProxy(const IsolatedTableProxy& rhs) = delete;
  IsolatedTableProxy(IsolatedTableProxy&& rhs) = delete;
  IsolatedTableProxy& operator=(const IsolatedTableProxy& rhs) = delete;
  IsolatedTableProxy& operator=(IsolatedTableProxy&& rhs) = delete;

  // Gets the least active instance
  std::size_t GetInstance() const;

  // Get the Table Proxy for the given instance
  const std::shared_ptr<casacore::TableProxy>& GetProxy(std::size_t instance) const;

  // Get the I/O pool for the given instance
  const std::shared_ptr<arrow::internal::ThreadPool>& GetPool(std::size_t instance) const;

  // Run the given functor in the I/O pool
  // and wait for the future's result
  template <typename Fn>
  ArrowResultType<Fn> RunInPoolSync(std::size_t instance, Fn&& functor) const {
    return RunInPool(instance, std::forward<Fn>(functor)).MoveResult();
  }

  // Run the given functor in the I/O pool
  // and wait for the future's result
  template <typename Fn>
  ArrowResultType<Fn> RunInPoolWait(std::size_t instance, Fn&& functor) {
    return RunInPool(instance, std::forward<Fn>(functor)).MoveResult();
  }

  // Run the given functor in the I/O pool
  // returning an future to the result
  template <typename Fn>
  ArrowFutureType<Fn> RunInPool(std::size_t instance, Fn&& functor) const {
    const auto& pool = proxy_pools_[instance].io_pool_;
    return arrow::DeferNotOk(pool->Submit(std::forward<Fn>(functor)));
  }

  // Run the given functor in the I/O pool
  // returning a future to the result
  template <typename Fn>
  ArrowFutureType<Fn> RunInPool(std::size_t instance, Fn&& functor) {
    const auto& pool = proxy_pools_[instance].io_pool_;
    return arrow::DeferNotOk(pool->Submit(std::forward<Fn>(functor)));
  }

 private:
  struct ProxyAndPool {
    std::shared_ptr<casacore::TableProxy> table_proxy_;
    std::shared_ptr<arrow::internal::ThreadPool> io_pool_;
  };

  std::vector<ProxyAndPool> proxy_pools_;
  bool is_closed_;
  std::vector<std::shared_ptr<IsolatedTableProxy>> dependencies_;
};

}  // namespace detail
}  // namespace arcae

#endif  // ARCAE_ISOLATED_TABLE_PROXY_H
