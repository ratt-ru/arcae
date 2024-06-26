#ifndef ARCAE_SAFE_TABLE_PROXY_H
#define ARCAE_SAFE_TABLE_PROXY_H

// #include <climits>
#include <cstddef>
#include <functional>
#include <type_traits>

#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/TableProxy.h>

#include <arrow/util/logging.h>
#include <arrow/util/thread_pool.h>

#include "arcae/base_column_map.h"
#include "arrow/status.h"

namespace arcae {

static constexpr char ARCAE_METADATA[]  = "__arcae_metadata__";
static constexpr char CASA_DESCRIPTOR[]  = "__casa_descriptor__";

/// @class SafeTableProxy
/// @brief Constrains Table access to an arrow::ThreadPool containing a single thread.
class SafeTableProxy {
private:
    std::shared_ptr<casacore::TableProxy> table_proxy;
    std::shared_ptr<arrow::internal::ThreadPool> io_pool;
    bool is_closed;

    friend arrow::Result<std::shared_ptr<SafeTableProxy>> Taql(
            const std::string & taql,
            const std::vector<std::shared_ptr<SafeTableProxy>> & tables);
protected:
    SafeTableProxy() = default;
    SafeTableProxy(const SafeTableProxy & rhs) = delete;
    SafeTableProxy(SafeTableProxy && rhs) = delete;
    SafeTableProxy& operator=(const SafeTableProxy & rhs) = delete;
    SafeTableProxy& operator=(SafeTableProxy && rhs) = delete;

    /// Run the given functor in the isolated Threadpool
    template <typename Fn>
    std::invoke_result_t<Fn> run_isolated(Fn && functor) {
        return arrow::DeferNotOk(this->io_pool->Submit(std::forward<Fn>(functor))).MoveResult();
    }

    /// Run the given functor in the isolated Threadpool
    template <typename Fn>
    std::invoke_result_t<Fn> run_isolated(Fn && functor) const {
        return arrow::DeferNotOk(this->io_pool->Submit(std::forward<Fn>(functor))).MoveResult();
    }

public:
    virtual ~SafeTableProxy() {
        auto result = Close();
        if(!result.ok()) {
            ARROW_LOG(WARNING) << "Error closing file " << result.status();
        }
    };

    template <typename Fn,
              typename = std::enable_if_t<
                std::is_invocable_v<Fn, const casacore::TableProxy &>>>
    std::invoke_result_t<Fn, const casacore::TableProxy &> run(Fn && functor) const {
        return run_isolated([this, functor = std::forward<Fn>(functor)]() mutable {
            return std::invoke(std::forward<Fn>(functor),
                               static_cast<const casacore::TableProxy &>(*this->table_proxy));
        });
    }

    template <typename Fn,
              typename = std::enable_if_t<
                std::is_invocable_v<Fn, casacore::TableProxy &>>>
    std::invoke_result_t<Fn, casacore::TableProxy &> run(Fn && functor) {
        return run_isolated([this, functor = std::forward<Fn>(functor)]() mutable {
            return std::invoke(std::forward<Fn>(functor),
                               static_cast<casacore::TableProxy &>(*this->table_proxy));
        });
    }


    template <typename Fn>
    static arrow::Result<std::shared_ptr<SafeTableProxy>> Make(
            Fn && functor,
            std::shared_ptr<arrow::internal::ThreadPool> io_pool=nullptr) {
        struct enable_make_shared_stp : public SafeTableProxy {};
        auto proxy = std::make_shared<enable_make_shared_stp>();

        if(io_pool) {
            if(io_pool->GetCapacity() != 1) {
                return arrow::Status::Invalid("Number of threads in supplied thread pool != 1");
            }
            proxy->io_pool = io_pool;
        } else {
            ARROW_ASSIGN_OR_RAISE(proxy->io_pool, ::arrow::internal::ThreadPool::Make(1));
        }


        // Mark as closed so that if construction fails, we don't try to close it
        proxy->is_closed = true;
        ARROW_ASSIGN_OR_RAISE(proxy->table_proxy, proxy->run_isolated(std::move(functor)));
        proxy->is_closed = false;

        return proxy;
    }

    static std::tuple<casacore::uInt, casacore::uInt>
    ClampRows(const casacore::Table & table,
              casacore::uInt startrow,
              casacore::uInt nrow);

    bool IsClosed() const { return is_closed; }

    arrow::Result<std::shared_ptr<arrow::Table>> ToArrow(
        const ColumnSelection & selection={},
        const std::vector<std::string> & columns = {}) const;

    arrow::Result<std::shared_ptr<arrow::Array>> GetColumn(
        const std::string & column,
        const ColumnSelection & selection) const;

    arrow::Result<bool> PutColumn(
        const std::string & column,
        const ColumnSelection & selection,
        const std::shared_ptr<arrow::Array> & data) const;

    arrow::Result<std::string> GetTableDescriptor() const;
    arrow::Result<std::string> GetColumnDescriptor(const std::string & column) const;

    arrow::Result<std::string> GetLockOptions() const;

    arrow::Result<bool> ReopenRW() const;
    arrow::Result<bool> IsWriteable() const;

    arrow::Result<std::vector<std::string>> Columns() const;
    arrow::Result<casacore::uInt> nColumns() const;
    arrow::Result<casacore::uInt> nRow() const;
    arrow::Result<std::vector<std::shared_ptr<SafeTableProxy>>> Partition(
        const std::vector<std::string> & partition_columns={},
        const std::vector<std::string> & sort_columns={}) const;

    arrow::Result<bool> AddRows(casacore::uInt nrows);

    arrow::Result<bool> Close();
};

} // namespace arcae

#endif
