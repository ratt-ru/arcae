#ifndef ARCAE_SAFE_TABLE_PROXY_H
#define ARCAE_SAFE_TABLE_PROXY_H

#include <climits>

#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/TableProxy.h>

#include <arrow/util/thread_pool.h>

#include "column_convert_visitor.h"


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

private:
    inline arrow::Status FailIfClosed() const {
        return is_closed ? arrow::Status::Invalid("Table is closed")
                         : arrow::Status::OK();
    };

protected:
    SafeTableProxy() = default;
    SafeTableProxy(const SafeTableProxy & rhs) = delete;
    SafeTableProxy(SafeTableProxy && rhs) = delete;
    SafeTableProxy& operator=(const SafeTableProxy & rhs) = delete;
    SafeTableProxy& operator=(SafeTableProxy && rhs) = delete;

    /// Run the given functor in the isolated Threadpool
    template <typename Fn>
    std::invoke_result_t<Fn> run_isolated(Fn && functor) {
        return arrow::DeferNotOk(this->io_pool->Submit(std::move(functor))).result();
    }

    /// Run the given functor in the isolated Threadpool
    template <typename Fn>
    std::invoke_result_t<Fn> run_isolated(Fn && functor) const {
        return arrow::DeferNotOk(this->io_pool->Submit(std::move(functor))).result();
    }

public:
    virtual ~SafeTableProxy() {
        auto result = close();
        if(!result.ok()) {
            ARROW_LOG(WARNING) << "Error closing file " << result.status();
        }
    };

    static arrow::Result<std::shared_ptr<SafeTableProxy>> Make(const casacore::String & filename);
    arrow::Result<std::shared_ptr<arrow::Table>> to_arrow(
        casacore::uInt startrow=0,
        casacore::uInt nrow=UINT_MAX,
        const std::vector<std::string> & columns = {}) const;
    arrow::Result<std::vector<std::string>> columns() const;
    arrow::Result<casacore::uInt> ncolumns() const;
    arrow::Result<casacore::uInt> nrow() const;
    arrow::Result<std::vector<std::shared_ptr<SafeTableProxy>>> partition(
        const std::vector<std::string> & partition_columns={},
        const std::vector<std::string> & sort_columns={}) const;

    arrow::Result<bool> close();
};

} // namespace arcae

#endif
