#ifndef ARCAE_SAFE_TABLE_PROXY_H
#define ARCAE_SAFE_TABLE_PROXY_H

#include <climits>

#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/TableProxy.h>

#include <arrow/util/thread_pool.h>

#include "column_convert_visitor.h"

using ::casacore::TableProxy;
using ::arrow::Future;
using ::arrow::Status;
using ::arrow::Result;
using ::arrow::internal::ThreadPool;

namespace arcae {

static constexpr char ARCAE_METADATA[]  = "__arcae_metadata__";
static constexpr char CASA_DESCRIPTOR[]  = "__casa_descriptor__";

/// @class SafeTableProxy
/// @brief Constrains Table access to an arrow::ThreadPool containing a single thread.
class SafeTableProxy {
private:
    Future<std::shared_ptr<TableProxy>> table_future;
    std::shared_ptr<ThreadPool> io_pool;
    bool is_closed;

private:
    inline Status FailIfClosed() const;

protected:
    SafeTableProxy() {};

public:
    virtual ~SafeTableProxy() {
        auto result = close();
        if(!result.ok()) {
            ARROW_LOG(WARNING) << "Error closing file " << result.status();
        }
    };

    static Result<std::shared_ptr<SafeTableProxy>> Make(const casacore::String & filename);
    Result<std::shared_ptr<arrow::Table>> to_arrow(
        casacore::uInt startrow=0,
        casacore::uInt nrow=UINT_MAX,
        const std::vector<std::string> & columns = {}) const;
    Result<std::vector<std::string>> columns() const;
    Result<casacore::uInt> ncolumns() const;
    Result<casacore::uInt> nrow() const;
    Result<std::vector<std::shared_ptr<SafeTableProxy>>> partition(
        const std::vector<std::string> & partition_columns={},
        const std::vector<std::string> & sort_columns={}) const;

    Result<bool> close();
};

} // namespace arcae

#endif
