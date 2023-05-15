#ifndef CASA_ARROW_SAFE_TABLE_PROXY_H
#define CASA_ARROW_SAFE_TABLE_PROXY_H

#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/TableProxy.h>

#include <arrow/util/thread_pool.h>

#include "column_convert_visitor.h"

static constexpr char CASA_ARROW_METADATA[]  = "__casa_arrow_metadata__";
static constexpr char CASA_DESCRIPTOR[]  = "__casa_descriptor__";

using ::casacore::TableProxy;
using ::arrow::Future;
using ::arrow::Result;
using ::arrow::internal::ThreadPool;

/// @class SafeTableProxy
/// @brief Constrains Table access to an arrow::ThreadPool containing a single thread.
class SafeTableProxy {
private:
    Future<std::shared_ptr<TableProxy>> table_future;
    std::shared_ptr<ThreadPool> io_pool;
    bool is_closed;

private:
    inline arrow::Status FailIfClosed() const;

protected:
    SafeTableProxy() {};

public:
    virtual ~SafeTableProxy() { close(); };

    static Result<std::shared_ptr<SafeTableProxy>> Make(const casacore::String & filename);
    Result<std::shared_ptr<arrow::Table>> to_arrow() const;
    Result<std::shared_ptr<arrow::Table>> to_arrow(casacore::uInt startrow, casacore::uInt nrow) const;
    Result<std::vector<std::string>> columns() const;
    Result<casacore::uInt> ncolumns() const;
    Result<casacore::uInt> nrow() const;
    Result<std::vector<std::shared_ptr<SafeTableProxy>>> partition(const std::vector<std::string> & columns) const;

    Result<bool> close();
};

#endif
