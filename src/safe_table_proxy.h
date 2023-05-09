#ifndef CASA_ARROW_SAFE_TABLE_PROXY_H
#define CASA_ARROW_SAFE_TABLE_PROXY_H

#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/TableProxy.h>
#include <casacore/casa/Json.h>

#include <arrow/util/logging.h>  // IWYU pragma: keep
#include <arrow/util/thread_pool.h>

#include "column_convert_visitor.h"

static constexpr char CASA_ARROW_METADATA[]  = "__casa_arrow_metadata__";
static constexpr char CASA_DESCRIPTOR[]  = "__casa_descriptor__";

/// @class SafeTableProxy
/// @brief Constrains Table access to an arrow::ThreadPool containing a single thread.
class SafeTableProxy {
private:
    arrow::Future<std::shared_ptr<casacore::TableProxy>> table_future;
    std::shared_ptr<arrow::internal::ThreadPool> io_pool;
    bool is_closed;

private:
    inline arrow::Status FailIfClosed() const;

protected:
    SafeTableProxy() {};

public:
    virtual ~SafeTableProxy();

    static arrow::Result<std::shared_ptr<SafeTableProxy>> Make(const casacore::String & filename);
    arrow::Result<std::shared_ptr<arrow::Table>> to_arrow() const;
    arrow::Result<std::shared_ptr<arrow::Table>> to_arrow(casacore::uInt startrow, casacore::uInt nrow) const;
    arrow::Result<std::vector<std::string>> columns() const;
    arrow::Result<casacore::uInt> ncolumns() const;
    arrow::Result<casacore::uInt> nrow() const;

    arrow::Result<bool> close();
};

#endif
