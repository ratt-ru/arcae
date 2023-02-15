#pragma once

#include <sstream>

#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/TableProxy.h>
#include <casacore/casa/Json.h>

#include <arrow/util/logging.h>  // IWYU pragma: keep
#include <arrow/util/thread_pool.h>

#include "casa_arrow.h"
#include "column_convert_visitor.h"


static const std::string CASA_ARROW_METADATA = "__casa_arrow_metadata__";
static const std::string CASA_DESCRIPTOR = "__casa_descriptor__";


/// @brief Constrains Table access to an arrow::ThreadPool containing a single thread.
class SafeTableProxy {
private:
    arrow::Future<std::shared_ptr<casacore::TableProxy>> table_future;
    std::shared_ptr<arrow::internal::ThreadPool> io_pool;
protected:
    SafeTableProxy() {};

public:
    static arrow::Result<std::shared_ptr<SafeTableProxy>> Make(const casacore::String & filename);
    arrow::Result<std::shared_ptr<arrow::Table>> read_table(casacore::uInt startrow, casacore::uInt nrow) const;
    arrow::Result<std::vector<std::string>> columns() const;
    arrow::Result<casacore::uInt> ncolumns() const;
    arrow::Result<casacore::uInt> nrow() const;
};
