#include "safe_table_proxy.h"
#include "table_factory.h"

#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/TableProxy.h>
#include <casacore/tables/Tables/TableLock.h>

using ::arrow::Result;
using ::arrow::Status;

using ::casacore::TableProxy;
using ::casacore::TableLock;
using ::casacore::Table;
using ::casacore::Record;

namespace arcae {

Result<std::shared_ptr<SafeTableProxy>> open_table(const std::string & filename) {
    return SafeTableProxy::Make([&filename]() -> Result<std::shared_ptr<TableProxy>> {
        Record record;
        TableLock lock(TableLock::LockOption::AutoNoReadLocking);

        record.define("option", "usernoread");
        record.define("internal", lock.interval());
        record.define("maxwait", casacore::Int(lock.maxWait()));

        try {
            return std::make_shared<TableProxy>(
                filename, record, Table::TableOption::Old);
        } catch(std::exception & e) {
            return Status::Invalid(e.what());
        }
    });
}

} // namespace arcae