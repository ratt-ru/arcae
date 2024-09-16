#include <string>

#include <arrow/result.h>

#include <casacore/tables/Tables/SetupNewTab.h>

namespace arcae {

arrow::Result<std::string> MSDescriptor(const std::string& table, bool complete = false);

arrow::Result<casacore::SetupNewTable> DefaultMSFactory(
    const std::string& name, const std::string& subtable,
    const std::string& json_table_desc = "{}", const std::string& json_dminfo = "{}");

}  // namespace arcae
