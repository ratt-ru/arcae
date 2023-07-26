#include <string>

#include <arrow/result.h>

#include <casacore/tables/Tables/SetupNewTab.h>

namespace arcae {

std::string ms_descriptor(const std::string & table, bool complete=false);

arrow::Result<casacore::SetupNewTable> default_ms_factory(
    const std::string & name,
    const std::string & subtable,
    const std::string & json_table_desc="{}",
    const std::string & json_dminfo="{}");

} // namespace arcae