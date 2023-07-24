#include <string>

#include <casacore/tables/Tables/TableDesc.h>
#include <casacore/tables/Tables/TableProxy.h>

namespace arcae {

std::string ms_descriptor(const std::string & table, bool complete=false);

casacore::TableProxy default_ms(const std::string & name,
                                const std::string & json_table_desc="{}",
                                const std::string & json_dminfo="{}");
casacore::TableProxy default_ms_subtable(const std::string & subtable,
                                         std::string name,
                                         const std::string & json_table_desc="{}",
                                         const std::string & json_dminfo="{}");


} // namespace arcae