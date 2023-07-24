#include <string>

#include <casacore/tables/Tables/TableDesc.h>
#include <casacore/tables/Tables/TableProxy.h>

namespace arcae {

std::string ms_descriptor(const std::string & table, bool complete=false);

casacore::TableProxy default_ms(const casacore::String & name,
                                const casacore::Record & table_desc={},
                                const casacore::Record & dminfo={});
casacore::TableProxy default_ms_subtable(const casacore::String & subtable,
                                         casacore::String name,
                                         const casacore::Record & table_desc={},
                                         const casacore::Record & dminfo={});


} // namespace arcae