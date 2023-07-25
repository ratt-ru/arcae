#include "descriptor.h"
#include "safe_table_proxy.h"
#include "table_factory.h"

#include <arrow/api.h>

#include <casacore/tables/Tables/SetupNewTab.h>
#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/TableProxy.h>
#include <casacore/tables/Tables/TableLock.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>

using ::arrow::Result;
using ::arrow::Status;

using ::casacore::String;

using ::casacore::SetupNewTable;
using ::casacore::TableProxy;
using ::casacore::TableLock;
using ::casacore::Table;
using ::casacore::Record;

using ::casacore::MeasurementSet;
using ::casacore::MSAntenna;
using ::casacore::MSDataDescription;
using ::casacore::MSDoppler;
using ::casacore::MSFeed;
using ::casacore::MSField;
using ::casacore::MSFlagCmd;
using ::casacore::MSFreqOffset;
using ::casacore::MSHistory;
using ::casacore::MSObservation;
using ::casacore::MSPointing;
using ::casacore::MSPolarization;
using ::casacore::MSProcessor;
using ::casacore::MSSource;
using ::casacore::MSSpectralWindow;
using ::casacore::MSState;
using ::casacore::MSSysCal;
using ::casacore::MSWeather;


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

template <typename SubTable>

Result<std::shared_ptr<SafeTableProxy>> default_ms_subtable(
                    const std::string & subtable,
                    const std::string & name,
                    const std::string & json_table_desc,
                    const std::string & json_dminfo)
{
    String table_ = subtable;
    table_.upcase();

    std::string modname = name.empty() || name == "MAIN" ?
                          "MeasurementSet.ms" : name;

    ARROW_ASSIGN_OR_RAISE(auto setup_new_table,
                          default_ms_factory(modname, subtable,
                                            json_table_desc, json_dminfo));

    return SafeTableProxy::Make([&]() -> Result<std::shared_ptr<TableProxy>> {
        if(table_.empty() || subtable == "MAIN") {
            return std::make_shared<TableProxy>(MeasurementSet(setup_new_table));
        } else if(table_ == "ANTENNA") {
            return std::make_shared<TableProxy>(MSAntenna(setup_new_table));
        } else if(table_ == "DATA_DESCRIPTION") {
            return std::make_shared<TableProxy>(MSDataDescription(setup_new_table));
        } else if(table_ == "DOPPLER") {
            return std::make_shared<TableProxy>(MSDoppler(setup_new_table));
        } else if(table_ == "FEED") {
            return std::make_shared<TableProxy>(MSFeed(setup_new_table));
        } else if(table_ == "FIELD") {
            return std::make_shared<TableProxy>(MSField(setup_new_table));
        } else if(table_ == "FLAG_CMD") {
            return std::make_shared<TableProxy>(MSFlagCmd(setup_new_table));
        } else if(table_ == "FREQ_OFFSET") {
            return std::make_shared<TableProxy>(MSFreqOffset(setup_new_table));
        } else if(table_ == "HISTORY") {
            return std::make_shared<TableProxy>(MSHistory(setup_new_table));
        } else if(table_ == "OBSERVATION") {
            return std::make_shared<TableProxy>(MSObservation(setup_new_table));
        } else if(table_ == "POINTING") {
            return std::make_shared<TableProxy>(MSPointing(setup_new_table));
        } else if(table_ == "POLARIZATION") {
            return std::make_shared<TableProxy>(MSPolarization(setup_new_table));
        } else if(table_ == "PROCESSOR") {
            return std::make_shared<TableProxy>(MSProcessor(setup_new_table));
        } else if(table_ == "SOURCE") {
            return std::make_shared<TableProxy>(MSSource(setup_new_table));
        } else if(table_ == "SPECTRAL_WINDOW") {
            return std::make_shared<TableProxy>(MSSpectralWindow(setup_new_table));
        } else if(table_ == "STATE") {
            return std::make_shared<TableProxy>(MSState(setup_new_table));
        } else if(table_ == "SYSCAL") {
            return std::make_shared<TableProxy>(MSSysCal(setup_new_table));
        } else if(table_ == "WEATHER") {
            return std::make_shared<TableProxy>(MSWeather(setup_new_table));
        }

        return arrow::Status::Invalid("Uknown table type: ", table_);
    });
}

Result<std::shared_ptr<SafeTableProxy>> default_ms(
                    const std::string & name,
                    const std::string & json_table_desc,
                    const std::string & json_dminfo)
{
    // Create the main Measurement Set
    ARROW_ASSIGN_OR_RAISE(auto setup_new_table,
                          default_ms_factory(name, "MAIN",
                                             json_table_desc, json_dminfo));

    return SafeTableProxy::Make([&]() -> Result<std::shared_ptr<TableProxy>> {
        MeasurementSet ms(setup_new_table);

        // Create the MS default subtables
        ms.createDefaultSubtables(Table::New);

        // Create a table proxy
        return std::make_shared<TableProxy>(ms);
    });
}

} // namespace arcae