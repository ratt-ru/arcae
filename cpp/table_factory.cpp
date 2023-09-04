/// Parts of this file were originally contributed to https://github.com/casacore/python-casacore
/// under a LGPLv3 license by https://github.com/sjperkins in the following PRs:
/// - https://github.com/casacore/python-casacore/pull/72
/// - https://github.com/casacore/python-casacore/pull/183
/// More generally https://github.com/sjperkins made all contributions
/// to pyms.cc up until python-casacore 3.5.2
/// - https://github.com/casacore/python-casacore/commits/v3.5.2/src/pyms.cc
/// Parts of this code are relicensed here under the BSD-3 license
/// available at https://github.com/ratt-ru/arcae/blob/main/LICENSE

#include <algorithm>

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

Result<std::shared_ptr<SafeTableProxy>> default_ms(
                    const std::string & name,
                    const std::string & subtable,
                    const std::string & json_table_desc,
                    const std::string & json_dminfo)
{
    // Upper case subtable name
    std::string usubtable(subtable.size(), '0');
    std::transform(std::begin(subtable), std::end(subtable), std::begin(usubtable),
                   [](unsigned char c) { return std::toupper(c); });

    std::string modname = name.empty() ? "measurementset.ms" : name;

    // Subtables are relative to the MS name
    if(usubtable != "MAIN") {
        modname.append(1, '/');
        modname.append(usubtable);
    }

    ARROW_ASSIGN_OR_RAISE(auto setup_new_table,
                          default_ms_factory(modname, usubtable,
                                             json_table_desc, json_dminfo));

    return SafeTableProxy::Make([&]() -> Result<std::shared_ptr<TableProxy>> {
        if(usubtable.empty() || usubtable == "MAIN") {
            auto ms = MeasurementSet(setup_new_table);
            // Create the MS default subtables
            ms.createDefaultSubtables(Table::New);
            // Create a table proxy
            return std::make_shared<TableProxy>(ms);
        } else if(usubtable == "ANTENNA") {
            return std::make_shared<TableProxy>(MSAntenna(setup_new_table));
        } else if(usubtable == "DATA_DESCRIPTION") {
            return std::make_shared<TableProxy>(MSDataDescription(setup_new_table));
        } else if(usubtable == "DOPPLER") {
            return std::make_shared<TableProxy>(MSDoppler(setup_new_table));
        } else if(usubtable == "FEED") {
            return std::make_shared<TableProxy>(MSFeed(setup_new_table));
        } else if(usubtable == "FIELD") {
            return std::make_shared<TableProxy>(MSField(setup_new_table));
        } else if(usubtable == "FLAG_CMD") {
            return std::make_shared<TableProxy>(MSFlagCmd(setup_new_table));
        } else if(usubtable == "FREQ_OFFSET") {
            return std::make_shared<TableProxy>(MSFreqOffset(setup_new_table));
        } else if(usubtable == "HISTORY") {
            return std::make_shared<TableProxy>(MSHistory(setup_new_table));
        } else if(usubtable == "OBSERVATION") {
            return std::make_shared<TableProxy>(MSObservation(setup_new_table));
        } else if(usubtable == "POINTING") {
            return std::make_shared<TableProxy>(MSPointing(setup_new_table));
        } else if(usubtable == "POLARIZATION") {
            return std::make_shared<TableProxy>(MSPolarization(setup_new_table));
        } else if(usubtable == "PROCESSOR") {
            return std::make_shared<TableProxy>(MSProcessor(setup_new_table));
        } else if(usubtable == "SOURCE") {
            return std::make_shared<TableProxy>(MSSource(setup_new_table));
        } else if(usubtable == "SPECTRAL_WINDOW") {
            return std::make_shared<TableProxy>(MSSpectralWindow(setup_new_table));
        } else if(usubtable == "STATE") {
            return std::make_shared<TableProxy>(MSState(setup_new_table));
        } else if(usubtable == "SYSCAL") {
            return std::make_shared<TableProxy>(MSSysCal(setup_new_table));
        } else if(usubtable == "WEATHER") {
            return std::make_shared<TableProxy>(MSWeather(setup_new_table));
        }

        return arrow::Status::Invalid("Uknown table type: ", usubtable);
    });
}

} // namespace arcae