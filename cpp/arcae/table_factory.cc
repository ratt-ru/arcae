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

#include "arcae/descriptor.h"
#include "arcae/safe_table_proxy.h"
#include "arcae/table_factory.h"

#include <arrow/api.h>

#include <casacore/tables/Tables/SetupNewTab.h>
#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/TableProxy.h>
#include <casacore/tables/Tables/TableLock.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>

using namespace std::literals;

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
namespace {

/// Table and subtable names
static constexpr char kAntenna[] = "ANTENNA";
static constexpr char kMain[] = "MAIN";
static constexpr char kDataDescription[] = "DATA_DESCRIPTION";
static constexpr char kDoppler[] = "DOPPLER";
static constexpr char kFeed[] = "FEED";
static constexpr char kField[] = "FIELD";
static constexpr char kFlagCmd[] = "FLAG_CMD";
static constexpr char kFreqOffset[] = "FREQ_OFFSET";
static constexpr char kHistory[] = "HISTORY";
static constexpr char kObservation[] = "OBSERVATION";
static constexpr char kPointing[] = "POINTING";
static constexpr char kPolarization[] = "POLARIZATION";
static constexpr char kProcessor[] = "PROCESSOR";
static constexpr char kSource[] = "SOURCE";
static constexpr char kSpectralWindow[] = "SPECTRAL_WINDOW";
static constexpr char kState[] = "STATE";
static constexpr char kSyscal[] = "SYSCAL";
static constexpr char kWeather[] = "WEATHER";

} // namespace

Result<std::shared_ptr<SafeTableProxy>> OpenTable(const std::string & filename) {
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

Result<std::shared_ptr<SafeTableProxy>> DefaultMS(
                    const std::string & name,
                    const std::string & subtable,
                    const std::string & json_table_desc,
                    const std::string & json_dminfo)
{
    // Upper case subtable name
    auto usubtable = std::string(subtable.size(), '0');
    std::transform(std::begin(subtable), std::end(subtable), std::begin(usubtable),
                   [](unsigned char c) { return std::toupper(c); });

    auto modname = name.empty() ? "measurementset.ms"s : name;

    // Subtables are relative to the MS name
    if(usubtable != kMain) {
        modname.append(1, '/');
        modname.append(usubtable);
    }

    ARROW_ASSIGN_OR_RAISE(auto setup_new_table,
                          DefaultMSFactory(modname, usubtable,
                                             json_table_desc, json_dminfo));

    return SafeTableProxy::Make([&]() -> Result<std::shared_ptr<TableProxy>> {
        if(usubtable.empty() || usubtable == kMain) {
            auto ms = MeasurementSet(setup_new_table);
            // Create the MS default subtables
            ms.createDefaultSubtables(Table::New);
            // Create a table proxy
            return std::make_shared<TableProxy>(ms);
        } else if(usubtable == kAntenna) {
            return std::make_shared<TableProxy>(MSAntenna(setup_new_table));
        } else if(usubtable == kDataDescription) {
            return std::make_shared<TableProxy>(MSDataDescription(setup_new_table));
        } else if(usubtable == kDoppler) {
            return std::make_shared<TableProxy>(MSDoppler(setup_new_table));
        } else if(usubtable == kFeed) {
            return std::make_shared<TableProxy>(MSFeed(setup_new_table));
        } else if(usubtable == kField) {
            return std::make_shared<TableProxy>(MSField(setup_new_table));
        } else if(usubtable == kFlagCmd) {
            return std::make_shared<TableProxy>(MSFlagCmd(setup_new_table));
        } else if(usubtable == kFreqOffset) {
            return std::make_shared<TableProxy>(MSFreqOffset(setup_new_table));
        } else if(usubtable == kHistory) {
            return std::make_shared<TableProxy>(MSHistory(setup_new_table));
        } else if(usubtable == kObservation) {
            return std::make_shared<TableProxy>(MSObservation(setup_new_table));
        } else if(usubtable == kPointing) {
            return std::make_shared<TableProxy>(MSPointing(setup_new_table));
        } else if(usubtable == kPolarization) {
            return std::make_shared<TableProxy>(MSPolarization(setup_new_table));
        } else if(usubtable == kProcessor) {
            return std::make_shared<TableProxy>(MSProcessor(setup_new_table));
        } else if(usubtable == kSource) {
            return std::make_shared<TableProxy>(MSSource(setup_new_table));
        } else if(usubtable == kSpectralWindow) {
            return std::make_shared<TableProxy>(MSSpectralWindow(setup_new_table));
        } else if(usubtable == kState) {
            return std::make_shared<TableProxy>(MSState(setup_new_table));
        } else if(usubtable == kSyscal) {
            return std::make_shared<TableProxy>(MSSysCal(setup_new_table));
        } else if(usubtable == kWeather) {
            return std::make_shared<TableProxy>(MSWeather(setup_new_table));
        }

        return arrow::Status::Invalid("Uknown table type: ", usubtable);
    });
}

} // namespace arcae