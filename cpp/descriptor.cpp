#ifndef ARCAE_DESCRIPTOR_H
#define ARCAE_DESCRIPTOR_H

#include <sstream>

#include "descriptor.h"

#include <arrow/result.h>

#include <casacore/casa/Json.h>
#include <casacore/casa/Json/JsonKVMap.h>
#include <casacore/casa/Containers/RecordInterface.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>
#include <casacore/tables/Tables/TableError.h>
#include <casacore/tables/Tables/TableRecord.h>
#include <casacore/tables/Tables/TableDesc.h>
#include <casacore/tables/Tables/TableProxy.h>
#include <casacore/casa/Containers/ValueHolder.h>

using ::arrow::Result;
using ::arrow::Status;

using ::casacore::JsonKVMap;
using ::casacore::JsonParser;
using ::casacore::JsonOut;
using ::casacore::String;
using ::casacore::Vector;

using ::casacore::TableDesc;
using ::casacore::SetupNewTable;
using ::casacore::Record;
using ::casacore::RecordInterface;
using ::casacore::TableProxy;
using ::casacore::TableError;
using ::casacore::Table;

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

template <typename SubTable>
TableDesc ms_subtable_desc(bool complete)
{
    if(!complete) {
        return SubTable::requiredTableDesc();
    }

    using CEnum = typename SubTable::PredefinedColumns;

    // Get required descriptor
    TableDesc td = SubTable::requiredTableDesc();

    // Add remaining columns
    for(int i = CEnum::NUMBER_REQUIRED_COLUMNS + 1;
        i <= CEnum::NUMBER_PREDEFINED_COLUMNS; ++i)
    {
        SubTable::addColumnToDesc(td, static_cast<CEnum>(i));
    }

    // NOTE(sjperkins)
    // Inspection of the casacore code base seems to indicate
    // that there are no optional MS subtable keywords.
    // NUMBER_REQUIRED_KEYWORDS is only defined in the MS
    return td;
}

std::string RecordToJson(const Record & record) {
    std::ostringstream json_oss;
    auto record_json = JsonOut(json_oss);
    record_json.start();

    for (casacore::uInt i=0; i < record.nfields(); ++i) {
        record_json.write(record.name(i), record.asValueHolder(i));
    }

    record_json.end();
    return json_oss.str();
}

Record JsonToRecord(const std::string & json_record) {
    return JsonParser::parse(json_record).toRecord();
}

TableDesc main_ms_desc(bool complete)
{
    // Get required descriptor
    TableDesc td = MeasurementSet::requiredTableDesc();

    if(!complete) {
        // Remove the CATEGORY keyword from the FLAG_CATEGORY column
        // This empty Vector<String> gets converted to a python dictionary as
        // 'FLAG_CATEGORY' : {
        //     ...
        //     keywords': {'CATEGORY' : []},
        //     ...
        // }
        //
        // Due to the missing type information this gets converted
        // into something like Vector<int> when passed to the C++ layer,
        // which results in Table Conformance errors
        // This is an OK solution since the C++ layer always adds this keyword
        // if it is missing from the MS
        // (see addCat())
        td.rwColumnDesc("FLAG_CATEGORY").rwKeywordSet().removeField("CATEGORY");
        return td;
    }

    using CEnum = typename MeasurementSet::PredefinedColumns;
    using KEnum = typename MeasurementSet::PredefinedKeywords;


    // Add remaining columns
    for(int i = CEnum::NUMBER_REQUIRED_COLUMNS + 1;
        i <= CEnum::NUMBER_PREDEFINED_COLUMNS; ++i)
    {
        MeasurementSet::addColumnToDesc(td, static_cast<CEnum>(i));
    }

    // Add remaining keywords
    for(int i = KEnum::NUMBER_REQUIRED_KEYWORDS + 1;
        i <= KEnum::NUMBER_PREDEFINED_KEYWORDS; ++i)
    {
        MeasurementSet::addKeyToDesc(td, static_cast<KEnum>(i));
    }

    return td;
}




// Get the required table descriptions for the given table.
// If "" or "MAIN", the table descriptions for a Measurement Set
// will be supplied, otherwise table should be some valid
// MeasurementSet subtable
TableDesc ms_table_desc(const String & table, bool complete)
{
    String table_ = table;

    // Upper case things to be sure
    table_.upcase();

    if(table.empty() || table_ == "MAIN") {
        return main_ms_desc(complete);
    } else if(table_ == "ANTENNA") {
        return ms_subtable_desc<MSAntenna>(complete);
    } else if(table_ == "DATA_DESCRIPTION") {
        return ms_subtable_desc<MSDataDescription>(complete);
    } else if(table_ == "DOPPLER") {
        return ms_subtable_desc<MSDoppler>(complete);
    } else if(table_ == "FEED") {
        return ms_subtable_desc<MSFeed>(complete);
    } else if(table_ == "FIELD") {
        return ms_subtable_desc<MSField>(complete);
    } else if(table_ == "FLAG_CMD") {
        return ms_subtable_desc<MSFlagCmd>(complete);
    } else if(table_ == "FREQ_OFFSET") {
        return ms_subtable_desc<MSFreqOffset>(complete);
    } else if(table_ == "HISTORY") {
        return ms_subtable_desc<MSHistory>(complete);
    } else if(table_ == "OBSERVATION") {
        return ms_subtable_desc<MSObservation>(complete);
    } else if(table_ == "POINTING") {
        return ms_subtable_desc<MSPointing>(complete);
    } else if(table_ == "POLARIZATION") {
        return ms_subtable_desc<MSPolarization>(complete);
    } else if(table_ == "PROCESSOR") {
        return ms_subtable_desc<MSProcessor>(complete);
    } else if(table_ == "SOURCE") {
        return ms_subtable_desc<MSSource>(complete);
    } else if(table_ == "SPECTRAL_WINDOW") {
        return ms_subtable_desc<MSSpectralWindow>(complete);
    } else if(table_ == "STATE") {
        return ms_subtable_desc<MSState>(complete);
    } else if(table_ == "SYSCAL") {
        return ms_subtable_desc<MSSysCal>(complete);
    } else if(table_ == "WEATHER") {
        return ms_subtable_desc<MSWeather>(complete);
    }

    throw TableError("Unknown table type: " + table_);
}

// Merge required and user supplied Table Descriptions
TableDesc merge_required_and_user_table_descs(const TableDesc & required_td,
                                              const TableDesc & user_td)
{
    TableDesc result = required_td;

    // Overwrite required columns with user columns
    for(casacore::uInt i=0; i < user_td.ncolumn(); ++i) {
        const String & name = user_td[i].name();

        // Remove if present in required
        if(result.isColumn(name)) {
            result.removeColumn(name);
        }

        // Add the column
        result.addColumn(user_td[i]);
    }

    // Overwrite required hypercolumns with user hypercolumns
    // In practice this shouldn't be necessary as requiredTableDesc
    // doesn't define hypercolumns by default...
    Vector<String> user_hc = user_td.hypercolumnNames();

    for(casacore::uInt i=0; i < user_hc.size(); ++i) {
        // Remove if hypercolumn is present
        if(result.isHypercolumn(user_hc[i])) {
        result.removeHypercolumnDesc(user_hc[i]);
        }

        Vector<String> dataColumnNames;
        Vector<String> coordColumnNames;
        Vector<String> idColumnNames;

        // Get the user hypercolumn
        casacore::uInt ndims = user_td.hypercolumnDesc(user_hc[i],
            dataColumnNames, coordColumnNames, idColumnNames);
        // Place it in result
        result.defineHypercolumn(user_hc[i], ndims,
            dataColumnNames, coordColumnNames, idColumnNames);
    }

    // Overwrite required keywords with user keywords
    result.rwKeywordSet().merge(user_td.keywordSet(),
        RecordInterface::OverwriteDuplicates);

    return result;
}

} // namespace


// Get the table descriptions for the given table.
// If "" or "MAIN", the table descriptions for a Measurement Set
// will be supplied, otherwise table should be some valid
// MeasurementSet subtable.
// If complete is true, the full descriptor is returned, otherwise
// only the required descriptor is returned
std::string ms_descriptor(const std::string & table, bool complete)
{
    return RecordToJson(TableProxy::getTableDesc(ms_table_desc(table, complete), true));
}



Result<SetupNewTable> default_ms_factory(const std::string & name,
                                         const std::string & subtable,
                                         const std::string & json_table_desc,
                                         const std::string & json_dminfo)
{

    auto table_desc = JsonParser::parse(json_table_desc).toRecord();
    auto dminfo = JsonParser::parse(json_dminfo).toRecord();


    String msg;
    TableDesc user_td;

    // Create Table Description object from extra user table description
    if(!TableProxy::makeTableDesc(table_desc, user_td, msg)) {
        return arrow::Status::Invalid("Failed to create Table Description", msg);
    }

    // Merge required and user table descriptions
    TableDesc final_desc = merge_required_and_user_table_descs(
                                ms_table_desc(subtable, false),
                                user_td);

    // Return SetupNewTable object
    SetupNewTable setup = SetupNewTable(name, final_desc, Table::New);

    // Apply any data manager info
    setup.bindCreate(dminfo);

    return setup;
}

} // namespace arcae


#endif // ARCAE_DESCRIPTOR_H
