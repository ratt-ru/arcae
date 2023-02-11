#include <sstream>

#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/TableProxy.h>
#include <casacore/casa/Json.h>

#include <arrow/util/logging.h>  // IWYU pragma: keep

#include "casa_arrow.h"
#include "column_convert_visitor.h"


static const std::string CASA_ARROW_METADATA = "__casa_arrow_metadata__";
static const std::string CASA_DESCRIPTOR = "__casa_descriptor__";

arrow::Result<std::shared_ptr<arrow::Table>> open_table(const std::string & filename) {
    if(!casacore::Table::isReadable(filename, false)) {
        return arrow::Status::Invalid(filename, " is not a valid Table");
    }

    std::ostringstream json_oss;

    auto casa_table = casacore::Table(filename);
    auto table_proxy = casacore::TableProxy(casa_table);
    auto table_desc = casa_table.tableDesc();
    auto column_names = table_desc.columnNames();

    auto fields = arrow::FieldVector();
    auto arrays = arrow::ArrayVector();

    for(casacore::uInt i=0; i < table_desc.ncolumn(); ++i) {
        auto table_column = casacore::TableColumn(casa_table, i);
        auto column_desc = table_column.columnDesc();
        auto visitor = ColumnConvertVisitor(table_column);
        auto visit_status = visitor.Visit(column_desc.dataType());

        if(!visit_status.ok()) {
            ARROW_LOG(WARNING)
                << "Ignoring " << column_desc.name() << " " << visit_status;
            continue;
        }

        if(!visitor.array) {
            ARROW_LOG(ERROR)
                << "Ignoring " << column_desc.name()
                << ". Arrow array not created by Visitor";
            continue;
        }


        json_oss.seekp(0);
        casacore::JsonOut column_json(json_oss);

        column_json.start();
        column_json.write(CASA_DESCRIPTOR, table_proxy.recordColumnDesc(column_desc, true));
        column_json.end();

        auto column_metadata = arrow::KeyValueMetadata::Make(
            {CASA_ARROW_METADATA}, {json_oss.str()});
        auto arrow_field = std::make_shared<arrow::Field>(
            column_names[i], visitor.array->type(),
            true, column_metadata);
        fields.emplace_back(std::move(arrow_field));
        arrays.emplace_back(std::move(visitor.array));
    }

    json_oss.seekp(0);
    casacore::JsonOut table_json(json_oss);

    table_json.start();
    table_json.write(CASA_DESCRIPTOR, table_proxy.getTableDescription(true, true));
    table_json.end();

    auto table_metadata = arrow::KeyValueMetadata::Make(
        {CASA_ARROW_METADATA}, {json_oss.str()});

    auto schema = arrow::schema(fields, table_metadata);
    return arrow::Table::Make(std::move(schema), arrays, casa_table.nrow());
}
