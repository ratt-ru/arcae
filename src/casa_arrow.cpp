#include <casacore/tables/Tables.h>

#include <arrow/util/logging.h>  // IWYU pragma: keep

#include "casa_arrow.h"
#include "column_convert_visitor.h"

arrow::Result<std::shared_ptr<arrow::Table>> open_table(const std::string & filename) {
    // if(!casacore::Table::isReadable(filename, false)) {
    //     return arrow::Status::Invalid(filename, " is not a valid Table");
    // }

    auto casa_table = casacore::Table(filename);
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

        auto arrow_field = std::make_shared<arrow::Field>(column_names[i], visitor.array->type());
        fields.emplace_back(std::move(arrow_field));
        arrays.emplace_back(std::move(visitor.array));
    }

    auto schema = arrow::schema(fields);
    return arrow::Table::Make(std::move(schema), arrays, -1);
}
