#include <memory>
#include <sstream>

#include <arrow/util/logging.h>  // IWYU pragma: keep

#include <casacore/casa/Json.h>
#include <casacore/tables/Tables/TableIterProxy.h>

#include "arcae/safe_table_proxy.h"


using ::arrow::DataType;
using ::arrow::Buffer;
using ::arrow::Future;
using ::arrow::Result;
using ::arrow::Status;

using ::casacore::Record;
using ::casacore::Table;
using ::casacore::TableColumn;
using ::casacore::TableIterProxy;
using ::casacore::TableLock;
using ::casacore::TableProxy;

namespace arcae {
namespace {
// https://stackoverflow.com/a/25069711
struct enable_make_shared_stp : public SafeTableProxy {};
} // namespace


std::tuple<casacore::uInt, casacore::uInt>
SafeTableProxy::ClampRows(const casacore::Table & table, casacore::uInt startrow, casacore::uInt nrow) {
    if(startrow > table.nrow()) {
        return {table.nrow(), 0};
    }

    if(startrow + nrow >= table.nrow()) {
        nrow = std::min(table.nrow(), table.nrow() - startrow);
    }

    return {startrow, nrow};
}

arrow::Result<std::string>
SafeTableProxy::GetTableDescriptor() const {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    return run_isolated([this]() -> arrow::Result<std::string> {
        std::ostringstream json_oss;
        casacore::JsonOut table_json(json_oss);

        table_json.start();
        table_json.write(CASA_DESCRIPTOR, table_proxy->getTableDescription(true, true));
        table_json.end();

        return json_oss.str();
    });
}

arrow::Result<std::string>
SafeTableProxy::GetColumnDescriptor(const std::string & column) const {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    return run_isolated([this, &column]() -> arrow::Result<std::string> {
        const auto & table_desc = this->table_proxy->table().tableDesc();

        if(!table_desc.isColumn(column)) {
            return arrow::Status::UnknownError(column, " does not exist");
        }

        std::ostringstream json_oss;
        casacore::JsonOut column_json(json_oss);

        column_json.start();
        column_json.write(column, table_proxy->getColumnDescription(column, true, true));
        column_json.end();

        return json_oss.str();
    });
}


arrow::Result<std::shared_ptr<arrow::Array>>
SafeTableProxy::GetColumn(const std::string & column, casacore::uInt startrow, casacore::uInt nrow) const {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    return run_isolated([this, &column, startrow, nrow]() -> arrow::Result<std::shared_ptr<arrow::Array>> {
        auto & casa_table = this->table_proxy->table();

        if(!casa_table.tableDesc().isColumn(column)) {
            return arrow::Status::UnknownError(column, " does not exist");
        }

        auto table_column = TableColumn(casa_table, column);
        const auto & column_desc = table_column.columnDesc();
        auto [start_row, n_row] = ClampRows(casa_table, startrow, nrow);
        auto visitor = ColumnConvertVisitor(table_column, start_row, n_row);
        auto visit_status = visitor.Visit(column_desc.dataType());
        ARROW_RETURN_NOT_OK(visit_status);
        return std::move(visitor.array_);
    });
}

arrow::Result<std::shared_ptr<arrow::Table>>
SafeTableProxy::ToArrow(casacore::uInt startrow, casacore::uInt nrow, const std::vector<std::string> & columns) const {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    return run_isolated([this, startrow, nrow, &columns]() -> arrow::Result<std::shared_ptr<arrow::Table>> {
        const auto & casa_table = this->table_proxy->table();
        const auto & table_desc = casa_table.tableDesc();
        auto column_names = columns.size() == 0 ? table_desc.columnNames() : casacore::Vector<casacore::String>(columns);
        auto [start_row, n_row] = ClampRows(casa_table, startrow, nrow);
        auto fields = arrow::FieldVector();
        auto arrays = arrow::ArrayVector();

        for(casacore::uInt i=0; i < column_names.size(); ++i) {
            auto column_name = column_names[i];

            if(!table_desc.isColumn(column_name)) {
                ARROW_LOG(WARNING) << column_name << " is not a valid column";
                continue;
            }

            auto table_column = TableColumn(casa_table, column_name);
            auto column_desc = table_column.columnDesc();
            auto visitor = ColumnConvertVisitor(table_column, start_row, n_row);
            auto visit_status = visitor.Visit(column_desc.dataType());

            if(!visit_status.ok()) {
                ARROW_LOG(WARNING)
                    << "Ignoring " << column_name << " " << visit_status;
                continue;
            }

            if(!visitor.array_) {
                ARROW_LOG(ERROR)
                    << "Ignoring " << column_name
                    << ". Arrow array not created by Visitor";
                continue;
            }


            std::ostringstream json_oss;
            casacore::JsonOut column_json(json_oss);

            column_json.start();
            column_json.write(CASA_DESCRIPTOR, this->table_proxy->recordColumnDesc(column_desc, true));
            column_json.end();

            auto column_metadata = arrow::KeyValueMetadata::Make(
                {ARCAE_METADATA}, {json_oss.str()});
            auto arrow_field = std::make_shared<arrow::Field>(
                column_name, visitor.array_->type(),
                true, std::move(column_metadata));
            fields.emplace_back(std::move(arrow_field));
            arrays.emplace_back(std::move(visitor.array_));
        }

        std::ostringstream json_oss;
        casacore::JsonOut table_json(json_oss);

        table_json.start();
        table_json.write(CASA_DESCRIPTOR, table_proxy->getTableDescription(true, true));
        table_json.end();

        auto table_metadata = arrow::KeyValueMetadata::Make(
            {ARCAE_METADATA}, {json_oss.str()});

        auto schema = arrow::schema(fields, std::move(table_metadata));
        auto table = arrow::Table::Make(std::move(schema), arrays, n_row);
        auto status = table->Validate();

        if(!status.ok()) {
            return status;
        }

        return table;
    });
}


Result<std::vector<std::string>>
SafeTableProxy::Columns() const {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    return run_isolated([this]() -> Result<std::vector<std::string>> {
        const auto& column_names = this->table_proxy->table().tableDesc().columnNames();
        return std::vector<std::string>(column_names.begin(), column_names.end());
    });
}


Result<casacore::uInt>
SafeTableProxy::nColumns() const {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    return run_isolated([this]() -> Result<casacore::uInt> {
        return this->table_proxy->table().tableDesc().ncolumn();
    });
}

Result<casacore::uInt>
SafeTableProxy::nRow() const {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    return run_isolated([this]() -> Result<casacore::uInt> {
        return this->table_proxy->table().nrow();
    });
}

Result<std::vector<std::shared_ptr<SafeTableProxy>>>
SafeTableProxy::Partition(
    const std::vector<std::string> & partition_columns,
    const std::vector<std::string> & sort_columns) const {

    if(partition_columns.size() == 0) {
        return Status::Invalid("No partitioning columns provided");
    }

    ARROW_RETURN_NOT_OK(FailIfClosed());

    return run_isolated([this, &partition_columns, &sort_columns]() -> Result<std::vector<std::shared_ptr<SafeTableProxy>>> {
        casacore::Block<casacore::String> casa_sort_cols(sort_columns.size());
        for(casacore::uInt i=0; i < sort_columns.size(); ++i)
            { casa_sort_cols[i] = sort_columns[i]; }

        auto casa_part_cols = casacore::Vector<casacore::String>(partition_columns);
        std::vector<std::shared_ptr<SafeTableProxy>> result;
        auto partition_proxy = std::make_shared<TableProxy>();
        auto iter = TableIterProxy(*this->table_proxy, casa_part_cols, "a", "q");

        while(iter.nextPart(*partition_proxy)) {
            if(casa_sort_cols.size() > 0) {
                partition_proxy = std::make_shared<TableProxy>(partition_proxy->table().sort(casa_sort_cols));
            }

            auto stp = std::make_shared<enable_make_shared_stp>();
            stp->table_proxy = std::move(partition_proxy);
            stp->io_pool = this->io_pool;
            stp->is_closed = false;
            result.push_back(std::move(stp));
            partition_proxy = std::make_shared<TableProxy>();
        }

        return result;
    });
}


Result<bool>
SafeTableProxy::AddRows(casacore::uInt nrows) {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    return run_isolated([this, nrows]() -> Result<bool> {
        this->table_proxy->addRow(nrows);
        return true;
    });
}


Result<bool>
SafeTableProxy::Close() {
    if(!is_closed) {
        std::shared_ptr<void> defer_close(nullptr, [this](...){ this->is_closed = true; });

        return run_isolated([this]() -> Result<bool> {
            this->table_proxy->close();
            return true;
        });
    }

    return false;
}

} // namespace arcae
