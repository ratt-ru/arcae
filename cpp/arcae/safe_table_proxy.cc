#include <memory>
#include <sstream>

#include <arrow/util/logging.h>  // IWYU pragma: keep

#include <casacore/casa/Json.h>
#include <casacore/tables/Tables/TableIter.h>
#include <casacore/tables/Tables/TableProxy.h>

#include "arcae/safe_table_proxy.h"


using ::arrow::DataType;
using ::arrow::Buffer;
using ::arrow::Future;
using ::arrow::Result;
using ::arrow::Status;

using ::casacore::Record;
using ::casacore::Table;
using ::casacore::TableColumn;
using ::casacore::TableLock;
using ::casacore::Table;
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
        auto table_json = casacore::JsonOut (json_oss);
        auto proxy = casacore::TableProxy(*this->table_);

        table_json.start();
        table_json.write(CASA_DESCRIPTOR, proxy.getTableDescription(true, true));
        table_json.end();

        return json_oss.str();
    });
}

arrow::Result<std::string>
SafeTableProxy::GetColumnDescriptor(const std::string & column) const {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    return run_isolated([this, &column]() -> arrow::Result<std::string> {
        const auto & table_desc = this->table_->tableDesc();

        if(this->table_->actualTableDesc().isColumn(column)) {
            return arrow::Status::UnknownError(column, " does not exist");
        }

        auto json_oss = std::ostringstream{};
        auto column_json = casacore::JsonOut{json_oss};
        auto proxy = casacore::TableProxy(*this->table_);

        column_json.start();
        column_json.write(column, proxy.getColumnDescription(column, true, true));
        column_json.end();

        return json_oss.str();
    });
}


arrow::Result<std::shared_ptr<arrow::Array>>
SafeTableProxy::GetColumn(const std::string & column, casacore::uInt startrow, casacore::uInt nrow) const {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    return run_isolated([this, &column, startrow, nrow]() -> arrow::Result<std::shared_ptr<arrow::Array>> {

        if(!this->table_->actualTableDesc().isColumn(column)) {
            return arrow::Status::UnknownError(column, " does not exist");
        }

        auto table_column = TableColumn(*this->table_, column);
        const auto & column_desc = table_column.columnDesc();
        auto [start_row, n_row] = ClampRows(*this->table_, startrow, nrow);
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
        const auto & table_desc = this->table_->actualTableDesc();
        auto column_names = columns.size() == 0 ? table_desc.columnNames() : casacore::Vector<casacore::String>(columns);
        auto [start_row, n_row] = ClampRows(*this->table_, startrow, nrow);
        auto fields = arrow::FieldVector();
        auto arrays = arrow::ArrayVector();

        for(casacore::uInt i=0; i < column_names.size(); ++i) {
            auto column_name = column_names[i];

            if(!table_desc.isColumn(column_name)) {
                ARROW_LOG(WARNING) << column_name << " is not a valid column";
                continue;
            }

            auto table_column = TableColumn(*this->table_, column_name);
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


            auto json_oss = std::ostringstream{};
            auto column_json = casacore::JsonOut{json_oss};
            auto proxy = casacore::TableProxy{*this->table_};

            column_json.start();
            column_json.write(CASA_DESCRIPTOR, proxy.recordColumnDesc(column_desc, true));
            column_json.end();

            auto column_metadata = arrow::KeyValueMetadata::Make(
                {ARCAE_METADATA}, {json_oss.str()});
            auto arrow_field = std::make_shared<arrow::Field>(
                column_name, visitor.array_->type(),
                true, std::move(column_metadata));
            fields.emplace_back(std::move(arrow_field));
            arrays.emplace_back(std::move(visitor.array_));
        }

        auto json_oss = std::ostringstream{};
        auto table_json = casacore::JsonOut{json_oss};
        auto proxy = casacore::TableProxy{*this->table_};

        table_json.start();
        table_json.write(CASA_DESCRIPTOR, proxy.getTableDescription(true, true));
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
        const auto& column_names = this->table_->actualTableDesc().columnNames();
        return std::vector<std::string>(column_names.begin(), column_names.end());
    });
}


Result<casacore::uInt>
SafeTableProxy::nColumns() const {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    return run_isolated([this]() -> Result<casacore::uInt> {
        return this->table_->actualTableDesc().ncolumn();
    });
}

Result<casacore::uInt>
SafeTableProxy::nRow() const {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    return run_isolated([this]() -> Result<casacore::uInt> {
        return this->table_->nrow();
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
        auto casa_part_cols = casacore::Block<casacore::String>(partition_columns.size());
        auto casa_sort_cols = casacore::Block<casacore::String>(sort_columns.size());

        for(casacore::uInt i=0; i < partition_columns.size(); ++i) {
            casa_part_cols[i] = partition_columns[i];
        }

        for(casacore::uInt i=0; i < sort_columns.size(); ++i) {
            casa_sort_cols[i] = sort_columns[i];
        }

        std::vector<std::shared_ptr<SafeTableProxy>> result;
        auto iter = casacore::TableIterator(*this->table_, casa_part_cols);

        while(!iter.pastEnd()) {
            auto stp = std::make_shared<enable_make_shared_stp>();
            stp->table_ = [&iter, &casa_sort_cols]() -> std::shared_ptr<Table> {
                if(casa_sort_cols.size() > 0) {
                    return std::make_shared<Table>(iter.table().sort(casa_sort_cols));
                } else {
                    return std::make_shared<Table>(iter.table());
                }
            }();

            stp->io_pool_ = this->io_pool_;
            stp->is_closed_ = false;
            result.emplace_back(std::move(stp));
        }

        return result;
    });
}


Result<bool>
SafeTableProxy::AddRows(casacore::uInt nrows) {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    return run_isolated([this, nrows]() -> Result<bool> {
        this->table_->addRow(nrows);
        return true;
    });
}


Result<bool>
SafeTableProxy::Close() {
    if(!is_closed_) {
        std::shared_ptr<void> defer_close(nullptr, [this](...){ this->is_closed_ = true; });

        return run_isolated([this]() -> Result<bool> {
            this->table_->flush();
            this->table_->unlock();
            return true;
        });
    }

    return false;
}

} // namespace arcae
