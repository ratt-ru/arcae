#include <memory>
#include <sstream>

#include <arrow/util/logging.h>  // IWYU pragma: keep

#include <casacore/casa/Json.h>
#include <casacore/tables/Tables/TableIterProxy.h>

#include "safe_table_proxy.h"


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

Status SafeTableProxy::FailIfClosed() const {
    return is_closed ? Status::Invalid("Table is closed") : Status::OK();
}

Result<std::shared_ptr<SafeTableProxy>>
SafeTableProxy::Make(const casacore::String & filename) {
    auto proxy = std::shared_ptr<SafeTableProxy>(new SafeTableProxy());
    ARROW_ASSIGN_OR_RAISE(proxy->io_pool, ::arrow::internal::ThreadPool::Make(1));

    // Mark as closed so that if construction fails, we don't try to close it
    proxy->is_closed = true;

    auto future = arrow::DeferNotOk(proxy->io_pool->Submit([&filename]() -> arrow::Result<std::shared_ptr<TableProxy>> {
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
    }));

    ARROW_RETURN_NOT_OK(future.result());
    proxy->table_future = std::move(future);
    proxy->is_closed = false;

    return proxy;
}

arrow::Result<std::shared_ptr<arrow::Table>>
SafeTableProxy::to_arrow(casacore::uInt startrow, casacore::uInt nrow, const std::vector<std::string> & columns) const {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    return SAFE_TABLE_FUNCTOR([this, startrow, nrow, &columns]() -> arrow::Result<std::shared_ptr<arrow::Table>> {
        ARROW_ASSIGN_OR_RAISE(auto table_proxy, this->table_future.result());
        auto & casa_table = table_proxy->table();
        auto table_desc = casa_table.tableDesc();
        auto column_names = columns.size() == 0 ? table_desc.columnNames() : casacore::Vector<casacore::String>(columns);
        auto startrow_ = startrow;
        auto nrow_ = nrow;


        if(startrow_ >= casa_table.nrow()) {
            startrow_ = casa_table.nrow();
            nrow_ = 0;
        }

        if(startrow_ + nrow_ >= casa_table.nrow()) {
            nrow_ = std::min(casa_table.nrow(), casa_table.nrow() - startrow_);
        }

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
            auto visitor = ColumnConvertVisitor(table_column, startrow_, nrow_);
            auto visit_status = visitor.Visit(column_desc.dataType());

            if(!visit_status.ok()) {
                ARROW_LOG(WARNING)
                    << "Ignoring " << column_name << " " << visit_status;
                continue;
            }

            if(!visitor.array) {
                ARROW_LOG(ERROR)
                    << "Ignoring " << column_name
                    << ". Arrow array not created by Visitor";
                continue;
            }

            std::ostringstream json_oss;
            casacore::JsonOut column_json(json_oss);

            column_json.start();
            column_json.write(CASA_DESCRIPTOR, table_proxy->recordColumnDesc(column_desc, true));
            column_json.end();

            auto column_metadata = arrow::KeyValueMetadata::Make(
                {ARCAE_METADATA}, {json_oss.str()});
            auto arrow_field = std::make_shared<arrow::Field>(
                column_name, visitor.array->type(),
                true, std::move(column_metadata));
            fields.emplace_back(std::move(arrow_field));
            arrays.emplace_back(std::move(visitor.array));
        }

        std::ostringstream json_oss;
        casacore::JsonOut table_json(json_oss);

        table_json.start();
        table_json.write(CASA_DESCRIPTOR, table_proxy->getTableDescription(true, true));
        table_json.end();

        auto table_metadata = arrow::KeyValueMetadata::Make(
            {ARCAE_METADATA}, {json_oss.str()});

        auto schema = arrow::schema(fields, std::move(table_metadata));
        auto table = arrow::Table::Make(std::move(schema), arrays, nrow_);
        auto status = table->Validate();

        if(status.ok()) {
            return table;
        } else {
            return status;
        }
    });
}


Result<std::vector<std::string>>
SafeTableProxy::columns() const {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    return SAFE_TABLE_FUNCTOR([this]() -> Result<std::vector<std::string>> {
        ARROW_ASSIGN_OR_RAISE(auto table_proxy, this->table_future.result());
        auto column_names = table_proxy->table().tableDesc().columnNames();
        return std::vector<std::string>(column_names.begin(), column_names.end());
    });
}


Result<casacore::uInt>
SafeTableProxy::ncolumns() const {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    return SAFE_TABLE_FUNCTOR([this]() -> Result<casacore::uInt> {
        ARROW_ASSIGN_OR_RAISE(auto table_proxy, this->table_future.result());
        return table_proxy->table().tableDesc().ncolumn();
    });
}

Result<casacore::uInt>
SafeTableProxy::nrow() const {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    return SAFE_TABLE_FUNCTOR([this]() -> Result<casacore::uInt> {
        ARROW_ASSIGN_OR_RAISE(auto table_proxy, this->table_future.result());
        return table_proxy->table().nrow();
    });
}

Result<std::vector<std::shared_ptr<SafeTableProxy>>>
SafeTableProxy::partition(
    const std::vector<std::string> & partition_columns,
    const std::vector<std::string> & sort_columns) const {

    if(partition_columns.size() == 0) {
        return Status::Invalid("No partitioning columns provided");
    }

    ARROW_RETURN_NOT_OK(FailIfClosed());

    return SAFE_TABLE_FUNCTOR([this, &partition_columns, &sort_columns]() -> Result<std::vector<std::shared_ptr<SafeTableProxy>>> {
        ARROW_ASSIGN_OR_RAISE(auto table_proxy, this->table_future.result());

        casacore::Block<casacore::String> casa_sort_cols(sort_columns.size());
        for(casacore::uInt i=0; i < sort_columns.size(); ++i)
            { casa_sort_cols[i] = sort_columns[i]; }

        auto casa_part_cols = casacore::Vector<casacore::String>(partition_columns);
        std::vector<std::shared_ptr<SafeTableProxy>> result;
        auto partition_proxy = std::make_shared<TableProxy>();
        auto iter = TableIterProxy(*table_proxy, casa_part_cols, "a", "q");

        while(iter.nextPart(*partition_proxy)) {
            auto stp = std::shared_ptr<SafeTableProxy>(new SafeTableProxy());
            if(casa_sort_cols.size() > 0) {
                partition_proxy = std::make_shared<TableProxy>(partition_proxy->table().sort(casa_sort_cols));
            }

            stp->table_future = Future<std::shared_ptr<TableProxy>>::MakeFinished(std::move(partition_proxy));
            stp->io_pool = this->io_pool;
            stp->is_closed = false;
            result.push_back(std::move(stp));
            partition_proxy = std::make_shared<TableProxy>();
        }

        return result;
    });
}


Result<bool>
SafeTableProxy::close() {
    if(!is_closed) {
        std::shared_ptr<void> defer_close(nullptr, [this](...){ this->is_closed = true; });

        return SAFE_TABLE_FUNCTOR([this]() -> Result<bool> {
            ARROW_ASSIGN_OR_RAISE(auto table_proxy, this->table_future.result());
            table_proxy->close();
            return true;
        });
    } else {
        return false;
    }
}

} // namespace arcae
