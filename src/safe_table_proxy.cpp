#include <memory>
#include <sstream>


#include "safe_table_proxy.h"

#define SAFE_TABLE_FUNCTOR(functor) \
    return arrow::DeferNotOk(this->io_pool->Submit((functor))).result()

SafeTableProxy::~SafeTableProxy() {
    this->close();
    io_pool->Shutdown(true);
};


arrow::Status SafeTableProxy::FailIfClosed() const {
    return is_closed ? arrow::Status::Invalid("Table is closed") : arrow::Status::OK();
}

arrow::Result<std::shared_ptr<SafeTableProxy>>
SafeTableProxy::Make(const casacore::String & filename) {
    auto proxy = std::shared_ptr<SafeTableProxy>(new SafeTableProxy());
    ARROW_ASSIGN_OR_RAISE(proxy->io_pool, arrow::internal::ThreadPool::Make(1));

    // Mark as closed so that if construction fails, we don't try to close it
    proxy->is_closed = true;

    auto future = arrow::DeferNotOk(proxy->io_pool->Submit([&]() -> arrow::Result<std::shared_ptr<casacore::TableProxy>> {
        casacore::Record record;
        casacore::TableLock lock(casacore::TableLock::LockOption::AutoNoReadLocking);

        record.define("option", "usernoread");
        record.define("internal", lock.interval());
        record.define("maxwait", casacore::Int(lock.maxWait()));

        try {
            return std::make_shared<casacore::TableProxy>(
                filename, record, casacore::Table::TableOption::Old);
        } catch(std::exception & e) {
            return arrow::Status::Invalid(e.what());
        }
    }));

    ARROW_RETURN_NOT_OK(future.result());
    proxy->table_future = std::move(future);
    proxy->is_closed = false;

    return proxy;
}

arrow::Result<std::shared_ptr<arrow::Table>>
SafeTableProxy::to_arrow(void) const {
    ARROW_RETURN_NOT_OK(FailIfClosed());
    ARROW_ASSIGN_OR_RAISE(auto n, nrow());
    return to_arrow(0, n);
}

arrow::Result<std::shared_ptr<arrow::Table>>
SafeTableProxy::to_arrow(casacore::uInt startrow, casacore::uInt nrow) const {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    SAFE_TABLE_FUNCTOR(([this, startrow, nrow]() -> arrow::Result<std::shared_ptr<arrow::Table>> {
        ARROW_ASSIGN_OR_RAISE(auto table_proxy, this->table_future.result());
        auto & casa_table = table_proxy->table();
        auto table_desc = casa_table.tableDesc();
        auto column_names = table_desc.columnNames();
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

        for(casacore::uInt i=0; i < table_desc.ncolumn(); ++i) {
            auto table_column = casacore::TableColumn(casa_table, i);
            auto column_desc = table_column.columnDesc();
            auto visitor = ColumnConvertVisitor(table_column, startrow_, nrow_);
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


            std::ostringstream json_oss;
            casacore::JsonOut column_json(json_oss);

            column_json.start();
            column_json.write(CASA_DESCRIPTOR, table_proxy->recordColumnDesc(column_desc, true));
            column_json.end();

            auto column_metadata = arrow::KeyValueMetadata::Make(
                {CASA_ARROW_METADATA}, {json_oss.str()});
            auto arrow_field = std::make_shared<arrow::Field>(
                column_names[i], visitor.array->type(),
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
            {CASA_ARROW_METADATA}, {json_oss.str()});

        auto schema = arrow::schema(fields, std::move(table_metadata));
        auto table = arrow::Table::Make(std::move(schema), arrays, nrow_);
        auto status = table->Validate();

        if(status.ok()) {
            return table;
        } else {
            return status;
        }
    }));
}


arrow::Result<std::vector<std::string>>
SafeTableProxy::columns() const {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    SAFE_TABLE_FUNCTOR([this]() -> arrow::Result<std::vector<std::string>> {
        ARROW_ASSIGN_OR_RAISE(auto table_proxy, this->table_future.result());
        auto column_names = table_proxy->table().tableDesc().columnNames();
        return std::vector<std::string>(column_names.begin(), column_names.end());
    });
}


arrow::Result<casacore::uInt>
SafeTableProxy::ncolumns() const {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    SAFE_TABLE_FUNCTOR([this]() -> arrow::Result<casacore::uInt> {
        ARROW_ASSIGN_OR_RAISE(auto table_proxy, this->table_future.result());
        return table_proxy->table().tableDesc().ncolumn();
    });
}

arrow::Result<casacore::uInt>
SafeTableProxy::nrow() const {
    ARROW_RETURN_NOT_OK(FailIfClosed());

    SAFE_TABLE_FUNCTOR([this]() -> arrow::Result<casacore::uInt> {
        ARROW_ASSIGN_OR_RAISE(auto table_proxy, this->table_future.result());
        return table_proxy->table().nrow();
    });
}

arrow::Result<bool>
SafeTableProxy::close() {
    if(!is_closed) {
        std::shared_ptr<void> defer_close(nullptr, [this](...){ this->is_closed = true; });

        SAFE_TABLE_FUNCTOR([this]() -> arrow::Result<bool> {
            ARROW_ASSIGN_OR_RAISE(auto table_proxy, this->table_future.result());
            table_proxy->close();
            return true;
        });
    } else {
        return false;
    }
}
