#include "safe_table_proxy.h"


#define SAFE_TABLE_FUNCTOR(functor) \
    return arrow::DeferNotOk(this->io_pool->Submit((functor))).result()


arrow::Result<std::shared_ptr<SafeTableProxy>>
SafeTableProxy::Make(const casacore::String & filename) {
    auto proxy = std::shared_ptr<SafeTableProxy>(new SafeTableProxy());
    ARROW_ASSIGN_OR_RAISE(proxy->io_pool, arrow::internal::ThreadPool::Make(1));

    auto future = arrow::DeferNotOk(proxy->io_pool->Submit([&]() {
        casacore::Record record;
        casacore::TableLock lock(casacore::TableLock::LockOption::AutoNoReadLocking);

        record.define("option", "usernoread");
        record.define("internal", lock.interval());
        record.define("maxwait", casacore::Int(lock.maxWait()));

        return std::make_shared<casacore::TableProxy>(
            filename, record, casacore::Table::TableOption::Old);
    }));

    ARROW_RETURN_NOT_OK(future.result());
    proxy->table_future = std::move(future);

    return proxy;
}

arrow::Result<std::shared_ptr<arrow::Table>>
SafeTableProxy::read_table(casacore::uInt startrow, casacore::uInt nrow) const {
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

        std::ostringstream json_oss;

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


            json_oss.seekp(0);
            casacore::JsonOut column_json(json_oss);

            column_json.start();
            column_json.write(CASA_DESCRIPTOR, table_proxy->recordColumnDesc(column_desc, true));
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
        table_json.write(CASA_DESCRIPTOR, table_proxy->getTableDescription(true, true));
        table_json.end();

        auto table_metadata = arrow::KeyValueMetadata::Make(
            {CASA_ARROW_METADATA}, {json_oss.str()});

        auto schema = arrow::schema(fields, table_metadata);
        return arrow::Table::Make(std::move(schema), arrays, nrow);
    }));
}


arrow::Result<std::vector<std::string>>
SafeTableProxy::columns() const {
    SAFE_TABLE_FUNCTOR([this]() -> arrow::Result<std::vector<std::string>> {
        ARROW_ASSIGN_OR_RAISE(auto table_proxy, this->table_future.result());
        auto column_names = table_proxy->table().tableDesc().columnNames();
        return std::vector<std::string>(column_names.begin(), column_names.end());
    });
}


arrow::Result<casacore::uInt>
SafeTableProxy::ncolumns() const {
    SAFE_TABLE_FUNCTOR([this]() -> arrow::Result<casacore::uInt> {
        ARROW_ASSIGN_OR_RAISE(auto table_proxy, this->table_future.result());
        return table_proxy->table().tableDesc().ncolumn();
    });
}

arrow::Result<casacore::uInt>
SafeTableProxy::nrow() const {
    SAFE_TABLE_FUNCTOR([this]() -> arrow::Result<casacore::uInt> {
        ARROW_ASSIGN_OR_RAISE(auto table_proxy, this->table_future.result());
        return table_proxy->table().nrow();
    });
}
