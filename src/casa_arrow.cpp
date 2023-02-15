#include <sstream>

#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/TableProxy.h>
#include <casacore/casa/Json.h>

#include <arrow/util/logging.h>  // IWYU pragma: keep
#include <arrow/util/thread_pool.h>

#include "casa_arrow.h"
#include "column_convert_visitor.h"


static const std::string CASA_ARROW_METADATA = "__casa_arrow_metadata__";
static const std::string CASA_DESCRIPTOR = "__casa_descriptor__";


class ThreadedTableProxy : casacore::TableProxy {
public:
    using TableFuture = arrow::Future<std::shared_ptr<casacore::TableProxy>>;

private:
    TableFuture table_future;
    std::shared_ptr<arrow::internal::ThreadPool> io_pool;
protected:
    ThreadedTableProxy() {};

public:
    static arrow::Result<std::shared_ptr<ThreadedTableProxy>> Make(const casacore::String & filename) {
        auto proxy = std::shared_ptr<ThreadedTableProxy>(new ThreadedTableProxy());

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

        future.Wait();
        ARROW_RETURN_NOT_OK(future.result());
        proxy->table_future = std::move(future);

        return proxy;
    }

    arrow::Result<casacore::uInt> ncolumns() {
        return arrow::DeferNotOk(this->io_pool->Submit(
            [this]() -> arrow::Result<casacore::uInt> {
                ARROW_ASSIGN_OR_RAISE(auto table_proxy, this->table_future.result());
                return table_proxy->table().tableDesc().ncolumn();
            }
        )).result();
    }


    arrow::Result<casacore::uInt> nrow() {
        return arrow::DeferNotOk(this->io_pool->Submit(
            [this]() -> arrow::Result<casacore::uInt> {
                ARROW_ASSIGN_OR_RAISE(auto table_proxy, this->table_future.result());
                return table_proxy->table().nrow();
            }
        )).result();
    }

    arrow::Result<std::shared_ptr<arrow::Table>> read_table(casacore::uInt startrow, casacore::uInt nrow) {
        auto future = arrow::DeferNotOk(this->io_pool->Submit(
            [this]() -> arrow::Result<std::shared_ptr<arrow::Table>> {
                ARROW_ASSIGN_OR_RAISE(auto table_proxy, this->table_future.result());
                auto & casa_table = table_proxy->table();
                auto table_desc = casa_table.tableDesc();
                auto column_names = table_desc.columnNames();

                auto fields = arrow::FieldVector();
                auto arrays = arrow::ArrayVector();

                std::ostringstream json_oss;

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
                return arrow::Table::Make(std::move(schema), arrays, casa_table.nrow());
            })
        );

        return future.result();
    }
};

arrow::Result<std::shared_ptr<arrow::Table>> open_table(const std::string & filename) {
    ARROW_ASSIGN_OR_RAISE(auto test_proxy, ThreadedTableProxy::Make(filename));
    ARROW_ASSIGN_OR_RAISE(auto nrow, test_proxy->nrow());
    return test_proxy->read_table(0, nrow);
}
