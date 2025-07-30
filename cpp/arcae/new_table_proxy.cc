#include "arcae/new_table_proxy.h"

#include <iterator>
#include <sstream>

#include <arrow/api.h>
#include <arrow/status.h>

#include <casacore/casa/Json.h>
#include <casacore/casa/Json/JsonKVMap.h>
#include <casacore/casa/Json/JsonParser.h>

#include <casacore/casa/Containers/Record.h>
#include <casacore/tables/Tables/TableProxy.h>

#include "arcae/read_impl.h"
#include "arcae/shared_fcntl_mutex.h"
#include "arcae/table_utils.h"
#include "arcae/write_impl.h"

using arcae::SharedFcntlGuard;
using LockMode = arcae::SharedFcntlGuard::LockMode;

using ::arrow::Array;
using ::arrow::Result;
using ::arrow::Table;

using ::casacore::JsonOut;
using ::casacore::JsonParser;
using ::casacore::Record;
using ::casacore::TableProxy;

namespace arcae {

// Read operations

Result<std::string> NewTableProxy::GetTableDescriptor() const {
  SharedFcntlGuard lock(*fcntl_mutex_, LockMode::READ);
  return itp_
      ->RunAsync([](TableProxy& tp) -> std::string {
        std::ostringstream oss;
        JsonOut table_json(oss);
        table_json.put(tp.getTableDescription(true, true));
        return oss.str();
      })
      .MoveResult();
}

Result<std::string> NewTableProxy::GetColumnDescriptor(const std::string& column) const {
  SharedFcntlGuard lock(*fcntl_mutex_, LockMode::READ);
  return itp_
      ->RunAsync([column = column](TableProxy& tp) -> Result<std::string> {
        ARROW_RETURN_NOT_OK(detail::ColumnExists(tp, column));
        std::ostringstream oss;
        JsonOut column_json(oss);
        column_json.start();
        column_json.write(column, tp.getColumnDescription(column, true, true));
        column_json.end();
        return oss.str();
      })
      .MoveResult();
}

Result<std::string> NewTableProxy::GetDataManagerInfo() const {
  SharedFcntlGuard lock(*fcntl_mutex_, LockMode::READ);
  return itp_
      ->RunAsync([](TableProxy& tp) {
        std::ostringstream oss;
        JsonOut dm_json(oss);
        dm_json.put(tp.getDataManagerInfo());
        return oss.str();
      })
      .MoveResult();
}

Result<std::string> NewTableProxy::GetLockOptions() const {
  SharedFcntlGuard lock(*fcntl_mutex_, LockMode::READ);
  return itp_
      ->RunAsync([](TableProxy& tp) {
        std::ostringstream oss;
        JsonOut lock_json(oss);
        lock_json.put(tp.lockOptions());
        return oss.str();
      })
      .MoveResult();
}

Result<std::shared_ptr<Table>> NewTableProxy::ToArrow(
    const detail::Selection& selection, const std::vector<std::string>& columns) const {
  SharedFcntlGuard lock(*fcntl_mutex_, LockMode::READ);
  return detail::ReadTableImpl(itp_, columns, selection).MoveResult();
}

Result<std::shared_ptr<Array>> NewTableProxy::GetColumn(
    const std::string& column, const detail::Selection& selection,
    const std::shared_ptr<Array>& result) const {
  SharedFcntlGuard lock(*fcntl_mutex_, LockMode::READ);
  return ReadImpl(itp_, column, selection, result).MoveResult();
}

Result<std::string> NewTableProxy::Name() const {
  SharedFcntlGuard lock(*fcntl_mutex_, LockMode::READ);
  return itp_
      ->RunAsync(
          [](const TableProxy& tp) -> std::string { return tp.table().tableName(); })
      .MoveResult();
}

Result<std::vector<std::string>> NewTableProxy::Columns() const {
  SharedFcntlGuard lock(*fcntl_mutex_, LockMode::READ);
  return itp_
      ->RunAsync([](const TableProxy& tp) -> std::vector<std::string> {
        const auto& columns = tp.table().tableDesc().columnNames();
        return std::vector<std::string>(std::begin(columns), std::end(columns));
      })
      .MoveResult();
}

Result<std::size_t> NewTableProxy::nColumns() const {
  SharedFcntlGuard lock(*fcntl_mutex_, LockMode::READ);
  return itp_
      ->RunAsync([](const TableProxy& tp) -> std::size_t {
        return tp.table().tableDesc().ncolumn();
      })
      .MoveResult();
}

Result<std::size_t> NewTableProxy::nRows() const {
  SharedFcntlGuard lock(*fcntl_mutex_, LockMode::READ);
  return itp_
      ->RunAsync([](const TableProxy& tp) -> std::size_t { return tp.table().nrow(); })
      .MoveResult();
}

// Write operations

Result<bool> NewTableProxy::AddRows(std::size_t nrows) {
  SharedFcntlGuard lock(*fcntl_mutex_, LockMode::WRITE);
  auto proxy = itp_->SpawnWriter();
  return proxy
      ->RunAsync([nrows = nrows](TableProxy& tp) {
        detail::MaybeReopenRW(tp);
        tp.addRow(nrows);
        return true;
      })
      .MoveResult();
}

Result<bool> NewTableProxy::AddColumns(const std::string& json_columndescs,
                                       const std::string& json_dminfo) {
  SharedFcntlGuard lock(*fcntl_mutex_, LockMode::WRITE);
  auto proxy = itp_->SpawnWriter();
  return proxy
      ->RunAsync([json_columndescs = json_columndescs,
                  json_dminfo = json_dminfo](TableProxy& tp) {
        detail::MaybeReopenRW(tp);
        Record columndescs = JsonParser::parse(json_columndescs).toRecord();
        Record dminfo = JsonParser::parse(json_dminfo).toRecord();
        tp.addColumns(columndescs, dminfo, false);
        return true;
      })
      .MoveResult();
}

Result<bool> NewTableProxy::PutColumn(const std::string& column,
                                      const std::shared_ptr<Array>& data,
                                      const detail::Selection& selection) const {
  SharedFcntlGuard lock(*fcntl_mutex_, LockMode::WRITE);
  auto proxy = itp_->SpawnWriter();
  return WriteImpl(proxy, column, data, selection).MoveResult();
}

Result<bool> NewTableProxy::Close() { return itp_->Close(); }

}  // namespace arcae
