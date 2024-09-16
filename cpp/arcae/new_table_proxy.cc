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
#include "arcae/table_utils.h"
#include "arcae/write_impl.h"

using ::arrow::Array;
using ::arrow::Result;
using ::arrow::Table;

using ::casacore::JsonOut;
using ::casacore::JsonParser;
using ::casacore::Record;
using ::casacore::TableProxy;

namespace arcae {

Result<std::string> NewTableProxy::GetTableDescriptor() const {
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
  return detail::ReadTableImpl(itp_, columns, selection).MoveResult();
}

Result<std::shared_ptr<Array>> NewTableProxy::GetColumn(
    const std::string& column, const detail::Selection& selection,
    const std::shared_ptr<Array>& result) const {
  return ReadImpl(itp_, column, selection, result).MoveResult();
}

Result<bool> NewTableProxy::PutColumn(const std::string& column,
                                      const std::shared_ptr<Array>& data,
                                      const detail::Selection& selection) const {
  return WriteImpl(itp_, column, data, selection).MoveResult();
}

Result<std::string> NewTableProxy::Name() const {
  return itp_
      ->RunAsync(
          [](const TableProxy& tp) -> std::string { return tp.table().tableName(); })
      .MoveResult();
}

Result<std::vector<std::string>> NewTableProxy::Columns() const {
  return itp_
      ->RunAsync([](const TableProxy& tp) -> std::vector<std::string> {
        const auto& columns = tp.table().tableDesc().columnNames();
        return std::vector<std::string>(std::begin(columns), std::end(columns));
      })
      .MoveResult();
}

Result<std::size_t> NewTableProxy::nColumns() const {
  return itp_
      ->RunAsync([](const TableProxy& tp) -> std::size_t {
        return tp.table().tableDesc().ncolumn();
      })
      .MoveResult();
}

Result<std::size_t> NewTableProxy::nRows() const {
  return itp_
      ->RunAsync([](const TableProxy& tp) -> std::size_t { return tp.table().nrow(); })
      .MoveResult();
}

Result<bool> NewTableProxy::AddRows(std::size_t nrows) {
  return itp_
      ->RunAsync([nrows = nrows](TableProxy& tp) {
        detail::MaybeReopenRW(tp);
        tp.addRow(nrows);
        return true;
      })
      .MoveResult();
}

Result<bool> NewTableProxy::AddColumns(const std::string& json_columndescs,
                                       const std::string& json_dminfo) {
  return itp_
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

Result<bool> NewTableProxy::Close() { return itp_->Close(); }

}  // namespace arcae
