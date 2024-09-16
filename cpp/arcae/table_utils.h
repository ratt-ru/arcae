#ifndef ARCAE_TABLE_UTILS_H
#define ARCAE_TABLE_UTILS_H

#include <string>

#include <arrow/api.h>

#include <casacore/casa/Utilities/DataType.h>
#include <casacore/tables/Tables/TableProxy.h>

namespace arcae {
namespace detail {

// Returns OK if the ColumnExists otherwise returns an error Status
arrow::Status ColumnExists(const casacore::TableProxy& tp, const std::string& column);

// Returns true if this is a primite CASA type
bool IsPrimitiveType(casacore::DataType data_type);

// Returns true if the table was opened readonly and was re-opened in readwrite mode
// otherwise returns false
bool MaybeReopenRW(casacore::TableProxy& tp);

}  // namespace detail
}  // namespace arcae

#endif  // ARCAE_TABLE_UTILS_H
