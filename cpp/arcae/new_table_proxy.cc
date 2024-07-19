#include "arcae/new_table_proxy.h"

#include <arrow/api.h>

#include "arcae/read_impl.h"

using ::arrow::Array;
using ::arrow::Result;

namespace arcae {
namespace detail {

Result<std::shared_ptr<Array>>
NewTableProxy::GetColumn(
    const std::string & column,
    const Selection & selection,
    const std::shared_ptr<Array> & result) const {
  return ReadImpl(itp_, column, selection, result).MoveResult();
}


}  // namespace detail
}  // namespace arcae