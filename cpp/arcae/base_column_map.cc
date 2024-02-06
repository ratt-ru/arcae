#include "arcae/base_column_map.h"

#include <arrow/status.h>

#include <casacore/casa/Arrays/IPosition.h>

namespace arcae {

std::ptrdiff_t SelectDim(std::size_t dim, std::size_t sdims, std::size_t ndims) {
  return std::ptrdiff_t(dim) + std::ptrdiff_t(sdims) - std::ptrdiff_t(ndims);
}

// Validate the selection against the shape
// The shape should include the row dimension
arrow::Status CheckSelectionAgainstShape(
  const casacore::IPosition & shape,
  const ColumnSelection & selection) {

  for(std::size_t dim=0; dim < shape.size(); ++dim) {
    auto sdim = SelectDim(dim, selection.size(), shape.size());
    if(sdim >= 0 && selection[sdim].size() > 0) {
      for(auto i: selection[sdim]) {
        if(i >= shape[dim]) {
          return arrow::Status::Invalid("Selection index ", i,
                                        " exceeds dimension ", dim,
                                        " of shape ", shape);
        }
      }
    }
  }
  return arrow::Status::OK();
}


} // namespace arcae