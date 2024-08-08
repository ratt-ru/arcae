#include "arcae/base_column_map.h"

#include <cstddef>

#include <arrow/api.h>

#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/Utilities/DataType.h>
#include <casacore/tables/Tables/TableColumn.h>

namespace arcae {

std::ptrdiff_t SelectDim(std::size_t dim, std::size_t sdims, std::size_t ndims) {
  return std::ptrdiff_t(dim) + std::ptrdiff_t(sdims) - std::ptrdiff_t(ndims);
}

// Validate the selection against the shape
// The shape should include the row dimension
arrow::Status CheckSelectionAgainstShape(
  const casacore::IPosition & shape,
  const ColumnSelection & selection) {

  auto sel_sz = std::ptrdiff_t(selection.size());
  auto shape_sz = shape.size();

  for(std::size_t dim=0; dim < shape.size(); ++dim) {
    if(auto sdim = SelectDim(dim, sel_sz, shape_sz); sdim >= 0 && sdim < sel_sz) {
      if(selection[sdim].size() == 0) continue;
      if(selection[sdim].size() > std::size_t(shape[dim])) {
        return arrow::Status::IndexError(
          "Number of selections ", selection[sdim].size(),
          " is greater than the dimension ", shape[dim],
          " of the input data");
      }
      for(auto i: selection[sdim]) {
        if(i >= shape[dim]) {
          return arrow::Status::IndexError(
            "Selection index ", i,
            " exceeds dimension ", dim,
            " of shape ", shape);
        }
      }
    }
  }
  return arrow::Status::OK();
}


// Check column range invariants
arrow::Status CheckColumnRangeInvariants(const ColumnRanges & column_ranges) {
  auto ValidRanges = [](auto & lhs, auto & rhs) -> bool {
    return lhs.type == rhs.type || (lhs.IsEmpty() && rhs.IsMap());
  };

  for(std::size_t dim=0; dim < column_ranges.size(); ++dim) {
    const auto & column_range = column_ranges[dim];
    for(std::size_t r=1; r < column_range.size(); ++r) {
      if(!ValidRanges(column_range[r - 1], column_range[r])) {
        return arrow::Status::NotImplemented(
          "Heterogenous Column Ranges in a dimension ",
          column_range[r-1].type, " ", column_range[r].type);
      }
    }
  }

  return arrow::Status::OK();
}


} // namespace arcae