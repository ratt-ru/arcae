#include "arcae/base_column_map.h"

#include <cstddef>
#include <memory>

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

// Get the properties of the input data
arrow::Result<ArrayProperties> GetArrayProperties(
  const casacore::TableColumn & column,
  const ColumnSelection & selection,
  const std::shared_ptr<arrow::Array> & data)
{
  if(!data) {
    return arrow::Status::ExecutionError("data is null");
  }

  // Starting state is a fixed shape array of 1 dimension
  // whose size is the number of rows in the arrow array
  auto fixed_shape = true;
  auto shape = std::vector<std::int64_t>{data->length()};
  auto ndim = std::size_t{1};
  auto tmp_data = data;

  auto MaybeUpdateShapeAndNdim = [&](auto list) -> arrow::Result<std::shared_ptr<arrow::Array>> {
    ++ndim;
    using ListType = std::decay<decltype(list)>;

    assert(list->null_count() == 0);

    if(!fixed_shape || list->length() == 0) {
      fixed_shape = false;
      return list->values();
    }

    auto dim_size = list->value_length(0);

    if constexpr(!std::is_same_v<ListType, std::shared_ptr<arrow::FixedSizeListArray>>) {
      for(std::int64_t i=0; i < list->length(); ++i) {
        if(dim_size != list->value_length(i)) {
          fixed_shape = false;
          return list->values();
        }
      }
    }

    shape.emplace_back(dim_size);
    return list->values();
  };

  std::shared_ptr<arrow::DataType> data_type;

  for(auto done=false; !done;) {
    switch(tmp_data->type_id()) {
      // Traverse nested list
      case arrow::Type::LARGE_LIST:
      {
        auto base_list = std::dynamic_pointer_cast<arrow::LargeListArray>(tmp_data);
        assert(base_list);
        ARROW_ASSIGN_OR_RAISE(tmp_data, MaybeUpdateShapeAndNdim(base_list));
        break;
      }
      case arrow::Type::LIST:
      {
        auto base_list = std::dynamic_pointer_cast<arrow::ListArray>(tmp_data);
        assert(base_list);
        ARROW_ASSIGN_OR_RAISE(tmp_data, MaybeUpdateShapeAndNdim(base_list));
        break;
      }
      case arrow::Type::FIXED_SIZE_LIST:
      {
        auto base_list = std::dynamic_pointer_cast<arrow::FixedSizeListArray>(tmp_data);
        assert(base_list);
        ARROW_ASSIGN_OR_RAISE(tmp_data, MaybeUpdateShapeAndNdim(base_list));
        break;
      }
      // We've traversed all nested arrays
      // infer the base type of the array
      case arrow::Type::BOOL:
      case arrow::Type::UINT8:
      case arrow::Type::UINT16:
      case arrow::Type::UINT32:
      case arrow::Type::UINT64:
      case arrow::Type::INT8:
      case arrow::Type::INT16:
      case arrow::Type::INT32:
      case arrow::Type::INT64:
      case arrow::Type::FLOAT:
      case arrow::Type::DOUBLE:
      case arrow::Type::STRING:
        data_type = tmp_data->type();
        done = true;
        break;
      default:
        return arrow::Status::TypeError(
            "Shape derivation of ",
            tmp_data->type()->ToString(),
            " is not supported");
    }
  }

  // If we're writing to Complex Data columns, the
  // Data array must contain a nested list of paired values
  // at it's root.
  // Modify ndim and shape to reflect the CASA shape
  const auto & casa_type = column.columnDesc().dataType();
  auto is_complex = casa_type == casacore::TpComplex ||
                           casa_type == casacore::TpDComplex;

  if(is_complex) {
    if(ndim <= 1) {
      return arrow::Status::Invalid(
        "A list array of paired numbers must be supplied when writing "
        "to complex typed column ", column.columnDesc().name());
    }

    --ndim;

    if(fixed_shape) {
      if(shape.back() != 2) {
        return arrow::Status::Invalid(
          "A list array of paired numbers must be supplied when writing "
          "to complex typed column ", column.columnDesc().name());
      }

      shape.pop_back();
    }
  }

  // Variably shaped data
  if(!fixed_shape) {
    return ArrayProperties{std::nullopt, ndim, std::move(data_type), is_complex};
  }

  // C-ORDER to FORTRAN-ORDER
  assert(ndim == shape.size());
  auto casa_shape = casacore::IPosition(ndim, 0);
  for(std::size_t dim=0; dim < ndim; ++dim) {
    casa_shape[ndim - dim - 1] = shape[dim];
  }

  // Fixed shape data
  return ArrayProperties{std::make_optional(casa_shape), ndim, std::move(data_type), is_complex};
}


} // namespace arcae