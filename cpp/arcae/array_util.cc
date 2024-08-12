#include "arcae/array_util.h"

#include <memory>

#include <arrow/api.h>
#include <arrow/array/array_base.h>
#include <arrow/array/util.h>
#include <arrow/util/logging.h>

#include <casacore/casa/Utilities/DataType.h>

#include "arcae/service_locator.h"

using ::arrow::Array;
using ::arrow::Buffer;
using ::arrow::Result;
using ::arrow::Status;

using ::casacore::DataType;

namespace arcae {

Status ValidateArray(const std::shared_ptr<Array> & array) {
  auto & config = ServiceLocator::configuration();
  auto validation_level = config.GetDefault("validation-level", "basic");

  if(validation_level == "basic") {
    return array->Validate();
  } else if(validation_level == "full") {
    return array->ValidateFull();
  } else if(validation_level == "none") {
    return Status::OK();
  } else {
    ARROW_LOG(WARNING) << "Invalid validation-level=" << validation_level
                        << ". No array construction validation will be performed";
    return Status::OK();
  }
}

Result<std::shared_ptr<Array>>
GetFlatArray(std::shared_ptr<Array> array, bool nulls) {
  auto array_data = array->data();

  while(true) {
    switch(array_data->type->id()) {
      case arrow::Type::LIST:
      case arrow::Type::LARGE_LIST:
      case arrow::Type::FIXED_SIZE_LIST:
      {
        if(!nulls && array_data->null_count > 0) {
          return Status::Invalid(
            "Null values were encountered "
            "during array flattening.");
        }
        array_data = array_data->child_data[0];
        break;
      }
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
        return arrow::MakeArray(array_data);
      default:
        return Status::NotImplemented(
          "Flattening of type ", array->type(),
          " is not supported");
    }
  }
}

arrow::Status
CheckElements(std::size_t map_size, std::size_t data_size) {
    if(map_size == data_size) return Status::OK();
    return Status::Invalid(
      "Number of map elements ", map_size, " does not "
      "match the length of the array ", data_size);
}


// Get the properties of the input data
arrow::Result<ArrayProperties> GetArrayProperties(
  const casacore::TableColumn & column,
  const std::shared_ptr<arrow::Array> & data)
{
  if(!data) {
    return Status::Invalid("data is null");
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
        return Status::TypeError(
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
      return Status::Invalid(
        "A list array of paired numbers must be supplied when writing "
        "to complex typed column ", column.columnDesc().name());
    }

    --ndim;

    if(fixed_shape) {
      if(shape.back() != 2) {
        return Status::Invalid(
          "A list array of paired numbers must be supplied when writing "
          "to complex typed column ", column.columnDesc().name());
      }

      shape.pop_back();
    }
  }

  // Variably shaped data
  if(!fixed_shape) {
    return ArrayProperties{
      std::nullopt,
      ndim,
      std::move(data_type),
      is_complex};
  }

  // C-ORDER to FORTRAN-ORDER
  assert(ndim == shape.size());
  auto casa_shape = casacore::IPosition(ndim, 0);
  for(std::size_t dim=0; dim < ndim; ++dim) {
    casa_shape[ndim - dim - 1] = shape[dim];
  }

  // Fixed shape data
  return ArrayProperties{
    std::make_optional(casa_shape),
    ndim,
    std::move(data_type),
    is_complex};
}


// Extracts the data buffer of the underlying result array
// ensuring it equals nbytes.
Result<std::shared_ptr<Buffer>>
GetResultBuffer(const std::shared_ptr<Array> & result,
                std::size_t nbytes) {

  if(!result) return Status::Invalid("Result array is null");
  // Extract underlying buffer from result array
  auto tmp = result->data();
  while(true) {
    switch(tmp->type->id()) {
      case arrow::Type::LARGE_LIST:
      case arrow::Type::LIST:
      case arrow::Type::FIXED_SIZE_LIST:
        if(tmp->child_data.size() == 0) return Status::Invalid("No child data");
        tmp = tmp->child_data[0];
        break;
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
      case arrow::Type::DOUBLE: {
        // The value buffer is in the last position
        // https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout
        if(tmp->buffers.size() == 0 || !tmp->buffers[tmp->buffers.size() - 1]) {
          return Status::Invalid("Result array does not contain a buffer");
        }
        auto buffer = tmp->buffers[tmp->buffers.size() - 1];
        if(std::size_t(buffer->size()) != nbytes) {
          return Status::Invalid(
            "Result buffer of ", buffer->size(),
            " bytes does not contain the"
            " expected number of bytes ", nbytes);
        }
        return buffer;
      }
      case arrow::Type::STRING:
      default:
        return Status::NotImplemented(
          "Extracting array buffer for type ",
          tmp->type->ToString());
    }
  }
}

} // namespace arcae