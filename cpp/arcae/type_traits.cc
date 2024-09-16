#include "arcae/type_traits.h"

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include <casacore/casa/Utilities/DataType.h>

using ::arrow::Result;
using ::arrow::Status;
using ::casacore::DataType;

namespace arcae {
namespace detail {

Result<std::size_t> CasaDataTypeSize(DataType data_type) {
  switch (data_type) {
    case DataType::TpBool:
      return sizeof(CasaDataTypeTraits<DataType::TpBool>::CasaType);
      break;
    case DataType::TpChar:
      return sizeof(CasaDataTypeTraits<DataType::TpChar>::CasaType);
      break;
    case DataType::TpUChar:
      return sizeof(CasaDataTypeTraits<DataType::TpUChar>::CasaType);
      break;
    case DataType::TpShort:
      return sizeof(CasaDataTypeTraits<DataType::TpShort>::CasaType);
      break;
    case DataType::TpUShort:
      return sizeof(CasaDataTypeTraits<DataType::TpUShort>::CasaType);
      break;
    case DataType::TpInt:
      return sizeof(CasaDataTypeTraits<DataType::TpInt>::CasaType);
      break;
    case DataType::TpUInt:
      return sizeof(CasaDataTypeTraits<DataType::TpUInt>::CasaType);
      break;
    case DataType::TpInt64:
      return sizeof(CasaDataTypeTraits<DataType::TpInt64>::CasaType);
      break;
    case DataType::TpFloat:
      return sizeof(CasaDataTypeTraits<DataType::TpFloat>::CasaType);
      break;
    case DataType::TpDouble:
      return sizeof(CasaDataTypeTraits<DataType::TpDouble>::CasaType);
      break;
    case DataType::TpComplex:
      return sizeof(CasaDataTypeTraits<DataType::TpComplex>::CasaType);
      break;
    case DataType::TpDComplex:
      return sizeof(CasaDataTypeTraits<DataType::TpDComplex>::CasaType);
      break;
    case DataType::TpString:
      return sizeof(CasaDataTypeTraits<DataType::TpString>::CasaType);
      break;
    default:
      return Status::NotImplemented("Data type ", data_type);
  }
}

Result<std::shared_ptr<arrow::DataType>> ArrowDataType(DataType data_type) {
  switch (data_type) {
    case DataType::TpBool:
      return CasaDataTypeTraits<DataType::TpBool>::ArrowDataType();
      break;
    case DataType::TpChar:
      return CasaDataTypeTraits<DataType::TpChar>::ArrowDataType();
      break;
    case DataType::TpUChar:
      return CasaDataTypeTraits<DataType::TpUChar>::ArrowDataType();
      break;
    case DataType::TpShort:
      return CasaDataTypeTraits<DataType::TpShort>::ArrowDataType();
      break;
    case DataType::TpUShort:
      return CasaDataTypeTraits<DataType::TpUShort>::ArrowDataType();
      break;
    case DataType::TpInt:
      return CasaDataTypeTraits<DataType::TpInt>::ArrowDataType();
      break;
    case DataType::TpUInt:
      return CasaDataTypeTraits<DataType::TpUInt>::ArrowDataType();
      break;
    case DataType::TpInt64:
      return CasaDataTypeTraits<DataType::TpInt64>::ArrowDataType();
      break;
    case DataType::TpFloat:
      return CasaDataTypeTraits<DataType::TpFloat>::ArrowDataType();
      break;
    case DataType::TpDouble:
      return CasaDataTypeTraits<DataType::TpDouble>::ArrowDataType();
      break;
    case DataType::TpComplex:
      return CasaDataTypeTraits<DataType::TpComplex>::ArrowDataType();
      break;
    case DataType::TpDComplex:
      return CasaDataTypeTraits<DataType::TpDComplex>::ArrowDataType();
      break;
    case DataType::TpString:
      return CasaDataTypeTraits<DataType::TpString>::ArrowDataType();
      break;
    default:
      return Status::TypeError("No arrow type for CASA type ", data_type);
      break;
  }
}

}  // namespace detail
}  // namespace arcae
