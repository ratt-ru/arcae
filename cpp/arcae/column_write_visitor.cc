#include "arcae/column_write_visitor.h"

#include <arrow/api.h>

#include <casacore/tables/Tables.h>

namespace arcae {


arrow::Result<std::shared_ptr<arrow::Array>>
ColumnWriteVisitor::GetFlatArray(bool nulls) const {
    auto data = map_.get().data_;

    if(!nulls && data->null_count() > 0) {
        return arrow::Status::Invalid("Null values were encountered "
                                      "during array flattening.");
    }

    while(true) {
        switch(data->type_id()) {
            case arrow::Type::LARGE_LIST:
            {
                auto lla = std::dynamic_pointer_cast<arrow::LargeListArray>(data);
                ARROW_ASSIGN_OR_RAISE(data, lla->Flatten(pool_));
                break;
            }
            case arrow::Type::LIST:
            {
                auto la = std::dynamic_pointer_cast<arrow::ListArray>(data);
                ARROW_ASSIGN_OR_RAISE(data, la->Flatten(pool_));
                break;
            }
            case arrow::Type::FIXED_SIZE_LIST:
            {
                auto fsl = std::dynamic_pointer_cast<arrow::FixedSizeListArray>(data);
                ARROW_ASSIGN_OR_RAISE(data, fsl->Flatten(pool_));
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
                return data;
            default:
                return arrow::Status::TypeError(
                    "Flattening of type ", data->type(),
                    " is not supported");
        }
    }
}

arrow::Status
ColumnWriteVisitor::CheckElements(std::size_t map_size, std::size_t data_size) const {
    if(map_size == data_size) return arrow::Status::OK();
    return arrow::Status::Invalid("Number of map elements ", map_size, " does not "
                                    "match the length of the array ", data_size);
}

arrow::Status
ColumnWriteVisitor::FailIfNotUTF8(const std::shared_ptr<arrow::DataType> & arrow_dtype) const {
    if(arrow_dtype == arrow::utf8()) { return arrow::Status::OK(); }
    return arrow::Status::Invalid(arrow_dtype->ToString(), " incompatible with casacore::String");
}

} // namespace arcae
