#include "arcae/column_write_visitor.h"

#include <arrow/api.h>

#include <casacore/tables/Tables.h>

namespace arcae {


arrow::Result<std::shared_ptr<arrow::Array>>
ColumnWriteVisitor::GetFlatArray(bool nulls) const {
    auto data = array_;

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
                                    "match the size of the data buffer ", data_size);
}

arrow::Status
ColumnWriteVisitor::FailIfNotUTF8(const std::shared_ptr<arrow::DataType> & arrow_dtype) const {
    if(arrow_dtype == arrow::utf8()) { return arrow::Status::OK(); }
    return arrow::Status::Invalid(arrow_dtype->ToString(), " incompatible with casacore::String");
}


arrow::Status ColumnWriteVisitor::VisitTpBool() {
    // TODO(sjperkins)
    // Looks like casacore bool is actually a char, improve this
    return this->WriteColumn<casacore::Bool>();
}

arrow::Status ColumnWriteVisitor::VisitTpChar() {
    return this->WriteColumn<casacore::Char>();
}

arrow::Status ColumnWriteVisitor::VisitTpUChar() {
    return this->WriteColumn<casacore::uChar>();
}

arrow::Status ColumnWriteVisitor::VisitTpShort() {
    return this->WriteColumn<casacore::Short>();
}

arrow::Status ColumnWriteVisitor::VisitTpUShort() {
    return this->WriteColumn<casacore::uShort>();
}

arrow::Status ColumnWriteVisitor::VisitTpInt() {
    return this->WriteColumn<casacore::Int>();
}

arrow::Status ColumnWriteVisitor::VisitTpUInt() {
    return this->WriteColumn<casacore::uInt>();
}

arrow::Status ColumnWriteVisitor::VisitTpInt64() {
    return this->WriteColumn<casacore::Int64>();
}

arrow::Status ColumnWriteVisitor::VisitTpFloat() {
    return this->WriteColumn<casacore::Float>();
}

arrow::Status ColumnWriteVisitor::VisitTpDouble() {
    return this->WriteColumn<casacore::Double>();
}

arrow::Status ColumnWriteVisitor::VisitTpComplex() {
    return this->WriteColumn<casacore::Complex>();
}

arrow::Status ColumnWriteVisitor::VisitTpDComplex() {
    return this->WriteColumn<casacore::DComplex>();
}

arrow::Status ColumnWriteVisitor::VisitTpString() {
    return this->WriteColumn<casacore::String>();
}

arrow::Status ColumnWriteVisitor::VisitTpQuantity() {
    return arrow::Status::NotImplemented("TpQuantity");
}

arrow::Status ColumnWriteVisitor::VisitTpRecord() {
    return arrow::Status::NotImplemented("TpRecord");
}

arrow::Status ColumnWriteVisitor::VisitTpTable() {
    return arrow::Status::NotImplemented("TpTable");
}

} // namespace arcae
