#include "arcae/column_write_visitor.h"

#include <memory>

#include <arrow/api.h>
#include <arrow/compute/cast.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include <casacore/casa/Utilities/DataType.h>
#include <casacore/tables/Tables.h>

#include "arcae/complex_type.h"

namespace arcae {

const std::map<casacore::DataType, std::shared_ptr<arrow::DataType>>
ColumnWriteVisitor::arrow_type_map_ = {
    {casacore::TpBool, arrow::uint8()},
    {casacore::TpChar, arrow::int8()},
    {casacore::TpUChar, arrow::uint8()},
    {casacore::TpShort, arrow::int16()},
    {casacore::TpUShort, arrow::uint16()},
    {casacore::TpInt, arrow::int32()},
    {casacore::TpUInt, arrow::uint32()},
    {casacore::TpInt64, arrow::int64()},
    {casacore::TpFloat, arrow::float32()},
    {casacore::TpDouble, arrow::float64()},
    {casacore::TpComplex, complex64()},
    {casacore::TpDComplex, complex128()},
    {casacore::TpString, arrow::utf8()}
};


arrow::Status ColumnWriteVisitor::Visit() {
    return CasaTypeVisitor::Visit(map_.get().column_.get().columnDesc().dataType());
}


arrow::Result<std::shared_ptr<arrow::Array>>
ColumnWriteVisitor::MaybeCastFlatArray(const std::shared_ptr<arrow::Array> & data) {
    auto casa_dtype = map_.get().column_.get().columnDesc().dataType();

    if(auto kv = arrow_type_map_.find(casa_dtype); kv != arrow_type_map_.end()) {
        auto arrow_dtype = kv->second;

        if(auto ct = std::dynamic_pointer_cast<ComplexFloatType>(arrow_dtype); ct) {
            arrow_dtype = arrow::float32();
        } else if (auto ct = std::dynamic_pointer_cast<ComplexDoubleType>(arrow_dtype); ct) {
            arrow_dtype = arrow::float64();
        }

        // Types match, don't cast
        if(data->type() == arrow_dtype) {
            return data;
        }

        ARROW_ASSIGN_OR_RAISE(auto datum, arrow::compute::Cast(data, arrow_dtype));
        return datum.make_array();
    }

    return arrow::Status::TypeError(casa_dtype, " does not map to an arrow type");
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
