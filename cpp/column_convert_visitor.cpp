#include <arrow/api.h>

#include <casacore/tables/Tables.h>

#include "column_convert_visitor.h"
#include "complex_type.h"

namespace arcae {

ColumnConvertVisitor::ColumnConvertVisitor(
    const casacore::TableColumn & column,
    casacore::uInt startrow,
    casacore::uInt nrow,
    arrow::MemoryPool * pool)
    : column(column), startrow(startrow), nrow(nrow),
      endrow(startrow + nrow),
      column_desc(column.columnDesc()), pool(pool) {

    assert(endrow <= column.nrow());
}


Status ColumnConvertVisitor::ValidateArray(const std::shared_ptr<arrow::Array> & array) {
    auto & config = ServiceLocator::configuration();
    auto validation_level = config.GetDefault("validation-level", "basic");

    if(validation_level == "basic") {
        return array->Validate();
    } else if(validation_level == "full") {
        return array->ValidateFull();
    } else if(validation_level == "none") {
        return Status::OK();
    } else {
        ARROW_LOG(WARNING) << "Invalid validation-level " << validation_level
                            << ". No array construction validation will be performed";
        return Status::OK();
    }
}


Result<std::shared_ptr<arrow::Array>>
ColumnConvertVisitor::MakeArrowPrimitiveArray(
        const std::shared_ptr<Buffer> & buffer,
        casacore::uInt nelements,
        const std::shared_ptr<DataType> & arrow_dtype) {

    if(auto complex_dtype = std::dynamic_pointer_cast<ComplexType>(arrow_dtype)) {
        auto child_array = std::make_shared<arrow::PrimitiveArray>(
            complex_dtype->value_type(), 2*nelements, buffer);

        auto & config = ServiceLocator::configuration();
        auto convert_strategy = config.GetDefault("casa.convert.strategy", "fixed");
        auto npos = std::string::npos;

        if(auto pos = convert_strategy.find("complex"); pos != npos) {
            // NOTE(sjperkins)
            // Check the FixedSizeListAray layout documents
            // https://arrow.apache.org/docs/format/Columnar.html#fixed-size-list-layout
            // A single empty buffer {nullptr} must be provided otherwise this segfaults
            auto array_data = arrow::ArrayData::Make(
                complex_dtype, nelements, {nullptr}, {child_array->data()});
            return complex_dtype->MakeArray(std::move(array_data));
        } else if(auto pos = convert_strategy.find("fixed"); pos == 0) {
            return arrow::FixedSizeListArray::FromArrays(child_array, 2);
        } else if(auto pos = convert_strategy.find("list"); pos == 0) {
            arrow::Int32Builder builder(pool);
            ARROW_RETURN_NOT_OK(builder.Reserve(nelements + 1));
            for(decltype(nelements) i=0; i < nelements + 1; ++i)
                { ARROW_RETURN_NOT_OK(builder.Append(2*i)); }
            ARROW_ASSIGN_OR_RAISE(auto offsets, builder.Finish());
            return arrow::ListArray::FromArrays(*offsets, *child_array);
        } else {
            return arrow::Status::Invalid("Invalid 'casa.convert.strategy=", convert_strategy, "'");
        }
    } else {
        return std::make_shared<arrow::PrimitiveArray>(arrow_dtype, nelements, buffer);
    }
}



arrow::Status ColumnConvertVisitor::VisitTpBool() {
    // TODO(sjperkins)
    // Looks like casacore bool is actually a char, improve this
    return this->ConvertColumn<casacore::Bool>(arrow::uint8());
}

arrow::Status ColumnConvertVisitor::VisitTpChar() {
    return this->ConvertColumn<casacore::Char>(arrow::int8());
}

arrow::Status ColumnConvertVisitor::VisitTpUChar() {
    return this->ConvertColumn<casacore::uChar>(arrow::uint8());
}

arrow::Status ColumnConvertVisitor::VisitTpShort() {
    return this->ConvertColumn<casacore::Short>(arrow::int16());
}

arrow::Status ColumnConvertVisitor::VisitTpUShort() {
    return this->ConvertColumn<casacore::uShort>(arrow::uint16());
}

arrow::Status ColumnConvertVisitor::VisitTpInt() {
    return this->ConvertColumn<casacore::Int>(arrow::int32());
}

arrow::Status ColumnConvertVisitor::VisitTpUInt() {
    return this->ConvertColumn<casacore::uInt>(arrow::uint32());
}

arrow::Status ColumnConvertVisitor::VisitTpInt64() {
    return this->ConvertColumn<casacore::Int64>(arrow::int64());
}

arrow::Status ColumnConvertVisitor::VisitTpFloat() {
    return this->ConvertColumn<casacore::Float>(arrow::float32());
}

arrow::Status ColumnConvertVisitor::VisitTpDouble() {
    return this->ConvertColumn<casacore::Double>(arrow::float64());
}

arrow::Status ColumnConvertVisitor::VisitTpComplex() {
    return this->ConvertColumn<casacore::Complex>(complex64());
}

arrow::Status ColumnConvertVisitor::VisitTpDComplex() {
    return this->ConvertColumn<casacore::DComplex>(complex128());
}

arrow::Status ColumnConvertVisitor::VisitTpString() {
    return this->ConvertColumn<casacore::String>(arrow::utf8());
}

arrow::Status ColumnConvertVisitor::VisitTpQuantity() {
    return arrow::Status::NotImplemented("TpQuantity");
}

arrow::Status ColumnConvertVisitor::VisitTpRecord() {
    return arrow::Status::NotImplemented("TpRecord");
}

arrow::Status ColumnConvertVisitor::VisitTpTable() {
    return arrow::Status::NotImplemented("TpTable");
}

} // namespace arcae
