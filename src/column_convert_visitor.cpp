#include <arrow/api.h>

#include <casacore/tables/Tables.h>

#include "column_convert_visitor.h"

ColumnConvertVisitor::ColumnConvertVisitor(const casacore::TableColumn & column)
    : column(column) {}

arrow::Status ColumnConvertVisitor::VisitTpBool() {
    // TODO(sjperkins)
    // Looks like casacore bool is actually a char, improve this
    return this->ConvertColumn<casacore::Bool>(arrow::uint8());
}

arrow::Status ColumnConvertVisitor::VisitTpChar() {
    return this->ConvertColumn<casacore::Char>(arrow::uint8());
}

arrow::Status ColumnConvertVisitor::VisitTpUChar() {
    return arrow::Status::NotImplemented("TpUChar");
}

arrow::Status ColumnConvertVisitor::VisitTpShort() {
    return arrow::Status::NotImplemented("TpShort");
}

arrow::Status ColumnConvertVisitor::VisitTpUShort() {
    return arrow::Status::NotImplemented("TpUShort");
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
    return this->ConvertColumn<casacore::Complex>(
        arrow::fixed_size_list(arrow::float32(), 2));
    // return this->ConvertColumn<casacore::Complex>(
    //     arrow::struct_({
    //         arrow::field("real", arrow::float32(), false),
    //         arrow::field("imag", arrow::float32(), false)}));
}

arrow::Status ColumnConvertVisitor::VisitTpDComplex() {
    return this->ConvertColumn<casacore::DComplex>(
        arrow::fixed_size_list(arrow::float64(), 2));
}

arrow::Status ColumnConvertVisitor::VisitTpString() {
    return this->ConvertColumn<casacore::String>(arrow::utf8());
}

arrow::Status ColumnConvertVisitor::VisitTpQuantity() {
    return arrow::Status::NotImplemented("TpQuantity");
}

arrow::Status ColumnConvertVisitor::VisitTpTable() {
    return arrow::Status::NotImplemented("TpTable");
}
