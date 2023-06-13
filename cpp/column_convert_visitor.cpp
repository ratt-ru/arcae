#include <arrow/api.h>

#include <casacore/tables/Tables.h>

#include "column_convert_visitor.h"
#include "complex_type.h"

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
