#include "arcae/column_write_visitor.h"

#include <arrow/api.h>

#include <casacore/tables/Tables.h>

namespace arcae {

arrow::Status ColumnWriteVisitor::VisitTpBool() {
    // TODO(sjperkins)
    // Looks like casacore bool is actually a char, improve this
    return this->ConvertColumn<casacore::Bool>();
}

arrow::Status ColumnWriteVisitor::VisitTpChar() {
    return this->ConvertColumn<casacore::Char>();
}

arrow::Status ColumnWriteVisitor::VisitTpUChar() {
    return this->ConvertColumn<casacore::uChar>();
}

arrow::Status ColumnWriteVisitor::VisitTpShort() {
    return this->ConvertColumn<casacore::Short>();
}

arrow::Status ColumnWriteVisitor::VisitTpUShort() {
    return this->ConvertColumn<casacore::uShort>();
}

arrow::Status ColumnWriteVisitor::VisitTpInt() {
    return this->ConvertColumn<casacore::Int>();
}

arrow::Status ColumnWriteVisitor::VisitTpUInt() {
    return this->ConvertColumn<casacore::uInt>();
}

arrow::Status ColumnWriteVisitor::VisitTpInt64() {
    return this->ConvertColumn<casacore::Int64>();
}

arrow::Status ColumnWriteVisitor::VisitTpFloat() {
    return this->ConvertColumn<casacore::Float>();
}

arrow::Status ColumnWriteVisitor::VisitTpDouble() {
    return this->ConvertColumn<casacore::Double>();
}

arrow::Status ColumnWriteVisitor::VisitTpComplex() {
    return this->ConvertColumn<casacore::Complex>();
}

arrow::Status ColumnWriteVisitor::VisitTpDComplex() {
    return this->ConvertColumn<casacore::DComplex>();
}

arrow::Status ColumnWriteVisitor::VisitTpString() {
    return this->ConvertColumn<casacore::String>();
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
