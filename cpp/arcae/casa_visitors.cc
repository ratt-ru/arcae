#include "arcae/casa_visitors.h"

namespace arcae {

arrow::Status CasaTypeVisitor::Visit(const casacore::DataType & dtype) {
    switch(dtype) {
        case casacore::DataType::TpBool:
            return VisitTpBool();
        case casacore::DataType::TpChar:
            return VisitTpChar();
        case casacore::DataType::TpUChar:
            return VisitTpUChar();
        case casacore::DataType::TpShort:
            return VisitTpShort();
        case casacore::DataType::TpUShort:
            return VisitTpUShort();
        case casacore::DataType::TpInt:
            return VisitTpInt();
        case casacore::DataType::TpUInt:
            return VisitTpUInt();
        case casacore::DataType::TpInt64:
            return VisitTpInt64();
        case casacore::DataType::TpFloat:
            return VisitTpFloat();
        case casacore::DataType::TpDouble:
            return VisitTpDouble();
        case casacore::DataType::TpComplex:
            return VisitTpComplex();
        case casacore::DataType::TpDComplex:
            return VisitTpDComplex();
        case casacore::DataType::TpString:
            return VisitTpString();
        case casacore::DataType::TpQuantity:
            return VisitTpQuantity();
        case casacore::DataType::TpRecord:
            return VisitTpRecord();
        case casacore::DataType::TpTable:
            return VisitTpTable();
        default:
            return arrow::Status::Invalid(dtype, "is not a valid CASA type");
    }
}


#define VISIT(CASA_TYPE) \
    arrow::Status CasaTypeVisitor::Visit##CASA_TYPE() { \
        return arrow::Status::NotImplemented(#CASA_TYPE); \
    }

VISIT_CASA_TYPES(VISIT)

#undef VISIT

} // namespace arcae

#undef CASA_TYPE_VISITOR_DEFAULT
