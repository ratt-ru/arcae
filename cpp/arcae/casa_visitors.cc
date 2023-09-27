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

// #define CASA_TYPE_VISITOR_DEFAULT(TYPE_CLASS)         \
//     arrow::Status CasaTypeVisitor::Visit##TYPE_CLASS() { \
//         return arrow::Status::NotImplemented(#TYPE_CLASS);   \
//     }

// CASA_TYPE_VISITOR_DEFAULT(TpBool)
// CASA_TYPE_VISITOR_DEFAULT(TpChar)
// CASA_TYPE_VISITOR_DEFAULT(TpUChar)
// CASA_TYPE_VISITOR_DEFAULT(TpShort)
// CASA_TYPE_VISITOR_DEFAULT(TpUShort)
// CASA_TYPE_VISITOR_DEFAULT(TpInt)
// CASA_TYPE_VISITOR_DEFAULT(TpUInt)
// CASA_TYPE_VISITOR_DEFAULT(TpInt64)
// CASA_TYPE_VISITOR_DEFAULT(TpFloat)
// CASA_TYPE_VISITOR_DEFAULT(TpDouble)
// CASA_TYPE_VISITOR_DEFAULT(TpComplex)
// CASA_TYPE_VISITOR_DEFAULT(TpDComplex)
// CASA_TYPE_VISITOR_DEFAULT(TpString)
// CASA_TYPE_VISITOR_DEFAULT(TpQuantity)
// CASA_TYPE_VISITOR_DEFAULT(TpTable)

} // namespace arcae

#undef CASA_TYPE_VISITOR_DEFAULT
