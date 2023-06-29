#ifndef ARCAE_CASA_VISITORS_H
#define ARCAE_CASA_VISITORS_H

#include <casacore/casa/Utilities/DataType.h>

#include <arrow/api.h>

namespace arcae {

class CasaArrayVisitor {
 public:
  virtual ~CasaArrayVisitor() = default;

};


#define VISIT_CASA_TYPES(VISIT) \
  VISIT(TpBool) \
  VISIT(TpChar) \
  VISIT(TpUChar) \
  VISIT(TpShort) \
  VISIT(TpUShort) \
  VISIT(TpInt) \
  VISIT(TpUInt) \
  VISIT(TpInt64) \
  VISIT(TpFloat) \
  VISIT(TpDouble) \
  VISIT(TpComplex) \
  VISIT(TpDComplex) \
  VISIT(TpString) \
  VISIT(TpQuantity) \
  VISIT(TpRecord) \
  VISIT(TpTable)



/// \brief Abstract type visitor class
///
/// Subclass this to create a visitor that can be used with the DataType::Accept()
/// method.
class CasaTypeVisitor {
 public:
  virtual ~CasaTypeVisitor() = default;

  arrow::Status Visit(const casacore::DataType & dtype);

  #define VISIT(CASA_TYPE) \
    virtual arrow::Status Visit##CASA_TYPE();

  VISIT_CASA_TYPES(VISIT)

  #undef VISIT
};

} // namespace arcae

#endif
