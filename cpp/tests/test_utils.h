#ifndef ARCAE_TEST_UTILS_H
#define ARCAE_TEST_UTILS_H

#include <string>

#include <casacore/ms/MeasurementSets/MSMainEnums.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>
#include <casacore/tables/Tables/ArrayColumn.h>
#include <casacore/tables/Tables/ScalarColumn.h>
#include <casacore/tables/Tables/TableColumn.h>

namespace arcae {

// Get a Scalar Measurement Set column
template <typename T>
casacore::ScalarColumn<T> GetScalarColumn(
    const casacore::MeasurementSet& ms, casacore::MSMainEnums::PredefinedColumns column) {
  return casacore::ScalarColumn<T>(
      casacore::TableColumn(ms, casacore::MeasurementSet::columnName(column)));
}

// Get a Scalar Measurement Set column
template <typename T>
casacore::ScalarColumn<T> GetScalarColumn(const casacore::MeasurementSet& ms,
                                          const std::string& column) {
  return casacore::ScalarColumn<T>(casacore::TableColumn(ms, column));
}

// Get an Array Measurement Set column
template <typename T>
casacore::ArrayColumn<T> GetArrayColumn(const casacore::MeasurementSet& ms,
                                        casacore::MSMainEnums::PredefinedColumns column) {
  return casacore::ArrayColumn<T>(
      casacore::TableColumn(ms, casacore::MeasurementSet::columnName(column)));
}

// Get an Array Measurement Set column
template <typename T>
casacore::ArrayColumn<T> GetArrayColumn(const casacore::MeasurementSet& ms,
                                        const std::string& column) {
  return casacore::ArrayColumn<T>(casacore::TableColumn(ms, column));
}

std::string hexuuid(std::size_t n = 8);

}  // namespace arcae

#endif  //  ARCAE_TEST_UTILS_H
