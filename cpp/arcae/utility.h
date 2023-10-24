#ifndef ARCAE_UTILITY_H
#define ARCAE_UTILITY_H

#include <casacore/casa/Arrays/IPosition.h>

namespace std {

/// @brief Implement std::hash<casacore::IPosition>
template <>
struct hash<casacore::IPosition> {
    std::size_t operator()(const casacore::IPosition & p) const noexcept;
};

}  // namespace std

#endif //  ARCAE_UTILITY_H