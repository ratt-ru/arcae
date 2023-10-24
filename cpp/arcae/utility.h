#ifndef ARCAE_UTILITY_H
#define ARCAE_UTILITY_H

#include <casacore/casa/Arrays/IPosition.h>

namespace std {

/// @brief Implement std::hash<casacore::IPosition>
template <>
struct hash<casacore::IPosition> {
    std::size_t operator()(const casacore::IPosition & p) const noexcept;
};

// IPosition operator== throws Conformance error if lhs.size() != rhs.size()
template <>
struct equal_to<casacore::IPosition> {
    bool operator()(const casacore::IPosition & lhs, const casacore::IPosition & rhs) const;
};

}  // namespace std

#endif //  ARCAE_UTILITY_H