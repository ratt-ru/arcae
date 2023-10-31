#include <random>

#include "arcae/utility.h"

namespace std {

size_t hash<casacore::IPosition>::operator()(const casacore::IPosition& p) const noexcept {
    // https://stackoverflow.com/questions/4948780/magic-number-in-boosthash-combine
    auto static device = std::random_device();
    auto static rng = std::mt19937(device());
    const auto static seed = rng();

    auto result = seed;

    for(auto const & s: p) {
        result ^= s + 0x9e3779b9 + (result << 6) + (result >> 2);
    }

    return result;
}

bool equal_to<casacore::IPosition>::operator()(
    const casacore::IPosition & lhs,
    const casacore::IPosition & rhs) const {

    return lhs.size() == rhs.size() && lhs == rhs;
}


}  // namespace std