#include <random>
#include <string>

namespace arcae {
namespace {

static constexpr char hex[] = "0123456789abcdef";

}  // namespace

std::string hexuuid(std::size_t n) {
  auto static device = std::random_device();
  auto static rng = std::mt19937(device());
  auto distribution = std::uniform_int_distribution<int>(0, 15);

  std::string result;
  result.reserve(n);

  for (std::size_t i = 0; i < n; ++i) {
    result += hex[distribution(rng)];
  }

  return result;
}

}  // namespace arcae
