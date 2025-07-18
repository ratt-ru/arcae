#include "arcae/configuration.h"

#include <cctype>
#include <string>
#include <string_view>

namespace arcae {

namespace {

static constexpr std::string_view TRUE = "TRUE";

bool TruthyString(std::string_view data) {
  std::string_view::size_type pos = 0;
  for (auto c = std::begin(data); c != std::end(data); ++c) {
    if (std::isspace(*c)) continue;
    if (std::isalpha(*c)) {
      if (pos > TRUE.size()) return false;
      if (std::toupper(*c) != TRUE[pos++]) return false;
    } else {
      return false;
    }
  }

  return pos == TRUE.size();
}

bool TruthyDigit(std::string_view data) {
  for (auto c = std::begin(data); c != std::end(data); ++c) {
    if (std::isspace(*c)) continue;
    if (std::isdigit(*c) && *c == '1') return true;
  }

  return false;
}

bool IsValueTruthy(std::string_view value) {
  return TruthyDigit(value) || TruthyString(value);
}

}  // namespace

arrow::Result<std::string> Configuration::Get(const std::string& key) const {
  if (auto it = kvmap_.find(key); it != kvmap_.end()) {
    return it->second;
  }
  return arrow::Status::KeyError(key);
}

bool Configuration::IsTruthy(const std::string& key) const {
  if (auto it = kvmap_.find(key); it != std::end(kvmap_))
    return IsValueTruthy(it->second);
  return false;
}

bool Configuration::IsTruthy(const std::string& key,
                             std::string_view default_value) const {
  if (auto it = kvmap_.find(key); it != std::end(kvmap_))
    return IsValueTruthy(it->second);
  return IsValueTruthy(std::move(default_value));
}

std::string Configuration::GetDefault(const std::string& key,
                                      std::string default_value) const {
  if (auto it = kvmap_.find(key); it != kvmap_.end()) {
    return it->second;
  }
  return default_value;
}

arrow::Result<bool> Configuration::Delete(const std::string& key) {
  if (auto it = kvmap_.find(key); it != kvmap_.end()) {
    kvmap_.erase(it);
    return true;
  }
  return arrow::Status::KeyError(key);
}

}  // namespace arcae
