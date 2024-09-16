#include "arcae/configuration.h"

#include <string>

namespace arcae {

arrow::Result<std::string> Configuration::Get(const std::string& key) const {
  if (auto it = kvmap_.find(key); it != kvmap_.end()) {
    return it->second;
  }
  return arrow::Status::KeyError(key);
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
