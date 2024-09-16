#ifndef ARCAE_CONFIG_H
#define ARCAE_CONFIG_H

#include <map>
#include <string>
#include <vector>

#include <arrow/result.h>

namespace arcae {

class Configuration {
 private:
  std::map<std::string, std::string> kvmap_;

 public:
  std::size_t Size() const { return kvmap_.size(); }

  std::vector<std::string> GetKeys() const {
    std::vector<std::string> keys;
    for (auto& kv : kvmap_) {
      keys.push_back(kv.first);
    }
    return keys;
  }

  void Set(const std::string& key, std::string value) { kvmap_[key] = value; }

  arrow::Result<std::string> Get(const std::string& key) const;
  std::string GetDefault(const std::string& key, std::string default_value) const;
  arrow::Result<bool> Delete(const std::string& key);
};

}  // namespace arcae

#endif
