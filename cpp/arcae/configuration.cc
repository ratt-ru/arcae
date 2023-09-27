#include "arcae/configuration.h"


namespace arcae {

arrow::Result<std::string> Configuration::Get(const std::string & key) const {
    auto it = kvmap_.find(key);

    if(it == kvmap_.end()) {
        return arrow::Status::KeyError(key);
    } else {
        return it->second;
    }
}


std::string Configuration::GetDefault(const std::string & key, std::string default_value) const {
    auto it = kvmap_.find(key);

    if(it == kvmap_.end()) {
        return std::move(default_value);
    } else {
        return it->second;
    }
}


arrow::Result<bool> Configuration::Delete(const std::string & key) {
    auto it = kvmap_.find(key);

    if(it == kvmap_.end()) {
        return arrow::Status::KeyError(key);
    } else {
        kvmap_.erase(it);
        return true;
    }
}

} // namespace arcae
