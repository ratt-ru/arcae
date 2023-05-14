#include "configuration.h"

void Configuration::Set(std::string key, std::string value) {
    kvmap_[key] = value;
}

arrow::Result<std::string> Configuration::Get(std::string key) const {
    auto it = kvmap_.find(key);

    if(it == kvmap_.end()) {
        return arrow::Status::KeyError(key);
    } else {
        return it->second;
    }
}

std::string Configuration::GetDefault(std::string key, std::string default_value) const {
    auto it = kvmap_.find(key);

    if(it == kvmap_.end()) {
        return std::move(default_value);
    } else {
        return it->second;
    }
}
