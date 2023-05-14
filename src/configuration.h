#ifndef CASA_ARROW_CONFIG_H
#define CASA_ARROW_CONFIG_H

#include <map>
#include <string>

#include <arrow/result.h>

class Configuration {
private:
    std::map<std::string, std::string> kvmap_;

public:
    void Set(std::string key, std::string value);
    arrow::Result<std::string> Get(std::string key) const;
    std::string GetDefault(std::string key, std::string default_value) const;
};

#endif
