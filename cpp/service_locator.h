#ifndef CASA_ARROW_SERVICE_LOCATOR_H
#define CASA_ARROW_SERVICE_LOCATOR_H

#include <memory>
#include <mutex>

#include "configuration.h"

class ServiceLocator {
private:
  static std::mutex mutex_;
  static std::unique_ptr<Configuration> configuration_service_;

public:
  static void SetConfigurationService(std::unique_ptr<Configuration> service);
  static Configuration & configuration();
};

#endif
