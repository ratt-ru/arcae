#ifndef ARCAE_SERVICE_LOCATOR_H
#define ARCAE_SERVICE_LOCATOR_H

#include <memory>
#include <mutex>

#include "arcae/configuration.h"

namespace arcae {

class ServiceLocator {
 private:
  static std::mutex mutex_;
  static std::unique_ptr<Configuration> configuration_service_;

 public:
  static void SetConfigurationService(std::unique_ptr<Configuration> service);
  static Configuration& configuration();
};

}  // namespace arcae

#endif
