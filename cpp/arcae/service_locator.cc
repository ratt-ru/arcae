#include "arcae/service_locator.h"

#include <memory>
#include <mutex>

namespace arcae {

std::mutex ServiceLocator::mutex_;
std::unique_ptr<Configuration> ServiceLocator::configuration_service_;

void ServiceLocator::SetConfigurationService(std::unique_ptr<Configuration> service) {
  std::lock_guard<std::mutex> lock(mutex_);
  configuration_service_ = std::move(service);
}

Configuration& ServiceLocator::configuration() {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!configuration_service_) {
    configuration_service_ = std::make_unique<Configuration>();
  }

  return *configuration_service_;
}

}  // namespace arcae
