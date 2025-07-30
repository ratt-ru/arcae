#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arcae/service_locator.h"

using ::arcae::ServiceLocator;

namespace {

TEST(Configuration, Truthiness) {
  auto& config = ServiceLocator::configuration();

  config.Set("key", "1");
  ASSERT_TRUE(config.IsTruthy("key"));
  config.Set("key", "1 ");
  ASSERT_TRUE(config.IsTruthy("key"));
  config.Set("key", " 1");
  ASSERT_TRUE(config.IsTruthy("key"));
  config.Set("key", "0");
  ASSERT_FALSE(config.IsTruthy("key"));

  config.Set("key", "true ");
  ASSERT_TRUE(config.IsTruthy("key"));
  config.Set("key", "TrUE ");
  ASSERT_TRUE(config.IsTruthy("key"));

  config.Set("key", "true ");
  ASSERT_TRUE(config.IsTruthy("key"));
  config.Set("key", "TrUE ");
  ASSERT_TRUE(config.IsTruthy("key"));

  config.Set("key", "        true        ");
  ASSERT_TRUE(config.IsTruthy("key"));
  config.Set("key", "        TrUe        ");
  ASSERT_TRUE(config.IsTruthy("key"));

  config.Set("key", "        true");
  ASSERT_TRUE(config.IsTruthy("key"));
  config.Set("key", "        TrUe");
  ASSERT_TRUE(config.IsTruthy("key"));

  config.Set("key", "somewhattrue");
  ASSERT_FALSE(config.IsTruthy("key"));

  config.Set("key", "somewhattrue");
  ASSERT_FALSE(config.IsTruthy("key"));
}

TEST(Configuration, TruthinessDefault) {
  auto& config = ServiceLocator::configuration();
  config.Set("key", "0");
  ASSERT_FALSE(config.IsTruthy("key", "true"));
  ASSERT_TRUE(config.IsTruthy("nokey", "true"));

  config.Set("key", "false");
  ASSERT_FALSE(config.IsTruthy("key", "true"));
  ASSERT_TRUE(config.IsTruthy("nokey", "true"));
}

}  // namespace
