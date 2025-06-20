#include "arcae/rwlock.h"

#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <random>
#include <thread>

#include <gtest/gtest.h>

#include <arrow/testing/gtest_util.h>
#include <arrow/util/future.h>
#include <arrow/util/thread_pool.h>

using namespace std::literals;

using arcae::RWLock;

namespace {

static constexpr std::size_t NTASKS = 10;

TEST(RWLockTest, Basic) {
  if (auto pid = fork(); pid > 0) {
    // parent process
    ASSERT_OK_AND_ASSIGN(auto lock, RWLock::Create("pants"));
    ASSERT_OK_AND_ASSIGN(auto pool, arrow::internal::ThreadPool::Make(8));
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> ms_distribution(0, 100);

    int status;
    int waits = 0;
    std::vector<arrow::Future<>> futures;

    for (std::size_t i = 0; i < NTASKS; ++i) {
      auto ms_wait = std::chrono::milliseconds{ms_distribution(gen)};
      ASSERT_OK_AND_ASSIGN(auto future, pool->Submit([&lock, ms_wait = ms_wait, i = i]() {
        ASSERT_OK(i % 2 ? lock->lock_shared() : lock->lock());
        std::this_thread::sleep_for(ms_wait);
        i % 2 ? lock->unlock_shared() : lock->unlock();
      }));
      futures.emplace_back(std::move(future));
    };

    ASSERT_OK(lock->other_locks());
    auto result = arrow::All(futures).result();

    while (waitpid(-1, &status, WNOHANG) == 0) {
      std::this_thread::sleep_for(100ms);
    }

    ASSERT_OK(pool->Shutdown(true));
    ASSERT_FALSE(status);
  } else {
    // child process
    ASSERT_OK_AND_ASSIGN(auto lock, RWLock::Create("pants"));
    ASSERT_OK_AND_ASSIGN(auto pool, arrow::internal::ThreadPool::Make(8));

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> ms_distribution(0, 100);
    std::vector<arrow::Future<>> futures;

    for (std::size_t i = 0; i < NTASKS; ++i) {
      auto ms_wait = std::chrono::milliseconds{ms_distribution(gen)};
      ASSERT_OK_AND_ASSIGN(auto future, pool->Submit([&lock, ms_wait = ms_wait, i = i]() {
        ASSERT_OK(i % 2 ? lock->lock_shared() : lock->lock());
        std::this_thread::sleep_for(ms_wait);
        i % 2 ? lock->unlock_shared() : lock->unlock();
      }));
      futures.emplace_back(std::move(future));
    };

    ASSERT_OK(lock->other_locks());
    auto result = arrow::All(futures).result();
    exit(0);
  }
}

}  // namespace
