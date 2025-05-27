#ifndef ARCAE_RWLOCK_H
#define ARCAE_RWLOCK_H

#include <cassert>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>

#include <fcntl.h>

namespace arcae {

class RWLock {
 private:
  static std::string make_lockname(std::string_view prefix = "lock");

 public:
  static RWLock Create(std::string_view lock_filename = "", bool write = false);

  int fd_;
  std::string lock_filename_;
  std::shared_mutex mutex_;
  std::unique_lock<std::shared_mutex> write_lock_;
  std::shared_lock<std::shared_mutex> read_lock_;
};

}  // namespace arcae

#endif  // #define ARCAE_RWLOCK_H
