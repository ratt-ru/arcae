#ifndef ARCAE_RWLOCK_H
#define ARCAE_RWLOCK_H

#include <cerrno>

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>

#if defined(_WIN32) || defined(_WIN64)
#error Posix fcntl support required
#else
#include <fcntl.h>
#include <unistd.h>
#endif

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/util/logging.h>

using namespace std::literals;

namespace arcae {

using FcntlLockType = decltype(flock::l_type);

// Base locking class
class BaseRWLock {
 public:
  virtual arrow::Status lock() = 0;
  virtual arrow::Status lock_shared() = 0;
  virtual arrow::Result<bool> try_lock() = 0;
  virtual arrow::Result<bool> try_lock_shared() = 0;

  virtual void unlock() = 0;
  virtual void unlock_shared() = 0;
  virtual ~BaseRWLock() {};
};

// Noops
class NullRWLock : public BaseRWLock {
 public:
  virtual arrow::Status lock() override { return arrow::Status::OK(); }
  virtual arrow::Status lock_shared() override { return arrow::Status::OK(); }

  virtual arrow::Result<bool> try_lock() override { return true; }
  virtual arrow::Result<bool> try_lock_shared() override { return true; }

  virtual void unlock() override {}
  virtual void unlock_shared() override {};
};

//
class RWLock : public BaseRWLock, std::enable_shared_from_this<RWLock> {
 public:
  struct LockInfo {
    FcntlLockType lock_type;
    pid_t pid;
  };

 public:
  static arrow::Result<std::shared_ptr<RWLock>> Create(
      std::string_view lock_filename = "");

  RWLock(const RWLock&) = delete;
  RWLock(RWLock&& rhs) = delete;
  RWLock& operator=(const RWLock&) = delete;
  RWLock& operator=(RWLock&& rhs) = delete;
  ~RWLock() override;

  // Acquire a write lock
  arrow::Status lock() override { return lock_impl(true); }
  // Acquire a read lock
  arrow::Status lock_shared() override { return lock_impl(false); }

  // Release a write lock
  void unlock() override;
  // Release a read lock
  void unlock_shared() override;

  // Try to acquire a write lock
  // Returns immediately indicating success or failure
  arrow::Result<bool> try_lock() override { return try_lock_impl(true); }
  // Try to acquire a read lock
  // Returns immediately indicating success or failure
  arrow::Result<bool> try_lock_shared() override { return try_lock_impl(false); }

  // Return the lock filename
  std::string_view lock_filename() const { return lock_filename_; }
  // Is RWLock managing a file descriptor
  bool has_fd() const { return fd_ != -1; }

  // Testing function prone to race conditions...
  std::size_t fcntl_readers();

  // Return information about other locks that would block
  // the requested lock type
  arrow::Result<LockInfo> other_locks(FcntlLockType lock_type);

 protected:
  RWLock(int fd, std::string_view lock_filename)
      : fd_(fd), fcntl_readers_(0), lock_filename_(lock_filename) {}

  arrow::Status lock_impl(bool write);
  arrow::Result<bool> try_lock_impl(bool write);

  static std::string make_lockname(std::string_view prefix = "lock");

 private:
  int fd_;
  int fcntl_readers_;
  std::string lock_filename_;
  std::shared_mutex mutex_;
  std::mutex fcntl_mutex_;
};

class RWLockGuard {
 private:
  BaseRWLock& lock_;
  bool write_;

 public:
  RWLockGuard(BaseRWLock& lock, bool write = false) : lock_(lock), write_(write) {
    if (auto status = write_ ? lock_.lock() : lock_.lock_shared(); !status.ok()) {
      ARROW_LOG(ERROR) << "Unable to lock " << status;
    }
  };
  ~RWLockGuard() { write_ ? lock_.unlock() : lock_.unlock_shared(); }
};

}  // namespace arcae

#endif  // #define ARCAE_RWLOCK_H
