#ifndef ARCAE_SHARED_FCNTL_MUTEX_H
#define ARCAE_SHARED_FCNTL_MUTEX_H

#include <cerrno>

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>

#include <fcntl.h>
#include <unistd.h>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/util/logging.h>

using namespace std::literals;

namespace arcae {

using FcntlLockType = decltype(flock::l_type);
using FcntlLockStartType = decltype(flock::l_start);
using FcntlLocLengthType = decltype(flock::l_len);

// Base locking class
class BaseSharedFcntlMutex {
 public:
  virtual arrow::Status lock() = 0;
  virtual arrow::Status lock_shared() = 0;
  virtual arrow::Result<bool> try_lock() = 0;
  virtual arrow::Result<bool> try_lock_shared() = 0;

  virtual arrow::Status unlock() = 0;
  virtual arrow::Status unlock_shared() = 0;
  virtual ~BaseSharedFcntlMutex() {}
};

// Readonly mutex class
class ReadonlyFcntlMutex : public BaseSharedFcntlMutex {
 public:
  virtual arrow::Status lock() override { return arrow::Status::IOError("Not writable"); }
  virtual arrow::Status lock_shared() override { return arrow::Status::OK(); }

  virtual arrow::Result<bool> try_lock() override {
    return arrow::Status::IOError("Not writable");
  }
  virtual arrow::Result<bool> try_lock_shared() override { return true; }

  virtual arrow::Status unlock() override {
    return arrow::Status::IOError("Not writable");
  }
  virtual arrow::Status unlock_shared() override { return arrow::Status::OK(); }
};

// Two part lock guarding access to a CASA table across multiple processes
//
// Its interface is somewhat similar to C++ standard library mutexes
// but methods may return arrow::{Status,Result} instead.
//
// It combines:
//
// 1. A per-process lock that provides multiple-reader single-writer (MRSW)
//    access to an underlying:
// 2. fcntl process lock that coordinates multiple-reader single-writer (MRSW)
//    access across processes.
//
// (1) is implemented using a std::shared_mutex. By its nature this restricts
// access to the underlying fcntl lock to a single thread in the case of writes.
// Further care must be taken to restrict access of multiple readers
// to the underlying fcntl lock: This is accomplished with a std::mutex
// and a reader counter.
class SharedFcntlMutex : public BaseSharedFcntlMutex {
 private:
  int fd_;
  std::string lock_filename_;
  // Multiple-reader Single-writer mutex
  // Guards access at the process level
  std::shared_mutex mutex_;
  // Number of concurrent readers
  int reader_count_;
  // Guards access to the fcntl lock
  // This is only needed for reads as
  // mutex_ will only ever allow one writer
  mutable std::mutex fcntl_read_mutex_;

  // This locks the entire file
  static constexpr FcntlLockStartType MUTEX_LOCK_START = 0;
  static constexpr FcntlLocLengthType MUTEX_LOCK_LENGTH = 0;

 public:
  struct LockInfo {
    FcntlLockType lock_type;
    pid_t pid;
  };

 public:
  // Primary factory function for creating an instance
  static arrow::Result<std::shared_ptr<SharedFcntlMutex>> Create(
      std::string_view lock_filename = "");

  // Disable copies and moves
  SharedFcntlMutex(const SharedFcntlMutex&) = delete;
  SharedFcntlMutex(SharedFcntlMutex&& rhs) = delete;
  SharedFcntlMutex& operator=(const SharedFcntlMutex&) = delete;
  SharedFcntlMutex& operator=(SharedFcntlMutex&& rhs) = delete;
  ~SharedFcntlMutex() override;

  // Acquire a write lock
  arrow::Status lock() override { return lock_impl(true); }
  // Acquire a read lock
  arrow::Status lock_shared() override { return lock_impl(false); }

  // Release a write lock
  arrow::Status unlock() override;
  // Release a read lock
  arrow::Status unlock_shared() override;

  // Try to acquire a write lock
  // Returns immediately indicating success or failure
  arrow::Result<bool> try_lock() override { return try_lock_impl(true); }
  // Try to acquire a read lock
  // Returns immediately indicating success or failure
  arrow::Result<bool> try_lock_shared() override { return try_lock_impl(false); }

  // Return the lock filename
  std::string_view lock_filename() const { return lock_filename_; }
  // Is the mutex managing a file descriptor?
  bool has_fd() const { return fd_ != -1; }

  // Testing function prone to race conditions...
  std::size_t fcntl_readers() const;

  // Testing function returning information about
  // other locks that would block the requested lock type
  arrow::Result<LockInfo> other_locks(FcntlLockType lock_type) const;

 protected:
  SharedFcntlMutex(int fd, std::string_view lock_filename)
      : fd_(fd), lock_filename_(lock_filename), reader_count_(0) {}

  arrow::Status lock_impl(bool write);
  arrow::Result<bool> try_lock_impl(bool write);
};

// RAII lock/unlocking of SharedFcntlMutex
class SharedFcntlGuard {
 public:
  enum LockMode { READ, WRITE };

 private:
  BaseSharedFcntlMutex& lock_;
  LockMode mode_;

 public:
  SharedFcntlGuard(BaseSharedFcntlMutex& lock, LockMode mode = READ)
      : lock_(lock), mode_(mode) {
    if (auto status = (mode_ == READ ? lock_.lock_shared() : lock_.lock());
        !status.ok()) {
      ARROW_LOG(FATAL) << "Unable to lock " << status;
    }
  };
  ~SharedFcntlGuard() {
    if (auto status = mode_ == READ ? lock_.unlock_shared() : lock_.unlock();
        !status.ok()) {
      ARROW_LOG(FATAL) << "unable to unlock " << status;
    }
  }
};

}  // namespace arcae

#endif  // #define ARCAE_SHARED_FCNTL_MUTEX_H
