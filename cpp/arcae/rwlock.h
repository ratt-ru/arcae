#ifndef ARCAE_RWLOCK_H
#define ARCAE_RWLOCK_H

#include <cerrno>

#include <chrono>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>

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

// Additional Status Detail related to RWLocks
class FcntlStatusDetail : public arrow::StatusDetail {
 public:
  FcntlStatusDetail(int e) : errorno_(e) {}
  virtual const char* type_id() const override { return "FcntlStatusDetail"; }
  virtual std::string ToString() const override { return std::to_string(errorno_); }
  int errorno_;
};

// Base locking class
class BaseRWLock : public std::enable_shared_from_this<BaseRWLock> {
 public:
  virtual arrow::Status lock() = 0;
  virtual arrow::Status lock_shared() = 0;
  virtual arrow::Status try_lock() = 0;
  virtual arrow::Status try_lock_shared() = 0;

  virtual void unlock() = 0;
  virtual void unlock_shared() = 0;
  virtual ~BaseRWLock() {};
};

// Noops
class NullRWLock : public BaseRWLock {
 public:
  virtual arrow::Status lock() override { return arrow::Status::OK(); }
  virtual arrow::Status lock_shared() override { return arrow::Status::OK(); }

  virtual arrow::Status try_lock() override { return arrow::Status::OK(); }
  virtual arrow::Status try_lock_shared() override { return arrow::Status::OK(); }

  virtual void unlock() override {}
  virtual void unlock_shared() override {};
};

//
class RWLock : public BaseRWLock {
 public:
  static arrow::Result<std::shared_ptr<RWLock>> Create(
      std::string_view lock_filename = "", bool write = false);

  RWLock(const RWLock&) = delete;
  RWLock(RWLock&& rhs) = delete;
  RWLock& operator=(const RWLock&) = delete;
  RWLock& operator=(RWLock&& rhs) = delete;
  ~RWLock() override;

  arrow::Status lock() override { return lock_impl(true); }
  arrow::Status lock_shared() override { return lock_impl(false); }

  void unlock() override;
  void unlock_shared() override;

  // Race condition, use for test cases only
  std::size_t read_locks() {
    std::unique_lock<std::mutex> fnctl_lock(fcntl_mutex_);
    return fcntl_readers_;
  }

  arrow::Status other_locks();

  arrow::Status try_lock() override { return try_lock_impl(true); }
  arrow::Status try_lock_shared() override { return try_lock_impl(false); }

  template <class Rep, class Period>
  arrow::Status try_lock_for(const std::chrono::duration<Rep, Period>& timeout) {
    return try_lock_for_impl(timeout, true);
  }

  template <class Rep, class Period>
  arrow::Status try_lock_shared_for(const std::chrono::duration<Rep, Period>& timeout) {
    return try_lock_for_impl(timeout, false);
  }

 protected:
  arrow::Status lock_impl(bool write);
  arrow::Status try_lock_impl(bool write);

  template <class Rep, class Period>
  arrow::Status try_lock_for_impl(const std::chrono::duration<Rep, Period>& timeout,
                                  bool write) {
    auto lock_type = std::string_view{write ? "write" : "read"};
    auto start = std::chrono::high_resolution_clock::now();
    auto timeout_ms = std::chrono::duration_cast<std::chrono::milliseconds>(timeout);

    if (!(write ? mutex_.try_lock_for(timeout) : mutex_.try_lock_shared_for(timeout))) {
      return arrow::Status::Cancelled("Unable to acquire ", lock_type, " lock in ",
                                      std::to_string(timeout_ms.count()), "ms");
    }

    auto end = std::chrono::high_resolution_clock::now();
    constexpr auto lock_sleep = 100ms;

    std::unique_lock<std::mutex> fnctl_lock(fcntl_mutex_);
    // TODO(sjperkins)
    // This assumes that the first reader succeeds when
    // acquiring the fcntl lock. Probably should wait on
    // a signal that the lock was acquired.
    if (fcntl_readers_++ > 0) return arrow::Status::OK();

    auto status = [&]() {
      for (auto remaining = timeout - (end - start); remaining > 0s;
           remaining -= lock_sleep) {
        using LockFlagType = decltype(flock::l_type);
        struct flock lock_data = {
            .l_type = LockFlagType(write ? F_WRLCK : F_RDLCK),
            .l_whence = SEEK_SET,
            .l_start = 0,
            .l_len = 0,
            //.l_pid = getpid(),
        };

        // Indicate success if file-locking succeeds
        if (fcntl(fd_, F_SETLK, &lock_data) == 0) return arrow::Status::OK();

        switch (errno) {
          case EAGAIN:
            break;
          case EBADF:
            return arrow::Status::IOError("Bad file descriptor ", fd_, " for ",
                                          lock_filename_);
          case EACCES:
            return arrow::Status::IOError("Permission denied attempting a ", lock_type,
                                          " lock on ", lock_filename_);
          case ENOLCK:
            return arrow::Status::IOError(
                "No locks were available on ", lock_filename_, ". ",
                "If located on an NFS filesystem, please log an issue");
          default:
            return arrow::Status::IOError("Error code ", errno, " returned attempting a ",
                                          lock_type, " lock on ", lock_filename_);
        }

        std::this_thread::sleep_for(lock_sleep);
      }

      return arrow::Status::Cancelled("Failed to acquire ", lock_type, " lock on ",
                                      lock_filename_, " in ",
                                      std::to_string(timeout_ms.count()), "ms");
    }();

    // Unlock if fcntl lock acquisition failed
    if (!status.ok()) write ? mutex_.unlock() : mutex_.unlock_shared();
    return status;
  }

 private:
  static std::string make_lockname(std::string_view prefix = "lock");

  RWLock(int fd, std::string_view lock_filename, bool write = false)
      : fd_(fd), fcntl_readers_(0), lock_filename_(lock_filename) {}

  int fd_;
  int fcntl_readers_;
  std::string lock_filename_;
  std::shared_timed_mutex mutex_;
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
      std::exit(1);
    }
  };
  ~RWLockGuard() { write_ ? lock_.unlock() : lock_.unlock_shared(); }
};

}  // namespace arcae

#endif  // #define ARCAE_RWLOCK_H
