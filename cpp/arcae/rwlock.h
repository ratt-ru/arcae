#ifndef ARCAE_RWLOCK_H
#define ARCAE_RWLOCK_H

#include <cerrno>

#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include <fcntl.h>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/util/logging.h>

using namespace std::literals;

namespace arcae {

class RWLock {
 public:
  static arrow::Result<RWLock> Create(std::string_view lock_filename = "",
                                      bool write = false);

  RWLock(RWLock&& other)
      : fd_(std::exchange(other.fd_, -1)),
        fcntl_readers_(0),
        lock_filename_(std::move(other.lock_filename_)),
        mutex_() {
    other.mutex_.unlock();
    other.mutex_.unlock_shared();
  }

  ~RWLock();

  arrow::Status lock() { return lock_impl(true); }
  arrow::Status lock_shared() { return lock_impl(false); }

  void unlock() {
    flock lock_data = {
        .l_type = F_UNLCK,
        .l_whence = SEEK_SET,
        .l_start = 0,
        .l_len = 0,
    };
    fcntl(fd_, F_SETLK, &lock_data);

    mutex_.unlock();
  }

  void unlock_shared() {
    std::unique_lock<std::mutex> fnctl_lock(fcntl_mutex_);
    if (--fcntl_readers_ == 0) {
      flock lock_data = {
          .l_type = F_UNLCK,
          .l_whence = SEEK_SET,
          .l_start = 0,
          .l_len = 0,
      };
      fcntl(fd_, F_SETLK, &lock_data);
    };

    mutex_.unlock_shared();
  }

  std::size_t read_locks() { return fcntl_readers_; }

  arrow::Status other_locks();

  arrow::Status try_lock() { return try_lock_impl(true); }
  arrow::Status try_lock_shared() { return try_lock_impl(false); }

  template <class Rep, class Period>
  arrow::Status try_lock_for(const std::chrono::duration<Rep, Period>& timeout) {
    return try_lock_for_impl(timeout, true);
  }

  template <class Rep, class Period>
  arrow::Status try_lock_shared_for(const std::chrono::duration<Rep, Period>& timeout) {
    return try_lock_for_impl(timeout, false);
  }

 protected:
  arrow::Status lock_impl(bool write) {
    std::string_view lock_type = write ? "write" : "read";
    write ? mutex_.lock() : mutex_.lock_shared();

    constexpr auto lock_sleep = 100ms;

    auto status = [&]() {
      while (true) {
        using LockType = decltype(flock::l_type);
        flock lock_data = {
            .l_type = LockType(write ? F_WRLCK : F_RDLCK),
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
    }();

    // Unlock if fcntl lock acquisition failed
    if (!status.ok()) write ? mutex_.unlock() : mutex_.unlock_shared();
    return status;
  }

  arrow::Status try_lock_impl(bool write) {
    std::string_view lock_type = write ? "write" : "read";
    if (!(write ? mutex_.try_lock() : mutex_.try_lock_shared())) {
      return arrow::Status::Cancelled("Acquisition of ", lock_type, " lock failed");
    }

    constexpr auto lock_sleep = 100ms;
    auto status = [&]() {
      while (true) {
        using LockType = decltype(flock::l_type);
        flock lock_data = {
            .l_type = LockType(write ? F_WRLCK : F_RDLCK),
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
    }();

    // Unlock if fcntl lock acquisition failed
    if (!status.ok()) write ? mutex_.unlock() : mutex_.unlock_shared();
    return status;
  }

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
        flock lock_data = {
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

}  // namespace arcae

#endif  // #define ARCAE_RWLOCK_H
