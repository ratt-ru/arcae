#include "arcae/rwlock.h"

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstring>

#include <chrono>
#include <memory>
#include <mutex>
#include <random>
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

namespace arcae {

arrow::Result<std::shared_ptr<RWLock>> RWLock::Create(std::string_view lock_filename) {
  // Enable std::make_shared with a protected constructor
  struct enable_rwlock : public RWLock {
   public:
    enable_rwlock(int fd, std::string_view lock_name) : RWLock(fd, lock_name) {};
  };

  if (lock_filename.empty()) return std::make_shared<enable_rwlock>(-1, lock_filename);

  int fd = -1;
  // Attempt to open or create the lock file in readwrite mode
  int flags = O_CREAT | O_RDWR;
  int mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

  if (fd = open(std::string(lock_filename).c_str(), flags, mode); fd == -1) {
    switch (errno) {
      // Can't access the directory
      // In this case the lock falls back to readonly
      case EACCES:
        fd = -1;
        break;
      default:
        return arrow::Status::IOError("Create/Open of ", lock_filename, " failed with  ",
                                      strerror(errno), " (", errno, ')');
    }
  }

  return std::make_shared<enable_rwlock>(fd, lock_filename);
}

RWLock::~RWLock() {
  if (fd_ != -1) {
    struct flock lock_data = {
        .l_type = F_UNLCK,
        .l_whence = SEEK_SET,
        .l_start = 0,
        .l_len = 0,
    };

    fcntl(fd_, F_SETLK, &lock_data);
    fcntl_readers_ = 0;
    close(fd_);
  }

  mutex_.unlock();
  mutex_.unlock_shared();
  lock_filename_.clear();
}

void RWLock::unlock() {
  if (fd_ != -1) {
    struct flock lock_data = {
        .l_type = F_UNLCK,
        .l_whence = SEEK_SET,
        .l_start = 0,
        .l_len = 0,
    };

    fcntl(fd_, F_SETLK, &lock_data);
  }
  mutex_.unlock();
}

void RWLock::unlock_shared() {
  std::unique_lock<std::mutex> fnctl_lock(fcntl_mutex_);
  // Last reader releases the fcntl lock
  if (fd_ != -1 && fcntl_readers_ > 0 && --fcntl_readers_ == 0) {
    struct flock lock_data = {
        .l_type = F_UNLCK,
        .l_whence = SEEK_SET,
        .l_start = 0,
        .l_len = 0,
    };
    fcntl(fd_, F_SETLK, &lock_data);
  };

  mutex_.unlock_shared();
}

std::size_t RWLock::fcntl_readers() {
  std::unique_lock<std::mutex> fnctl_lock(fcntl_mutex_);
  return fcntl_readers_;
}

arrow::Result<RWLock::LockInfo> RWLock::other_locks(FcntlLockType lock_type) {
  struct flock lock_data = {
      .l_type = lock_type,
      .l_whence = SEEK_SET,
      .l_start = 0,
      .l_len = 0,
      .l_pid = getpid(),
  };

  if (fcntl(fd_, F_GETLK, &lock_data) == -1) {
    return arrow::Status::IOError("Fail to retrieve lock information for ",
                                  lock_filename_);
  }

  return RWLock::LockInfo{lock_data.l_type, lock_data.l_pid};
}

arrow::Status RWLock::lock_impl(bool write) {
  const std::string_view lock_type(write ? "write" : "read");
  std::unique_lock<std::mutex> fcntl_reader_lock(fcntl_mutex_, std::defer_lock);

  if (write) {
    if (!has_fd()) return arrow::Status::Cancelled("Readonly Table");
    mutex_.lock();
  } else {
    mutex_.lock_shared();
    if (!has_fd()) return arrow::Status::OK();
    fcntl_reader_lock.lock();
    if (fcntl_readers_++ > 0) return arrow::Status::OK();
  }

  // Repeatedly attempt acquisition of
  // the fcntl lock with exponential backoff.
  // A blocking F_SETLKW could be used but in practice
  // this produces EDEADLK.
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> dist(-1e-4, 1e-4);
  std::chrono::duration<double> lock_sleep = 10ms;

  auto status = [&]() {
    while (true) {
      struct flock lock_data = {
          .l_type = FcntlLockType(write ? F_WRLCK : F_RDLCK),
          .l_whence = SEEK_SET,
          .l_start = 0,
          .l_len = 0,
          //.l_pid = getpid(),
      };

      // Indicate success if file-locking succeeds
      if (fcntl(fd_, F_SETLK, &lock_data) == 0) return arrow::Status::OK();

      switch (errno) {
        case EAGAIN: {
          auto jitter = std::chrono::duration<double>(dist(gen));
          std::this_thread::sleep_for(lock_sleep + jitter);
          if (lock_sleep * 1.5 < 1s) lock_sleep *= 1.5;
        } break;
        case EACCES:
          return arrow::Status::IOError("Permission denied attempting a ", lock_type,
                                        " lock on ", lock_filename_);
        case EBADF:
          return arrow::Status::IOError("Bad file descriptor ", fd_, " for ",
                                        lock_filename_);
        case ENOLCK:
          return arrow::Status::IOError(
              "No locks were available on ", lock_filename_, ". ",
              "If located on an NFS filesystem, please log an issue");
        default:
          return arrow::Status::IOError("Error code ", errno, " returned attempting a ",
                                        lock_type, " lock on ", lock_filename_);
      }
    }
  }();

  // Unlock if fcntl lock acquisition failed
  if (!status.ok()) {
    if (write) {
      mutex_.unlock();
    } else {
      mutex_.unlock_shared();
      fcntl_readers_--;
    }
  }

  return status;
}

arrow::Result<bool> RWLock::try_lock_impl(bool write) {
  std::string_view lock_type = write ? "write" : "read";
  std::unique_lock<std::mutex> fcntl_reader_lock(fcntl_mutex_, std::defer_lock);

  if (write) {
    if (!has_fd()) return arrow::Status::Cancelled("Readonly Table");
    if (!mutex_.try_lock()) return false;
  } else {
    if (!mutex_.try_lock_shared()) return false;
    if (!has_fd()) return true;
    fcntl_reader_lock.lock();
    if (fcntl_readers_++ > 0) return true;
  }

  auto result = [&]() -> arrow::Result<bool> {
    while (true) {
      struct flock lock_data = {
          .l_type = FcntlLockType(write ? F_WRLCK : F_RDLCK),
          .l_whence = SEEK_SET,
          .l_start = 0,
          .l_len = 0,
          //.l_pid = getpid(),
      };

      // Indicate success if file-locking succeeds
      if (fcntl(fd_, F_SETLK, &lock_data) == 0) return true;

      switch (errno) {
        case EAGAIN:
          return false;
        case EACCES:
          return arrow::Status::IOError("Permission denied attempting a ", lock_type,
                                        " lock on ", lock_filename_);
        case EBADF:
          return arrow::Status::IOError("Bad file descriptor ", fd_, " for ",
                                        lock_filename_);
        case ENOLCK:
          return arrow::Status::IOError(
              "No locks were available on ", lock_filename_, ". ",
              "If located on an NFS filesystem, please log an issue");
        default:
          return arrow::Status::IOError("Error code ", errno, " returned attempting a ",
                                        lock_type, " lock on ", lock_filename_);
      }
    }
  }();

  // Unlock mutex if fcntl lock acquisition failed
  if (!result.ok() || result.ValueOrDie() == false) {
    if (write) {
      mutex_.unlock();
    } else {
      mutex_.unlock_shared();
      --fcntl_readers_;
    }
  }

  return result;
}

}  // namespace arcae
