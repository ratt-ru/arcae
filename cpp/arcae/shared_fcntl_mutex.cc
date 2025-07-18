#include "arcae/shared_fcntl_mutex.h"

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

#include <fcntl.h>
#include <unistd.h>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/util/logging.h>

#include "arcae/service_locator.h"

using arrow::Result;
using arrow::Status;

namespace arcae {

namespace {

// Returns true on no locks avaiable on an NFS file system
auto ConsiderSuccessful(int err) -> bool {
  return err == ENOLCK && ServiceLocator::configuration().IsTruthy("nfs", "1");
}

// Returns true on try again and signal interrupts
auto ShouldRetry(int err) -> bool { return err == EAGAIN || err == EINTR; }

// Increase the given duration by a given factor,
// up to a maximum
template <typename Duration>
auto IncreaseSleep(const Duration& duration, double factor = 1.5,
                   const Duration maximum = 200ms) -> Duration {
  auto new_sleep = duration * factor;
  if (new_sleep > maximum) new_sleep = maximum;
  return new_sleep;
}

// Convert an errno into an arrow::Status
auto FcntlSetLockError(int err, FcntlLockType lock_type,
                       std::string_view filename) -> arrow::Status {
  const std::string_view lock_str = [&]() {
    if (lock_type == F_WRLCK) return "write lock";
    if (lock_type == F_RDLCK) return "read lock";
    if (lock_type == F_UNLCK) return "unlock";
    return "unknown";
  }();

  switch (err) {
    case EACCES:
      return Status::IOError("Permission denied attempting a ", lock_str,
                             " operation on ", filename);
    case EBADF:
      return Status::IOError("Bad file descriptor accessing ", filename, " during a ",
                             lock_str, " operation");
    case ENOLCK:
      return Status::IOError("No locks were available on ", filename, " during a ",
                             lock_str, "operation. ", "If ", filename,
                             " is located on an NFS filesystem ",
                             "consider configuring nfs=true within arcae's configuration "
                             "(See arcae's README.rst).");
    default: {
      char errmsg[1024];
      [[maybe_unused]] auto r = strerror_r(errno, errmsg, sizeof(errmsg) / sizeof(char));

      return Status::IOError("Error code ", err, " with message '", r,
                             "' returned "
                             "when attempting a lock on ",
                             filename);
    }
  }
}

}  // namespace

auto SharedFcntlMutex::Create(std::string_view lock_filename)
    -> Result<std::shared_ptr<SharedFcntlMutex>> {
  // Enable std::make_shared with a protected constructor
  struct enable_rwlock : public SharedFcntlMutex {
   public:
    enable_rwlock(int fd, std::string_view lock_name) : SharedFcntlMutex(fd, lock_name) {}
  };

  if (lock_filename.empty()) return std::make_shared<enable_rwlock>(-1, lock_filename);

  // Attempt to open or create the lock file in readwrite mode
  int flags = O_CREAT | O_RDWR;
  int mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;
  int fd = open(std::string(lock_filename).c_str(), flags, mode);

  if (fd == -1) {
    switch (errno) {
      // Can't access the file
      // In this case the lock falls back to readonly
      case EACCES:
        fd = -1;
        break;
      default: {
        char errmsg[1024];
        [[maybe_unused]] auto r =
            strerror_r(errno, errmsg, sizeof(errmsg) / sizeof(char));
        return Status::IOError("Create/Open of ", lock_filename, " failed with  ", errmsg,
                               " (", errno, ')');
      }
    }
  }

  return std::make_shared<enable_rwlock>(fd, lock_filename);
}

SharedFcntlMutex::~SharedFcntlMutex() {
  if (fd_ != -1) {
    // Note that this destructor doesn't handle all edge cases.
    // For example, if another thread holds a read lock, this
    // code will close the file descriptor, but the other thread
    // will not know about it. It's assumed that all locks are
    // released before the destructor is called.
    close(fd_);
  }
  lock_filename_.clear();
}

arrow::Status SharedFcntlMutex::unlock() {
  if (fd_ != -1) {
    struct flock lock = {
        .l_type = F_UNLCK,
        .l_whence = SEEK_SET,
        .l_start = MUTEX_LOCK_START,
        .l_len = MUTEX_LOCK_LENGTH,
    };
    std::chrono::duration<double> lock_sleep = 10ms;

    while (fcntl(fd_, F_SETLK, &lock) != 0 && !ConsiderSuccessful(errno)) {
      if (!ShouldRetry(errno))
        return FcntlSetLockError(errno, lock.l_type, lock_filename_);

      std::this_thread::sleep_for(lock_sleep);
      lock_sleep = IncreaseSleep(lock_sleep);
    }
  }
  mutex_.unlock();
  return Status::OK();
}

arrow::Status SharedFcntlMutex::unlock_shared() {
  std::unique_lock<std::mutex> fcntl_lock(fcntl_read_mutex_);
  // Last reader releases the fcntl lock
  if (fd_ != -1 && reader_count_ > 0 && --reader_count_ == 0) {
    struct flock lock = {
        .l_type = F_UNLCK,
        .l_whence = SEEK_SET,
        .l_start = MUTEX_LOCK_START,
        .l_len = MUTEX_LOCK_LENGTH,
    };
    std::chrono::duration<double> lock_sleep = 10ms;

    while (fcntl(fd_, F_SETLK, &lock) != 0 && !ConsiderSuccessful(errno)) {
      if (!ShouldRetry(errno))
        return FcntlSetLockError(errno, lock.l_type, lock_filename_);

      std::this_thread::sleep_for(lock_sleep);
      lock_sleep = IncreaseSleep(lock_sleep);
    }
  }

  mutex_.unlock_shared();
  return Status::OK();
}

auto SharedFcntlMutex::fcntl_readers() const -> std::size_t {
  std::lock_guard<std::mutex> fcntl_lock(fcntl_read_mutex_);
  return reader_count_;
}

auto SharedFcntlMutex::other_locks(FcntlLockType lock_type) const
    -> Result<SharedFcntlMutex::LockInfo> {
  struct flock lock = {
      .l_type = lock_type,
      .l_whence = SEEK_SET,
      .l_start = MUTEX_LOCK_START,
      .l_len = MUTEX_LOCK_LENGTH,
  };

  if (fcntl(fd_, F_GETLK, &lock) == -1) {
    return Status::IOError("Error ", errno,
                           " occurred while inspecting fcntl locks "
                           "on file ",
                           lock_filename_);
  }

  return SharedFcntlMutex::LockInfo{lock.l_type, lock.l_pid};
}

auto SharedFcntlMutex::lock_impl(bool write) -> Status {
  std::unique_lock<std::mutex> reader_lock(fcntl_read_mutex_, std::defer_lock);

  if (write) {
    if (!has_fd()) return Status::Cancelled("Readonly Table");
    mutex_.lock();
  } else {
    mutex_.lock_shared();
    // If there's no fcntl lock we're done
    if (!has_fd()) return Status::OK();
    // Acquire the reader lock
    // If the underlying fcntl lock has already been acquired
    // then increment again and return successfully.
    // Otherwise the read lock now guards the acquisition
    // of the fcntl lock below
    reader_lock.lock();
    if (reader_count_++ > 0) return Status::OK();
  }

  // Repeatedly attempt acquisition of
  // the fcntl lock with exponential backoff.
  // A blocking F_SETLKW could be used but in practice
  // this can result in the kernel producing EDEADLK.
  std::mt19937 gen(std::chrono::high_resolution_clock::now().time_since_epoch().count());
  std::uniform_real_distribution<> dist(-1e-4, 1e-4);
  std::chrono::duration<double> lock_sleep = 10ms;

  auto status = [&]() {
    struct flock lock {
      .l_type = FcntlLockType(write ? F_WRLCK : F_RDLCK), .l_whence = SEEK_SET,
      .l_start = MUTEX_LOCK_START, .l_len = MUTEX_LOCK_LENGTH,
    };

    while (true) {
      if (fcntl(fd_, F_SETLK, &lock) == 0 || ConsiderSuccessful(errno))
        return Status::OK();
      if (!ShouldRetry(errno))
        return FcntlSetLockError(errno, lock.l_type, lock_filename_);
      auto jitter = std::chrono::duration<double>(dist(gen));
      std::this_thread::sleep_for(lock_sleep + jitter);
      lock_sleep = IncreaseSleep(lock_sleep);
    }
  }();

  // Unlock if fcntl lock acquisition failed
  if (!status.ok()) {
    if (write) {
      mutex_.unlock();
    } else {
      // Covered by reader_lock
      reader_count_--;
      mutex_.unlock_shared();
    }
  }

  return status;
}

auto SharedFcntlMutex::try_lock_impl(bool write) -> Result<bool> {
  std::unique_lock<std::mutex> reader_lock(fcntl_read_mutex_, std::defer_lock);

  if (write) {
    if (!has_fd()) return Status::Cancelled("Readonly Table");
    if (!mutex_.try_lock()) return false;
  } else {
    if (!mutex_.try_lock_shared()) return false;
    // If there's no fcntl lock we're done
    if (!has_fd()) return true;
    // Acquire the reader lock
    // If the underlying fcntl lock has already been acquired
    // then increment again and return successfully.
    // Otherwise the read lock now guards the acquisition
    // of the fcntl lock below
    reader_lock.lock();
    if (reader_count_++ > 0) return true;
  }

  struct flock lock {
    .l_type = FcntlLockType(write ? F_WRLCK : F_RDLCK), .l_whence = SEEK_SET,
    .l_start = MUTEX_LOCK_START, .l_len = MUTEX_LOCK_LENGTH,
  };

  Result<bool> result;

  if (fcntl(fd_, F_SETLK, &lock) == 0 || ConsiderSuccessful(errno)) {
    result = true;
  } else if (ShouldRetry(errno)) {
    result = false;
  } else {
    result = FcntlSetLockError(errno, lock.l_type, lock_filename_);
  }

  // Unlock mutex if fcntl lock acquisition failed
  if (!result.ok() || result.ValueOrDie() == false) {
    if (write) {
      mutex_.unlock();
    } else {
      // Covered by reader_lock
      --reader_count_;
      mutex_.unlock_shared();
    }
  }

  return result;
}

}  // namespace arcae
