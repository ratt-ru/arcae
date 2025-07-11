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

#if defined(_WIN32) || defined(_WIN64)
#error Posix fcntl support required
#else
#include <fcntl.h>
#include <unistd.h>
#endif

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/util/logging.h>

using arrow::Result;
using arrow::Status;

namespace arcae {

namespace {

// Sets a read or write fcntl lock on the
// given file descriptor
// Returns true on success or false on an EAGAIN result
// Other fcntl error codes are propagated as errors in the result object
auto MaybeSetFcntlLock(int fd, bool write,
                       std::string_view lock_filename) -> Result<bool> {
  const std::string_view lock_type(write ? "write" : "read");
  struct flock lock_data = {
      .l_type = FcntlLockType(write ? F_WRLCK : F_RDLCK),
      .l_whence = SEEK_SET,
      .l_start = 0,
      .l_len = 0,
      //.l_pid = getpid(),
  };

  // Indicate success if file-locking succeeds
  if (fcntl(fd, F_SETLK, &lock_data) == 0) return true;

  switch (errno) {
    case EAGAIN:
      return false;
    case EACCES:
      return Status::IOError("Permission denied attempting a ", lock_type, " lock on ",
                             lock_filename);
    case EBADF:
      return Status::IOError("Bad file descriptor ", fd, " for ", lock_filename);
    case ENOLCK:
      return Status::IOError("No locks were available on ", lock_filename, ". ",
                             "If located on an NFS filesystem, please log an issue");
    default:
      return Status::IOError("Error code ", errno, " returned attempting a ", lock_type,
                             " lock on ", lock_filename);
  }
}

}  // namespace

auto SharedFcntlMutex::Create(std::string_view lock_filename)
    -> Result<std::shared_ptr<SharedFcntlMutex>> {
  // Enable std::make_shared with a protected constructor
  struct enable_rwlock : public SharedFcntlMutex {
   public:
    enable_rwlock(int fd, std::string_view lock_name)
        : SharedFcntlMutex(fd, lock_name) {};
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
        return Status::IOError("Create/Open of ", lock_filename, " failed with  ",
                               strerror(errno), " (", errno, ')');
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

void SharedFcntlMutex::unlock() {
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

void SharedFcntlMutex::unlock_shared() {
  std::unique_lock<std::mutex> fnctl_lock(fcntl_read_mutex_);
  // Last reader releases the fcntl lock
  if (fd_ != -1 && reader_counts_ > 0 && --reader_counts_ == 0) {
    struct flock lock_data = {
        .l_type = F_UNLCK,
        .l_whence = SEEK_SET,
        .l_start = 0,
        .l_len = 0,
    };
    fcntl(fd_, F_SETLK, &lock_data);
  }

  mutex_.unlock_shared();
}

auto SharedFcntlMutex::fcntl_readers() -> std::size_t {
  std::unique_lock<std::mutex> fcntl_lock(fcntl_read_mutex_);
  return reader_counts_;
}

auto SharedFcntlMutex::other_locks(FcntlLockType lock_type)
    -> Result<SharedFcntlMutex::LockInfo> {
  struct flock lock_data = {
      .l_type = lock_type,
      .l_whence = SEEK_SET,
      .l_start = 0,
      .l_len = 0,
      .l_pid = getpid(),
  };

  if (fcntl(fd_, F_GETLK, &lock_data) == -1) {
    return Status::IOError("Fail to retrieve lock information for ", lock_filename_);
  }

  return SharedFcntlMutex::LockInfo{lock_data.l_type, lock_data.l_pid};
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
    if (reader_counts_++ > 0) return Status::OK();
  }

  // Repeatedly attempt acquisition of
  // the fcntl lock with exponential backoff.
  // A blocking F_SETLKW could be used but in practice
  // this can result in the kernel producing EDEADLK.
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> dist(-1e-4, 1e-4);
  std::chrono::duration<double> lock_sleep = 10ms;

  auto status = [&]() {
    while (true) {
      ARROW_ASSIGN_OR_RAISE(auto success, MaybeSetFcntlLock(fd_, write, lock_filename_));
      if (success) return Status::OK();
      auto jitter = std::chrono::duration<double>(dist(gen));
      std::this_thread::sleep_for(lock_sleep + jitter);
      if ((lock_sleep *= 1.5) > 200ms) lock_sleep = 200ms;
    }
  }();

  // Unlock if fcntl lock acquisition failed
  if (!status.ok()) {
    if (write) {
      mutex_.unlock();
    } else {
      // Covered by reader_lock
      reader_counts_--;
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
    if (reader_counts_++ > 0) return true;
  }

  auto result = MaybeSetFcntlLock(fd_, write, lock_filename_);

  // Unlock mutex if fcntl lock acquisition failed
  if (!result.ok() || result.ValueOrDie() == false) {
    if (write) {
      mutex_.unlock();
    } else {
      // Covered by reader_lock
      --reader_counts_;
      mutex_.unlock_shared();
    }
  }

  return result;
}

}  // namespace arcae
