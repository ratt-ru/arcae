#include "arcae/rwlock.h"

#include <cassert>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <utility>

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

namespace {

static constexpr char hex[] = "0123456789abcdef";
static constexpr std::size_t NHEXDIGITS = 8;

}  // namespace

std::string RWLock::make_lockname(std::string_view prefix) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distribution(0, 15);

  std::string result;
  result.reserve(prefix.size() + 1 + NHEXDIGITS);
  result.append(prefix);
  result.push_back('-');

  for (std::size_t i = 0; i < NHEXDIGITS; ++i) {
    result.push_back(hex[distribution(gen)]);
  }

  return result;
}

RWLock::RWLock(RWLock&& rhs)
    : fd_(std::exchange(rhs.fd_, -1)),
      fcntl_readers_(std::exchange(rhs.fcntl_readers_, 0)),
      lock_filename_(std::move(rhs.lock_filename_)),
      mutex_() {
  rhs.mutex_.unlock();
  rhs.mutex_.unlock_shared();
}

RWLock& RWLock::operator=(RWLock&& rhs) {
  using std::swap;
  if (this != &rhs) {
    std::swap(fd_, rhs.fd_);
    std::swap(fcntl_readers_, rhs.fcntl_readers_);
    std::swap(lock_filename_, rhs.lock_filename_);
    rhs.mutex_.unlock();
    rhs.mutex_.unlock_shared();
  }
  return *this;
}

RWLock::~RWLock() {
  if (fd_ != -1) close(fd_);
  mutex_.unlock();
  mutex_.unlock_shared();
  // fcntl_readers_ = 0;
  lock_filename_.clear();
}

void RWLock::unlock() {
  struct flock lock_data = {
      .l_type = F_UNLCK,
      .l_whence = SEEK_SET,
      .l_start = 0,
      .l_len = 0,
  };

  assert(fd_ != -1);
  fcntl(fd_, F_SETLK, &lock_data);
  mutex_.unlock();
}

void RWLock::unlock_shared() {
  std::unique_lock<std::mutex> fnctl_lock(fcntl_mutex_);
  assert(fd_ != -1);
  // Last reader releases the fcntl lock
  if (--fcntl_readers_ == 0) {
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

arrow::Result<std::shared_ptr<RWLock>> RWLock::Create(std::string_view lock_filename,
                                                      bool write) {
  std::string lock_name = make_lockname(lock_filename);
  // lock_filename.size() == 0 ? make_lockname() : std::string(lock_filename);
  int fd = 0;

  if (fd = open(lock_name.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR); fd == -1) {
    return arrow::Status::IOError("Creating lock file ", lock_name, " failed");
  }

  struct flock lock_data = {
      .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_start = 0, .l_len = 0,
      // .l_pid = getpid(),
  };

  if (fcntl(fd, F_GETLK, &lock_data) == -1) {
    return arrow::Status::IOError("Retrieving lock information for ", lock_name,
                                  " failed");
  };

  struct enable_make_shared : public RWLock {
   public:
    using RWLock::RWLock;
  };
  return std::make_shared<enable_make_shared>(fd, std::string_view(lock_name), write);
}

arrow::Status RWLock::other_locks() {
  struct flock lock_data = {
      .l_type = F_WRLCK,
      .l_whence = SEEK_SET,
      .l_start = 0,
      .l_len = 0,
      .l_pid = getpid(),
  };

  if (fcntl(fd_, F_GETLK, &lock_data) == -1) {
    return arrow::Status::IOError("Retrieving lock information for ", lock_filename_,
                                  " failed");
  }

  ARROW_LOG(INFO) << "show_lock pid " << lock_data.l_pid << " l_type "
                  << (lock_data.l_type == F_RDLCK   ? "Read"
                      : lock_data.l_type == F_WRLCK ? "Write"
                      : lock_data.l_type == F_UNLCK ? "Unlocked"
                                                    : "Unknown")
                  << " l_start " << lock_data.l_start << " l_len " << lock_data.l_len;

  return arrow::Status::OK();
}

arrow::Status RWLock::lock_impl(bool write) {
  const std::string_view lock_type(write ? "write" : "read");
  write ? mutex_.lock() : mutex_.lock_shared();
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> distribution(-1e-4, 1e-4);
  std::chrono::duration<double> lock_sleep = 10ms;

  // Repeatedly attempt acquisition of
  // the fcntl lock with exponential backoff.
  // A blocking F_SETLKW could be used but in practice
  // one ends up having to handle EDEADLK.
  auto status = [&]() {
    while (true) {
      using LockType = decltype(flock::l_type);
      assert(fd_ != -1);
      struct flock lock_data = {
          .l_type = LockType(write ? F_WRLCK : F_RDLCK),
          .l_whence = SEEK_SET,
          .l_start = 0,
          .l_len = 0,
          //.l_pid = getpid(),
      };

      // Indicate success if file-locking succeeds
      assert(fd_ != -1);
      if (fcntl(fd_, F_SETLK, &lock_data) == 0) return arrow::Status::OK();

      switch (errno) {
        case EAGAIN:
          break;
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

      std::this_thread::sleep_for(lock_sleep);
      if (lock_sleep * 1.5 < 1s) {
        lock_sleep *= 1.5;
        lock_sleep += std::chrono::duration<double>(distribution(gen));
      }
    }
  }();

  // Unlock if fcntl lock acquisition failed
  if (!status.ok()) write ? mutex_.unlock() : mutex_.unlock_shared();
  return status;
}

arrow::Status RWLock::try_lock_impl(bool write) {
  std::string_view lock_type = write ? "write" : "read";
  if (!(write ? mutex_.try_lock() : mutex_.try_lock_shared())) {
    return arrow::Status::Cancelled("Acquisition of ", lock_type, " lock failed");
  }

  constexpr auto lock_sleep = 100ms;
  auto status = [&]() {
    while (true) {
      using LockType = decltype(flock::l_type);
      struct flock lock_data = {
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

      std::this_thread::sleep_for(lock_sleep);
    }
  }();

  // Unlock if fcntl lock acquisition failed
  if (!status.ok()) write ? mutex_.unlock() : mutex_.unlock_shared();
  return status;
}

}  // namespace arcae
