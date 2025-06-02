#include "arcae/rwlock.h"

#include <atomic>
#include <random>
#include <string>
#include <string_view>
#include "arrow/status.h"
#include "arrow/util/logging.h"

#include <fcntl.h>
#include <unistd.h>

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
  // result.push_back('-');

  // for (std::size_t i = 0; i < NHEXDIGITS; ++i) {
  //   result.push_back(hex[distribution(gen)]);
  // }

  return result;
}

RWLock::~RWLock() {
  if (fd_ != -1) close(fd_);
  mutex_.unlock();
  mutex_.unlock_shared();
}

arrow::Result<RWLock> RWLock::Create(std::string_view lock_filename, bool write) {
  std::string lock_name =
      lock_filename.size() == 0 ? make_lockname() : std::string(lock_filename);
  int fd;

  if (fd = open(lock_name.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR); fd == -1) {
    return arrow::Status::IOError("Creating lock file ", lock_name, " failed");
  }

  flock lock_data = {
      .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_start = 0, .l_len = 0,
      // .l_pid = getpid(),
  };

  if (fcntl(fd, F_GETLK, &lock_data) == -1) {
    return arrow::Status::IOError("Retrieving lock information for ", lock_name,
                                  " failed");
  };

  return RWLock(fd, lock_name, write);
}

arrow::Status RWLock::other_locks() {
  flock lock_data = {
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

}  // namespace arcae
