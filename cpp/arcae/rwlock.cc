#include "arcae/rwlock.h"

#include <iostream>
#include <random>
#include <string>
#include <string_view>

#include <fcntl.h>

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

RWLock RWLock::Create(std::string_view lock_filename, bool write) {
  std::string lock_name =
      lock_filename.size() == 0 ? make_lockname() : std::string(lock_filename);
  int fd;

  if (fd = open(lock_name.c_str(), O_CREAT | O_RDWR); fd == -1) {
    std::cout << "Creation of " << lock_name << " failed" << std::endl;
  }

  std::cout << lock_name << " created." << std::endl;

  flock lock_data = {
      .l_type = F_WRLCK,
      .l_whence = SEEK_SET,
      .l_start = 0,
      .l_len = 100,
  };
  if (fcntl(fd, F_GETLK, &lock_data) == -1) {
    std::cout << "Unable to get lock data for " << lock_name << std::endl;
  };
  std::cout << "pid" << lock_data.l_pid << " l_type "
            << (lock_data.l_type == F_RDLCK   ? "Read"
                : lock_data.l_type == F_WRLCK ? "Write"
                : lock_data.l_type == F_UNLCK ? "Unlocked"
                                              : "Unknown")
            << " l_start " << lock_data.l_start << " l_len " << lock_data.l_len
            << std::endl;

  return RWLock{.lock_filename_ = lock_name};
}

}  // namespace arcae
