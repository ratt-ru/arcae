#include "arcae/rwlock.h"

#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <map>
#include <memory>
#include <random>
#include <sstream>
#include <string>

#include <fcntl.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/util/future.h>
#include <arrow/util/thread_pool.h>

using namespace std::literals;

namespace fs = std::filesystem;
using arcae::RWLock;

namespace {

static constexpr char kExit[] = "exit";
static constexpr char kWriteLock[] = "write lock";
static constexpr char kReadLock[] = "read lock";
static constexpr char kTryWriteLock[] = "try write lock";
static constexpr char kTryReadLock[] = "try read lock";
static constexpr char kWriteUnlock[] = "write unlock";
static constexpr char kReadUnlock[] = "read unlock";
static constexpr char kRequestLock[] = "request lock";
static constexpr char kQueryFcntlReaders[] = "query fcntl readers";

static constexpr std::size_t NTASKS = 10;

// Generate a temporary directory name
fs::path temp_directory_name(std::size_t len = 8) {
  std::random_device dev;
  std::mt19937 prng(dev());
  std::uniform_int_distribution<> dist(0, 16);

  std::stringstream ss;
  ss << "temp-dir-";
  fs::path path = fs::current_path();

  for (std::size_t i = 0; i < len; ++i) {
    ss << std::hex << dist(prng);
  }

  return path / ss.str();
}

static constexpr std::size_t NCHILDREN = 2;

struct PipeComms {
  static constexpr std::size_t NPIPES = 2;
  static constexpr std::size_t MSG_SIZE = 1024;

  // Are we communicating via the parent or child process
  enum ProcessContext { PARENT = 0, CHILD = 1 };
  // Which end of the pipe are we interacting with?
  enum PipeEnd { READ = 0, WRITE = 1 };

  char buffer[MSG_SIZE];
  // parent to child descriptors
  int p_to_c[NPIPES] = {-1};
  // child to parent descriptors
  int c_to_p[NPIPES] = {-1};

  PipeComms() = default;
  PipeComms(const PipeComms&) = delete;
  PipeComms& operator=(const PipeComms& other) = delete;
  PipeComms(PipeComms&& other) {
    for (std::size_t p = 0; p < NPIPES; ++p) {
      p_to_c[p] = std::exchange(other.p_to_c[p], -1);
      c_to_p[p] = std::exchange(other.c_to_p[p], -1);
    }
  }
  PipeComms& operator=(PipeComms&& other) {
    for (std::size_t p = 0; p < NPIPES; ++p) {
      p_to_c[p] = std::exchange(other.p_to_c[p], -1);
      c_to_p[p] = std::exchange(other.c_to_p[p], -1);
    }
    return *this;
  }

  // Factory method creating a PipeComms object
  static arrow::Result<PipeComms> Create() {
    PipeComms result;
    if (pipe(result.p_to_c) == -1)
      return arrow::Status::IOError("Failed to create parent to child pipe");
    if (pipe(result.c_to_p) == -1)
      return arrow::Status::IOError("Failed to create child to parent pipe");
    return result;
  }

  // POSIX pipes are unidirectional
  // Close unused ends of the pipe within the current context
  void close_unused_ends(ProcessContext context) {
    auto p2c_ctx = context == PARENT ? 0 : 1;
    auto c2p_ctx = context == PARENT ? 1 : 0;
    close(p_to_c[p2c_ctx]);
    close(c_to_p[c2p_ctx]);
    p_to_c[p2c_ctx] = -1;
    c_to_p[c2p_ctx] = -1;
  }

  // Send message in the current context
  arrow::Status send(std::string_view msg, ProcessContext context) {
    auto n = MSG_SIZE < msg.size() ? MSG_SIZE : msg.size();
    auto pipe = (context == PARENT ? p_to_c : c_to_p)[PipeEnd::WRITE];
    std::copy_n(std::begin(msg), n, buffer);
    if (write(pipe, buffer, n) == -1) {
      return arrow::Status::IOError("Write of ", n, " bytes failed due to ",
                                    strerror(errno), " on pipe ", pipe);
    }
    return arrow::Status::OK();
  }

  // Receive a message in the current context
  arrow::Result<std::string> receive(ProcessContext context) {
    auto pipe = (context == PARENT ? c_to_p : p_to_c)[PipeEnd::READ];

    if (auto n = read(pipe, buffer, MSG_SIZE); n > 0) {
      return std::string(buffer, buffer + n);
    } else if (n < 0) {
      return arrow::Status::IOError("Read failed due to ", strerror(errno), " on pipe ",
                                    pipe);
    }

    return arrow::Status::Cancelled("Read failed due to EOF on pipe ", pipe);
  }

  // Expect a message in the current context
  arrow::Status expect(std::string_view expected, ProcessContext context) {
    ARROW_ASSIGN_OR_RAISE(auto msg, receive(context));
    if (msg == expected) return arrow::Status::OK();
    return arrow::Status::Invalid('\"', msg, "\" did not match \"", expected, '\"');
  }

  // Close any open pipe descriptors
  ~PipeComms() {
    for (std::size_t i = 0; i < NPIPES; ++i) {
      if (p_to_c[i] != -1) {
        close(p_to_c[i]);
        p_to_c[i] = -1;
      }
      if (c_to_p[i] != -1) {
        close(c_to_p[i]);
        c_to_p[i] = -1;
      }
    }
  }
};

// Child process loop
arrow::Status child_loop(PipeComms& pipe_comms, std::string_view lock_filename) {
  constexpr auto context = PipeComms::CHILD;
  auto threads = std::map<int, arrow::internal::ThreadPool>();
  ARROW_ASSIGN_OR_RAISE(auto lock, RWLock::Create(lock_filename));
  if (lock == nullptr) return arrow::Status::Invalid("Cast failed");
  if (!lock->has_fd()) return arrow::Status::Invalid("No disk lock");

  // auto MaybeRunInThread = [&](auto && f, auto && s, std::string_view msg) {
  //   int thread = -1;

  //   if (auto p = msg.find("thread"); p != std::string_view::npos) {
  //     p += strlen("thread");
  //     try {
  //       thread = std::stoi(std::string(msg.substr(p)));
  //     } catch(std::exception & e) {
  //       thread = -1;
  //     }
  //   }

  //   auto result = std::move(f)();

  // };

  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto msg, pipe_comms.receive(context));
    if (msg == kExit) {
      // Exit child loop
      ARROW_RETURN_NOT_OK(pipe_comms.send("ok", context));
      return arrow::Status::OK();
    } else if (msg == kWriteLock) {
      // Acquire write lock
      ARROW_RETURN_NOT_OK(lock->lock());
      ARROW_RETURN_NOT_OK(pipe_comms.send("ok", context));
    } else if (msg == kWriteUnlock) {
      // Release write lock
      lock->unlock();
      ARROW_RETURN_NOT_OK(pipe_comms.send("ok", context));
    } else if (msg == kReadLock) {
      // Acquire read lock
      ARROW_RETURN_NOT_OK(lock->lock_shared());
      ARROW_RETURN_NOT_OK(pipe_comms.send("ok", context));
    } else if (msg == kReadUnlock) {
      // Release read lock
      lock->unlock_shared();
      ARROW_RETURN_NOT_OK(pipe_comms.send("ok", context));
    } else if (msg == kTryWriteLock) {
      ARROW_ASSIGN_OR_RAISE(auto success, lock->try_lock());
      ARROW_RETURN_NOT_OK(pipe_comms.send(success ? "ok" : "fail", context));
    } else if (msg == kTryReadLock) {
      ARROW_ASSIGN_OR_RAISE(auto success, lock->try_lock_shared());
      ARROW_RETURN_NOT_OK(pipe_comms.send(success ? "ok" : "fail", context));
    } else if (msg == kQueryFcntlReaders) {
      // Query fcntl readers
      auto response = "ok " + std::to_string(lock->fcntl_readers());
      ARROW_RETURN_NOT_OK(pipe_comms.send(response, context));
    } else if (msg.starts_with(kRequestLock)) {
      // Query locks from other processes that interfere with the requested lock
      auto n = strlen(kRequestLock) + 1;
      auto req_lock_type_str = std::string_view(std::begin(msg) + n, std::end(msg));
      ARROW_ASSIGN_OR_RAISE(
          auto requested_lock, [&]() -> arrow::Result<decltype(F_UNLCK)> {
            if (req_lock_type_str == "none")
              return F_UNLCK;
            else if (req_lock_type_str == "read")
              return F_RDLCK;
            else if (req_lock_type_str == "write")
              return F_WRLCK;
            else
              return arrow::Status::IOError("unknown lock type ", req_lock_type_str);
          }());

      ARROW_ASSIGN_OR_RAISE(auto lock_info, lock->other_locks(requested_lock));
      auto [lock_type, pid] = lock_info;
      auto response = [&]() {
        switch (lock_type) {
          case F_UNLCK:
            return "ok";
          case F_RDLCK:
            return "fail read";
          case F_WRLCK:
            return "fail write";
          default:
            return "unknown";
        }
      }();
      ARROW_RETURN_NOT_OK(pipe_comms.send(response, context));

    } else {
      return arrow::Status::IOError("Unhandled message ", msg);
    }
  }
}

TEST(RWLockTest, Basic) {
  PipeComms comms[NCHILDREN];
  pid_t child_pid[NCHILDREN];

  for (std::size_t c = 0; c < NCHILDREN; ++c) {
    ASSERT_OK_AND_ASSIGN(comms[c], PipeComms::Create());
  }

  auto path = temp_directory_name();
  ASSERT_TRUE(fs::create_directory(path));
  auto lock_name = path / "table.lock";

  for (std::size_t c = 0; c < NCHILDREN; ++c) {
    if (child_pid[c] = fork(); child_pid[c] == 0) {
      // child process
      // Run child loop and exit the process appropriately
      comms[c].close_unused_ends(PipeComms::CHILD);
      auto status = child_loop(comms[c], lock_name.native());
      if (status.ok()) exit(EXIT_SUCCESS);
      ARROW_LOG(WARNING) << "Child " << c << " failed with " << status;
      exit(EXIT_FAILURE);
    } else if (child_pid[c] == -1) {
      FAIL() << "Child " << c << " forks failed";
    } else {
      // parent process
      // do some cleanup
      // and then fallback to the comms
      // pattern below
      comms[c].close_unused_ends(PipeComms::PARENT);
    }
  }

  constexpr auto context = PipeComms::PARENT;

  // Acquire a read lock in the first child
  ASSERT_OK(comms[0].send(kReadLock, context));
  ASSERT_OK(comms[0].expect("ok", context));

  ASSERT_OK(comms[0].send(kQueryFcntlReaders, context));
  ASSERT_OK(comms[0].expect("ok 1", context));

  ASSERT_OK(comms[0].send(kReadLock, context));
  ASSERT_OK(comms[0].expect("ok", context));

  ASSERT_OK(comms[0].send(kQueryFcntlReaders, context));
  ASSERT_OK(comms[0].expect("ok 2", context));

  ASSERT_OK(comms[0].send(kReadUnlock, context));
  ASSERT_OK(comms[0].expect("ok", context));

  ASSERT_OK(comms[0].send(kQueryFcntlReaders, context));
  ASSERT_OK(comms[0].expect("ok 1", context));

  // A read lock in another child process would not
  // be blocked by the read lock in the first child process
  ASSERT_OK(comms[0].send(kRequestLock + " read"s, context));
  ASSERT_OK(comms[0].expect("ok", context));

  // A write lock in another child would encounter a read lock
  // from the first child
  ASSERT_OK(comms[1].send(kRequestLock + " write"s, context));
  ASSERT_OK(comms[1].expect("fail read", context));

  ASSERT_OK(comms[0].send(kReadUnlock, context));
  ASSERT_OK(comms[0].expect("ok", context));

  ASSERT_OK(comms[0].send(kQueryFcntlReaders, context));
  ASSERT_OK(comms[0].expect("ok 0", context));

  ASSERT_OK(comms[0].send(kReadUnlock, context));
  ASSERT_OK(comms[0].expect("ok", context));

  ASSERT_OK(comms[0].send(kQueryFcntlReaders, context));
  ASSERT_OK(comms[0].expect("ok 0", context));

  // A write lock in another child would
  // not encounter conflicting locks
  ASSERT_OK(comms[1].send(kRequestLock + " write"s, context));
  ASSERT_OK(comms[1].expect("ok", context));

  // Acquire a write lock in second child
  ASSERT_OK(comms[1].send(kWriteLock, context));
  ASSERT_OK(comms[1].expect("ok", context));

  // Attempting a read lock in the first child
  // would encounter the write lock in the second
  ASSERT_OK(comms[0].send(kRequestLock + " read"s, context));
  ASSERT_OK(comms[0].expect("fail write", context));

  ASSERT_OK(comms[0].send(kTryReadLock, context));
  ASSERT_OK(comms[0].expect("fail", context));

  ASSERT_OK(comms[1].send(kWriteUnlock, context));
  ASSERT_OK(comms[1].expect("ok", context));

  ASSERT_OK(comms[0].send(kTryReadLock, context));
  ASSERT_OK(comms[0].expect("ok", context));

  ASSERT_OK(comms[0].send(kQueryFcntlReaders, context));
  ASSERT_OK(comms[0].expect("ok 1", context));

  for (std::size_t c = 0; c < NCHILDREN; ++c) {
    int status;
    ASSERT_OK(comms[c].send("exit", context));
    ASSERT_OK(comms[c].expect("ok", context));

    if (auto w_pid = waitpid(child_pid[c], &status, 0); w_pid == -1) {
      FAIL() << "Waiting for child failed";
    } else if (status != EXIT_SUCCESS) {
      FAIL() << "Child " << w_pid << " failed with exit code " << status;
    }
  }
}

TEST(RWLockTest, ReadFallback) {
  using fs::perm_options;
  using fs::perms;

  auto tmp_dir = temp_directory_name();

  // Cleanup test directory
  std::shared_ptr<void> defer(nullptr, [&](...) {
    std::filesystem::permissions(tmp_dir,
                                 perms::owner_all | perms::group_all | perms::others_all,
                                 perm_options::add);

    fs::remove_all(tmp_dir);
  });

  ASSERT_TRUE(fs::create_directory(tmp_dir));
  std::filesystem::permissions(tmp_dir,
                               perms::owner_all | perms::group_all | perms::others_all,
                               perm_options::remove);

  auto lock_name = tmp_dir / "arcae.table.lock";

  ASSERT_OK_AND_ASSIGN(auto lock, RWLock::Create(lock_name.native()));
  ASSERT_FALSE(lock->has_fd());
  arcae::RWLockGuard guard(*lock);
  ASSERT_OK(lock->lock_shared());
}

}  // namespace
