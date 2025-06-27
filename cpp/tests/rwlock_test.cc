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
#include <sched.h>
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

using arrow::Result;
using arrow::Status;
using arrow::internal::ThreadPool;

namespace {

// Instructions to the child process control loop
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

// Helper class for managing pipe interactions between
// a parent and a child process
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
      return Status::IOError("Failed to create parent to child pipe");
    if (pipe(result.c_to_p) == -1)
      return Status::IOError("Failed to create child to parent pipe");
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
      return Status::IOError("Write of ", n, " bytes failed due to ", strerror(errno),
                             " on pipe ", pipe);
    }
    return Status::OK();
  }

  // Receive a message in the current context
  arrow::Result<std::string> receive(ProcessContext context) {
    auto pipe = (context == PARENT ? c_to_p : p_to_c)[PipeEnd::READ];

    if (auto n = read(pipe, buffer, MSG_SIZE); n > 0) {
      return std::string(buffer, buffer + n);
    } else if (n < 0) {
      return Status::IOError("Read failed due to ", strerror(errno), " on pipe ", pipe);
    }

    return Status::Cancelled("Read failed due to EOF on pipe ", pipe);
  }

  // Expect a message in the current context
  arrow::Status expect(std::string_view expected, ProcessContext context) {
    ARROW_ASSIGN_OR_RAISE(auto msg, receive(context));
    if (msg == expected) return Status::OK();
    return Status::Invalid('\"', msg, "\" did not match \"", expected, '\"');
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
// Waits for instructions from the parent process such as
// "read lock", "write lock", "query fctnl readers",
// "read lock thread 1", "write lock thread 2".
// Generally responds to the parent process with
// "ok", "fail" or some additional requested information
arrow::Status child_loop(PipeComms& pipe_comms, std::string_view lock_filename) {
  constexpr auto context = PipeComms::CHILD;
  ARROW_ASSIGN_OR_RAISE(auto lock, RWLock::Create(lock_filename));
  if (lock == nullptr) return Status::Invalid("Cast failed");
  if (!lock->has_fd()) return Status::Invalid("No disk lock");

  // Lambda handling individual messages
  // Messages are distinguished by their suffix.
  // i.e "write lock" or "query fctnl readers"
  auto HandleMsg = [&](std::string_view msg) -> Result<bool> {
    if (msg == kExit) {
      // Exit child loop
      ARROW_RETURN_NOT_OK(pipe_comms.send("ok", context));
      return false;
    } else if (msg.starts_with(kWriteLock)) {
      // Acquire write lock
      ARROW_RETURN_NOT_OK(lock->lock());
      ARROW_RETURN_NOT_OK(pipe_comms.send("ok", context));
    } else if (msg.starts_with(kWriteUnlock)) {
      // Release write lock
      lock->unlock();
      ARROW_RETURN_NOT_OK(pipe_comms.send("ok", context));
    } else if (msg.starts_with(kReadLock)) {
      // Acquire read lock
      ARROW_RETURN_NOT_OK(lock->lock_shared());
      ARROW_RETURN_NOT_OK(pipe_comms.send("ok", context));
    } else if (msg.starts_with(kReadUnlock)) {
      // Release read lock
      lock->unlock_shared();
      ARROW_RETURN_NOT_OK(pipe_comms.send("ok", context));
    } else if (msg.starts_with(kTryWriteLock)) {
      ARROW_ASSIGN_OR_RAISE(auto success, lock->try_lock());
      ARROW_RETURN_NOT_OK(pipe_comms.send(success ? "ok" : "fail", context));
    } else if (msg.starts_with(kTryReadLock)) {
      ARROW_ASSIGN_OR_RAISE(auto success, lock->try_lock_shared());
      ARROW_RETURN_NOT_OK(pipe_comms.send(success ? "ok" : "fail", context));
    } else if (msg.starts_with(kQueryFcntlReaders)) {
      // Query fcntl readers
      auto response = "ok " + std::to_string(lock->fcntl_readers());
      ARROW_RETURN_NOT_OK(pipe_comms.send(response, context));
    } else if (msg.starts_with(kRequestLock)) {
      // Query locks from other processes that interfere with the requested lock
      auto n = strlen(kRequestLock) + 1;
      auto req_lock_type_str = std::string_view(std::begin(msg) + n, std::end(msg));
      ARROW_ASSIGN_OR_RAISE(
          auto requested_lock, [&]() -> arrow::Result<decltype(F_UNLCK)> {
            if (req_lock_type_str.starts_with("none"))
              return F_UNLCK;
            else if (req_lock_type_str.starts_with("read"))
              return F_RDLCK;
            else if (req_lock_type_str.starts_with("write"))
              return F_WRLCK;
            else
              return Status::IOError("unknown lock type ", req_lock_type_str);
          }());

      ARROW_ASSIGN_OR_RAISE(auto lock_info, lock->other_locks(requested_lock));
      auto [lock_type, pid] = lock_info;
      auto response = [&]() -> std::string {
        switch (lock_type) {
          case F_UNLCK:
            return "none";
          case F_RDLCK:
            return "fail read " + std::to_string(pid);
          case F_WRLCK:
            return "fail write " + std::to_string(pid);
          default:
            return "unknown";
        }
      }();
      ARROW_RETURN_NOT_OK(pipe_comms.send(response, context));
    } else {
      return Status::IOError("Unhandled message ", msg);
    }

    return true;
  };

  // Single thread ThreadPools
  auto threads = std::map<int, std::shared_ptr<ThreadPool>>();

  // Possibly execute the supplied functor in a ThreadPool,
  // depending on the presence of constructs like "thread 1"
  // or "thread 2" in the message suffix
  auto MaybeRunInThread = [&](std::string_view msg, auto&& f) -> arrow::Result<bool> {
    int thread = [&]() -> int {
      if (auto p = msg.find("thread"); p != std::string_view::npos) {
        try {
          return std::stoi(std::string(msg.substr(p + strlen("thread"))));
        } catch (std::exception& e) {
        }
      }
      return -1;
    }();

    // Execute functor on thread if requested, creating it if necessary
    if (thread >= 0) {
      ARROW_LOG(INFO) << "Executing " << msg << " on thread " << thread;
      decltype(threads.begin()) pool_it;
      if (pool_it = threads.find(thread); pool_it == threads.end()) {
        ARROW_ASSIGN_OR_RAISE(auto pool, ThreadPool::Make(1));
        pool_it = threads.insert({thread, pool}).first;
      }
      auto future = arrow::DeferNotOk(pool_it->second->Submit(std::move(f)));
      return future.MoveResult();
    }

    // Otherwise call functor in the main thread
    return std::move(f)();
  };

  // Run the message loop until exit (or error)
  for (auto loop = true; loop;) {
    ARROW_ASSIGN_OR_RAISE(auto msg, pipe_comms.receive(context));
    ARROW_ASSIGN_OR_RAISE(loop, MaybeRunInThread(msg, [&]() { return HandleMsg(msg); }));
  }

  return Status::OK();
}

// Spawns some children running child_loop
// and returns control to the parent process
template <std::size_t CHILDREN>
arrow::Status SpawnChildren(std::array<PipeComms, CHILDREN>& comms,
                            std::array<pid_t, CHILDREN>& child_pid) {
  for (std::size_t c = 0; c < CHILDREN; ++c) {
    ARROW_ASSIGN_OR_RAISE(comms[c], PipeComms::Create());
  }

  auto path = temp_directory_name();
  if (!fs::create_directory(path))
    return Status::IOError("Unable to create ", path.native());
  auto lock_name = path / "table.lock";

  for (std::size_t c = 0; c < CHILDREN; ++c) {
    if (child_pid[c] = fork(); child_pid[c] == 0) {
      // child process
      // run child loop and exit appropriately
      comms[c].close_unused_ends(PipeComms::CHILD);
      auto status = [&]() {
        try {
          return child_loop(comms[c], lock_name.native());
        } catch (std::exception& e) {
          return Status::UnknownError(e.what());
        }
      }();

      if (status.ok()) exit(EXIT_SUCCESS);
      ARROW_LOG(WARNING) << "Child " << c << " failed: " << status;
      exit(EXIT_FAILURE);
    } else if (child_pid[c] == -1) {
      return Status::IOError("Fork of Child ", c, " failed");
    }
    // parent process. do pipe end cleanup
    comms[c].close_unused_ends(PipeComms::PARENT);
  }

  return Status::OK();
}

// Issues exit instructions to the child process
// and waits for it to shutdown
template <std::size_t CHILDREN>
arrow::Status ShutdownChildren(std::array<PipeComms, CHILDREN>& comms,
                               std::array<pid_t, CHILDREN>& child_pid) {
  for (std::size_t c = 0; c < NCHILDREN; ++c) {
    int status;
    ARROW_RETURN_NOT_OK(comms[c].send("exit", PipeComms::PARENT));
    ARROW_RETURN_NOT_OK(comms[c].expect("ok", PipeComms::PARENT));

    if (auto w_pid = waitpid(child_pid[c], &status, 0); w_pid == -1) {
      return Status::IOError("Waiting for child ", c, " failed");
    } else if (status != EXIT_SUCCESS) {
      return Status::IOError("Child ", c, " failed with exit code", status);
    }
  }

  return Status::OK();
}

static constexpr auto PARENT_CONTEXT = PipeComms::PARENT;

TEST(RWLockTest, FcntlReaders) {
  std::array<PipeComms, NCHILDREN> comms;
  std::array<pid_t, NCHILDREN> child_pid;

  ASSERT_OK(SpawnChildren(comms, child_pid));
  constexpr std::size_t NREADLOCKS = 3;

  // Base case for no locks
  ASSERT_OK(comms[0].send(kQueryFcntlReaders, PARENT_CONTEXT));
  ASSERT_OK(comms[0].expect("ok 0", PARENT_CONTEXT));

  for (std::size_t i = 0; i < NREADLOCKS; ++i) {
    auto thread = std::to_string(i);

    // Acquire a read lock in the first child
    ASSERT_OK(comms[0].send(kReadLock + " thread "s + thread, PARENT_CONTEXT));
    ASSERT_OK(comms[0].expect("ok", PARENT_CONTEXT));

    // Confirm number of readers
    ASSERT_OK(comms[0].send(kQueryFcntlReaders + " thread "s + std::to_string(i),
                            PARENT_CONTEXT));
    ASSERT_OK(comms[0].expect("ok " + std::to_string(i + 1), PARENT_CONTEXT));
  }

  for (std::size_t i = 0; i < NREADLOCKS; ++i) {
    auto thread = std::to_string(i);
    // Release a read lock in the first child
    ASSERT_OK(comms[0].send(kReadUnlock + " thread "s + thread, PARENT_CONTEXT));
    ASSERT_OK(comms[0].expect("ok", PARENT_CONTEXT));

    // Confirm number of readers
    ASSERT_OK(comms[0].send(kQueryFcntlReaders + " thread "s + thread, PARENT_CONTEXT));
    ASSERT_OK(
        comms[0].expect("ok " + std::to_string(NREADLOCKS - i - 1), PARENT_CONTEXT));
  }

  ASSERT_OK(ShutdownChildren(comms, child_pid));
}

TEST(RWLockTest, InterProcessObservability) {
  std::array<PipeComms, NCHILDREN> comms;
  std::array<pid_t, NCHILDREN> child_pid;

  ASSERT_OK(SpawnChildren(comms, child_pid));

  // Acquire a write lock in the first child
  ASSERT_OK(comms[0].send(kWriteLock + "thread 1"s, PARENT_CONTEXT));
  ASSERT_OK(comms[0].expect("ok", PARENT_CONTEXT));

  // Failed to acquire a write lock in the same process and thread
  ASSERT_OK(comms[0].send(kTryWriteLock + "thread 1"s, PARENT_CONTEXT));
  ASSERT_OK(comms[0].expect("fail", PARENT_CONTEXT));

  // Failed to acquire a write lock in the same process and a different thread
  ASSERT_OK(comms[0].send(kTryWriteLock + "thread 2"s, PARENT_CONTEXT));
  ASSERT_OK(comms[0].expect("fail", PARENT_CONTEXT));

  // Failed to acquire a write lock in a different process
  ASSERT_OK(comms[1].send(kTryWriteLock + "thread 2"s, PARENT_CONTEXT));
  ASSERT_OK(comms[1].expect("fail", PARENT_CONTEXT));

  // Second child observes the first child's write lock
  ASSERT_OK(comms[1].send(kRequestLock + " read"s, PARENT_CONTEXT));
  ASSERT_OK(
      comms[1].expect("fail write " + std::to_string(child_pid[0]), PARENT_CONTEXT));

  // Release the write lock in the first child
  ASSERT_OK(comms[0].send(kWriteUnlock + "thread 2"s, PARENT_CONTEXT));
  ASSERT_OK(comms[0].expect("ok", PARENT_CONTEXT));

  // Second child observes no lock
  ASSERT_OK(comms[1].send(kRequestLock + " read"s, PARENT_CONTEXT));
  ASSERT_OK(comms[1].expect("none", PARENT_CONTEXT));

  // Second child acquires a read lock
  ASSERT_OK(comms[1].send(kReadLock + " thread 1"s, PARENT_CONTEXT));
  ASSERT_OK(comms[1].expect("ok", PARENT_CONTEXT));

  // First child observes no lock in response to a read lock request
  ASSERT_OK(comms[0].send(kRequestLock + " read"s, PARENT_CONTEXT));
  ASSERT_OK(comms[0].expect("none", PARENT_CONTEXT));

  // First child observes read lock in response to a write lock request
  ASSERT_OK(comms[0].send(kRequestLock + " read"s, PARENT_CONTEXT));
  ASSERT_OK(comms[0].expect("none", PARENT_CONTEXT));

  ASSERT_OK(ShutdownChildren(comms, child_pid));
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
