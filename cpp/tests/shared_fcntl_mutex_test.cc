#include "arcae/shared_fcntl_mutex.h"

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <locale>
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
using arcae::SharedFcntlMutex;

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
  std::uniform_int_distribution<> dist(0, 15);

  std::stringstream ss;
  ss.imbue(std::locale::classic());
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
  PipeComms(PipeComms&& other) noexcept {
    for (std::size_t p = 0; p < NPIPES; ++p) {
      p_to_c[p] = std::exchange(other.p_to_c[p], -1);
      c_to_p[p] = std::exchange(other.c_to_p[p], -1);
    }
  }
  PipeComms& operator=(PipeComms&& other) noexcept {
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

  // Send a null terminated message in the current context
  arrow::Status send(std::string_view msg, ProcessContext context) {
    auto pipe = (context == PARENT ? p_to_c : c_to_p)[PipeEnd::WRITE];
    std::size_t written = 0;

    for (std::size_t written = 0; written < msg.size();) {
      auto n = write(pipe, msg.data() + written, msg.size() - written);
      if (n == -1)
        return Status::IOError("Write of ", msg.size(), " bytes failed due to ",
                               strerror(errno), " on pipe ", pipe);
      written += n;
    }

    const char nullchar = 0;

    if (write(pipe, &nullchar, sizeof(nullchar)) == -1)
      return Status::IOError("Write of ", msg.size(), " bytes failed due to ",
                             strerror(errno), " on pipe ", pipe);

    return Status::OK();
  }

  // Receive a message terminated with 0 in the current context
  arrow::Result<std::string> receive(ProcessContext context) {
    auto pipe = (context == PARENT ? c_to_p : p_to_c)[PipeEnd::READ];
    std::size_t bytes_read = 0;

    while (bytes_read < MSG_SIZE) {
      auto nread = read(pipe, buffer + bytes_read, MSG_SIZE - bytes_read);
      if (nread == -1)
        return Status::IOError("Read failed due to ", strerror(errno), " on pipe ", pipe);
      else if (nread == 0)
        break;
      bytes_read += nread;
      // exit if null terminated
      if (buffer[bytes_read - 1] == 0) break;
    }

    return std::string(buffer, buffer + bytes_read - 1);
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
// "read lock", "write lock", "query fcntl readers",
// "read lock thread 1", "write lock thread 2".
// Generally responds to the parent process with
// "ok", "fail" or some additional requested information
arrow::Status child_loop(PipeComms& pipe_comms, std::string_view lock_filename) {
  constexpr auto context = PipeComms::CHILD;
  ARROW_ASSIGN_OR_RAISE(auto lock, SharedFcntlMutex::Create(lock_filename));
  if (lock == nullptr) return Status::Invalid("Cast failed");
  if (!lock->has_fd()) return Status::Invalid("No disk lock");

  // Lambda handling individual messages
  // Messages are distinguished by their prefix.
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
      arcae::FcntlLockType requested_lock;
      if (req_lock_type_str.starts_with("none"))
        requested_lock = F_UNLCK;
      else if (req_lock_type_str.starts_with("read"))
        requested_lock = F_RDLCK;
      else if (req_lock_type_str.starts_with("write"))
        requested_lock = F_WRLCK;
      else
        return Status::IOError("unknown lock type ", req_lock_type_str);

      ARROW_ASSIGN_OR_RAISE(auto lock_info, lock->other_locks(requested_lock));
      auto [lock_type, pid] = lock_info;
      std::string response;
      if (lock_type == F_UNLCK)
        response = "none";
      else if (lock_type == F_RDLCK)
        response = "fail read " + std::to_string(pid);
      else if (lock_type == F_WRLCK)
        response = "fail write " + std::to_string(pid);
      else
        response = "unknown";

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
    using FType = decltype(f);
    int thread = [&]() -> int {
      if (auto p = msg.find("thread"); p != std::string_view::npos) {
        return std::stoi(std::string(msg.substr(p + strlen("thread"))));
      }
      return -1;
    }();

    // Execute functor on thread if requested, creating it if necessary
    if (thread >= 0) {
      ARROW_LOG(INFO) << "Executing " << msg << " on thread " << thread;
      auto pool_it = threads.find(thread);
      if (pool_it == threads.end()) {
        ARROW_ASSIGN_OR_RAISE(auto pool, ThreadPool::Make(1));
        pool_it = threads.emplace(thread, pool).first;
      }
      auto future = arrow::DeferNotOk(pool_it->second->Submit(std::forward<FType>(f)));
      return future.MoveResult();
    }

    // Otherwise call functor in the main thread
    return std::forward<FType>(f)();
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
      arrow::Status status;
      try {
        status = child_loop(comms[c], lock_name.native());
      } catch (std::exception& e) {
        status = Status::UnknownError(e.what());
      }

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

// Issue exit instructions to child processes
// and waits for them to shutdown
template <std::size_t CHILDREN>
arrow::Status ShutdownChildren(std::array<PipeComms, CHILDREN>& comms,
                               const std::array<pid_t, CHILDREN>& child_pid) {
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

// Test that acquiring read locks increases the number of fcntl readers
TEST(SharedFcntlMutexTest, FcntlReaders) {
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

// Test that IPC locking and lock observation behaves as expected
TEST(SharedFcntlMutexTest, InterProcessObservability) {
  std::array<PipeComms, NCHILDREN> comms;
  std::array<pid_t, NCHILDREN> child_pid;
  ASSERT_OK(SpawnChildren(comms, child_pid));

  // Acquire a write lock in the first process
  ASSERT_OK(comms[0].send(kWriteLock + " thread 1"s, PARENT_CONTEXT));
  ASSERT_OK(comms[0].expect("ok", PARENT_CONTEXT));

  // Failed to acquire a write lock in the same process and thread
  ASSERT_OK(comms[0].send(kTryWriteLock + " thread 1"s, PARENT_CONTEXT));
  ASSERT_OK(comms[0].expect("fail", PARENT_CONTEXT));

  // Failed to acquire a write lock in the same process and a different thread
  ASSERT_OK(comms[0].send(kTryWriteLock + " thread 2"s, PARENT_CONTEXT));
  ASSERT_OK(comms[0].expect("fail", PARENT_CONTEXT));

  // Failed to acquire a write lock in a different process
  ASSERT_OK(comms[1].send(kTryWriteLock + " thread 2"s, PARENT_CONTEXT));
  ASSERT_OK(comms[1].expect("fail", PARENT_CONTEXT));

  // Second process observes the first's write lock
  ASSERT_OK(comms[1].send(kRequestLock + " read"s, PARENT_CONTEXT));
  auto child_0_pid = std::to_string(child_pid[0]);
  ASSERT_OK(comms[1].expect("fail write " + child_0_pid, PARENT_CONTEXT));

  // Release write lock in the first process
  ASSERT_OK(comms[0].send(kWriteUnlock + " thread 2"s, PARENT_CONTEXT));
  ASSERT_OK(comms[0].expect("ok", PARENT_CONTEXT));

  // Second process observes no lock
  ASSERT_OK(comms[1].send(kRequestLock + " read"s, PARENT_CONTEXT));
  ASSERT_OK(comms[1].expect("none", PARENT_CONTEXT));

  // Second process acquires a read lock
  ASSERT_OK(comms[1].send(kReadLock + " thread 1"s, PARENT_CONTEXT));
  ASSERT_OK(comms[1].expect("ok", PARENT_CONTEXT));

  // First process observes no lock when requesting a read lock
  ASSERT_OK(comms[0].send(kRequestLock + " read"s, PARENT_CONTEXT));
  ASSERT_OK(comms[0].expect("none", PARENT_CONTEXT));

  // First process observes read lock when requesting a write lock
  ASSERT_OK(comms[0].send(kRequestLock + " write"s, PARENT_CONTEXT));
  auto child_1_pid = std::to_string(child_pid[1]);
  ASSERT_OK(comms[0].expect("fail read " + child_1_pid, PARENT_CONTEXT));

  ASSERT_OK(ShutdownChildren(comms, child_pid));
}

// Test readonly fallback in the case of a
// read-only table lock/directory
TEST(SharedFcntlMutexTest, ReadFallback) {
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

  ASSERT_OK_AND_ASSIGN(auto lock, SharedFcntlMutex::Create(lock_name.native()));
  ASSERT_FALSE(lock->has_fd());
  // Write locks fail but read locks succeed
  ASSERT_NOT_OK(lock->lock());
  ASSERT_OK(lock->lock_shared());
}

// Test that the SharedFcntlGuard behaves as expected
TEST(SharedFcntlMutexTest, LockGuard) {
  auto path = temp_directory_name();
  if (!fs::create_directory(path)) FAIL() << "Unable to create " << path.native();
  auto lock_name = path / "table.lock";

  ASSERT_OK_AND_ASSIGN(auto mutex, SharedFcntlMutex::Create(lock_name.native()));
  bool success;

  {
    // Read guard
    arcae::SharedFcntlGuard lock(*mutex, false);
    // Write locking fails
    Result<bool> result = Status::IOError("thread didn't join");
    std::thread([&]() { result = mutex->try_lock(); }).join();
    ASSERT_OK_AND_ASSIGN(auto success, result);
    ASSERT_FALSE(success);

    ASSERT_EQ(mutex->fcntl_readers(), 1);
  }

  // Can write lock
  ASSERT_OK_AND_ASSIGN(success, mutex->try_lock());
  ASSERT_TRUE(success);
  mutex->unlock();

  {
    // Write guard
    arcae::SharedFcntlGuard lock(*mutex, true);
    // Read locking fails
    Result<bool> result = Status::IOError("thread didn't join");
    std::thread([&]() { result = mutex->try_lock_shared(); }).join();
    ASSERT_OK_AND_ASSIGN(auto success, result);
    ASSERT_FALSE(success);
  }

  // Can read lock
  ASSERT_OK_AND_ASSIGN(success, mutex->try_lock_shared());
  ASSERT_TRUE(success);
  mutex->unlock();
}
}  // namespace
