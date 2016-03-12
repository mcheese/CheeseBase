// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "common.h"
#include "sync.h"
#include <atomic>
#include <set>
#include <unordered_map>

namespace cheesebase {

struct BlockLock {
  enum class Writer { None, Waiting, Locked };
  size_t reader{ 0 };
  Writer writer{ Writer::None };
};

class BlockLocker;

class BlockLockSession {
  friend class BlockLocker;

public:
  BlockLockSession() = delete;
  ~BlockLockSession();

  void readLock(Addr block);
  void writeLock(Addr block);
  void unlock(Addr block);
  void unlockAll();

private:
  BlockLockSession(BlockLocker& locker) : locker_(locker) {}

  BlockLocker& locker_;
  std::set<Addr> read_locks_;
  std::set<Addr> write_locks_;
};

class BlockLocker {
  friend class BlockLockSession;

public:
  BlockLocker() = default;

  BlockLockSession startSession();

private:
  void readLock(Addr);
  void writeLock(Addr);
  void readUnlock(Addr);
  void writeUnlock(Addr);

  std::shared_ptr<BlockLock> getLock(Addr);

  Mutex mtx_;
  Cond cnd_;
  std::unordered_map<Addr, std::shared_ptr<BlockLock>> locks_;
};

} // namespace cheesebase
