// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "common/common.h"
#include "common/sync.h"
#include <atomic>
#include <set>
#include <unordered_map>

namespace cheesebase {

DEF_EXCEPTION(BlockLockError);

struct BlockLock {
  enum class Writer { None, Waiting, Locked };
  size_t reader{ 0 };
  Writer writer{ Writer::None };
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

  Mutex m_mtx;
  Cond m_cnd;
  std::unordered_map<Addr, std::shared_ptr<BlockLock>> m_locks;
};

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
  BlockLockSession(BlockLocker& locker) : m_locker(locker) {}

  BlockLocker& m_locker;
  std::set<Addr> m_read_locks;
  std::set<Addr> m_write_locks;
};

} // namespace cheesebase
