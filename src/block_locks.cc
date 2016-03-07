// Licensed under the Apache License 2.0 (see LICENSE file).

#include "block_locks.h"

namespace cheesebase {

BlockLockSession::~BlockLockSession() { unlockAll(); }

void BlockLockSession::readLock(Addr block) {
  auto lookup = m_read_locks.find(block);

  if (lookup != m_read_locks.end())
    throw BlockLockError("read requested lock already locked");
  if (m_write_locks.count(block) > 0)
    throw BlockLockError("read requested lock already write locked");

  m_locker.readLock(block);
  m_read_locks.emplace_hint(lookup, block);
}

void BlockLockSession::writeLock(Addr block) {
  auto lookup = m_write_locks.find(block);

  if (lookup != m_write_locks.end())
    throw BlockLockError("write requested lock already locked");
  if (m_read_locks.count(block) > 0)
    throw BlockLockError("write requested lock already write locked");

  m_locker.writeLock(block);
  m_write_locks.emplace_hint(lookup, block);
}

void BlockLockSession::unlock(Addr block) {
  if (m_read_locks.count(block) > 0) return m_locker.readUnlock(block);
  if (m_write_locks.count(block) > 0) return m_locker.writeUnlock(block);
  throw BlockLockError("unlock requested lock not locked");
}

void BlockLockSession::unlockAll() {
  for (auto a : m_read_locks) m_locker.readUnlock(a);
  for (auto a : m_write_locks) m_locker.writeUnlock(a);
  m_read_locks.clear();
  m_write_locks.clear();
}

BlockLockSession BlockLocker::startSession() { return BlockLockSession(*this); }

std::shared_ptr<BlockLock> BlockLocker::getLock(Addr block) {
  std::shared_ptr<BlockLock> block_lock;

  auto lookup = m_locks.find(block);
  if (lookup == m_locks.end()) {
    auto emplace =
        m_locks.emplace_hint(lookup, block, std::make_shared<BlockLock>());
    block_lock = emplace->second;
  } else {
    block_lock = lookup->second;
  }

  Ensures(block_lock.use_count() >= 2);

  return block_lock;
}

void BlockLocker::readLock(Addr block) {
  ExLock<Mutex> lck{ m_mtx };

  auto block_lock = getLock(block);

  while (block_lock->writer != BlockLock::Writer::None) m_cnd.wait(lck);
  ++(block_lock->reader);
}

void BlockLocker::writeLock(Addr block) {
  ExLock<Mutex> lck{ m_mtx };

  auto block_lock = getLock(block);

  while (block_lock->writer != BlockLock::Writer::None) m_cnd.wait(lck);
  block_lock->writer = BlockLock::Writer::Waiting;
  while (block_lock->reader > 0) m_cnd.wait(lck);
  block_lock->writer = BlockLock::Writer::Locked;
}

void BlockLocker::readUnlock(Addr block) {
  ExLock<Mutex> lck{ m_mtx };
  auto lookup = m_locks.find(block);

  auto block_lock = getLock(block);

  Expects(block_lock.use_count() >= 2); // me and map
  Expects(block_lock->reader > 0);

  --(block_lock->reader);

  // check if I am the only one caring about this lock
  if (block_lock.use_count() == 2) {
    if (block_lock->reader == 0 &&
        block_lock->writer == BlockLock::Writer::None) {
      m_locks.erase(block);
    }
  } else {
    m_cnd.notify_all();
  }
}

void BlockLocker::writeUnlock(Addr block) {
  ExLock<Mutex> lck{ m_mtx };

  auto block_lock = getLock(block);

  Expects(block_lock.use_count() >= 2); // me and map
  Expects(block_lock->reader == 0);
  Expects(block_lock->writer == BlockLock::Writer::Locked);

  block_lock->writer = BlockLock::Writer::None;

  // check if I am the only one caring about this lock
  if (block_lock.use_count() == 2) {
    if (block_lock->reader == 0 &&
        block_lock->writer == BlockLock::Writer::None) {
      m_locks.erase(block);
    }
  } else {
    m_cnd.notify_all();
  }
}

} // namespace cheesebase
