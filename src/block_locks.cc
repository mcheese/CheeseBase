// Licensed under the Apache License 2.0 (see LICENSE file).

#include "block_locks.h"

namespace cheesebase {

BlockLockSession::~BlockLockSession() { unlockAll(); }

void BlockLockSession::readLock(Addr block) {
  auto lookup = read_locks_.find(block);

  if (lookup != read_locks_.end())
    throw BlockLockError("read requested lock already locked");
  if (write_locks_.count(block) > 0)
    throw BlockLockError("read requested lock already write locked");

  locker_.readLock(block);
  read_locks_.emplace_hint(lookup, block);
}

void BlockLockSession::writeLock(Addr block) {
  auto lookup = write_locks_.find(block);

  if (lookup != write_locks_.end())
    throw BlockLockError("write requested lock already locked");
  if (read_locks_.count(block) > 0)
    throw BlockLockError("write requested lock already write locked");

  locker_.writeLock(block);
  write_locks_.emplace_hint(lookup, block);
}

void BlockLockSession::unlock(Addr block) {
  if (read_locks_.count(block) > 0) return locker_.readUnlock(block);
  if (write_locks_.count(block) > 0) return locker_.writeUnlock(block);
  throw BlockLockError("unlock requested lock not locked");
}

void BlockLockSession::unlockAll() {
  for (auto a : read_locks_) locker_.readUnlock(a);
  for (auto a : write_locks_) locker_.writeUnlock(a);
  read_locks_.clear();
  write_locks_.clear();
}

BlockLockSession BlockLocker::startSession() { return BlockLockSession(*this); }

std::shared_ptr<BlockLock> BlockLocker::getLock(Addr block) {
  std::shared_ptr<BlockLock> block_lock;

  auto lookup = locks_.find(block);
  if (lookup == locks_.end()) {
    auto emplace =
        locks_.emplace_hint(lookup, block, std::make_shared<BlockLock>());
    block_lock = emplace->second;
  } else { block_lock = lookup->second; }

  Ensures(block_lock.use_count() >= 2);

  return block_lock;
}

void BlockLocker::readLock(Addr block) {
  ExLock<Mutex> lck{ mtx_ };

  auto block_lock = getLock(block);

  while (block_lock->writer != BlockLock::Writer::None) cnd_.wait(lck);
  ++(block_lock->reader);
}

void BlockLocker::writeLock(Addr block) {
  ExLock<Mutex> lck{ mtx_ };

  auto block_lock = getLock(block);

  while (block_lock->writer != BlockLock::Writer::None) cnd_.wait(lck);
  block_lock->writer = BlockLock::Writer::Waiting;
  while (block_lock->reader > 0) cnd_.wait(lck);
  block_lock->writer = BlockLock::Writer::Locked;
}

void BlockLocker::readUnlock(Addr block) {
  ExLock<Mutex> lck{ mtx_ };
  auto lookup = locks_.find(block);

  auto block_lock = getLock(block);

  Expects(block_lock.use_count() >= 2); // me and map
  Expects(block_lock->reader > 0);

  --(block_lock->reader);

  // check if I am the only one caring about this lock
  if (block_lock.use_count() == 2) {
    if (block_lock->reader == 0 &&
        block_lock->writer == BlockLock::Writer::None) {
      locks_.erase(block);
    }
  } else { cnd_.notify_all(); }
}

void BlockLocker::writeUnlock(Addr block) {
  ExLock<Mutex> lck{ mtx_ };

  auto block_lock = getLock(block);

  Expects(block_lock.use_count() >= 2); // me and map
  Expects(block_lock->reader == 0);
  Expects(block_lock->writer == BlockLock::Writer::Locked);

  block_lock->writer = BlockLock::Writer::None;

  // check if I am the only one caring about this lock
  if (block_lock.use_count() == 2) {
    if (block_lock->reader == 0 &&
        block_lock->writer == BlockLock::Writer::None) {
      locks_.erase(block);
    }
  } else { cnd_.notify_all(); }
}

} // namespace cheesebase
