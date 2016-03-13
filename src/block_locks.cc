// Licensed under the Apache License 2.0 (see LICENSE file).

#include "block_locks.h"

namespace cheesebase {

std::shared_ptr<RwMutex> BlockLockPool::getMutex(Addr block) {
  Guard<Mutex> guard{ mtx_ };

  std::shared_ptr<RwMutex> mutex;

  auto lookup = map_.find(block);
  if (lookup == map_.end()) {
    // not existing
    mutex = std::make_shared<RwMutex>();
    map_.emplace_hint(lookup, block, mutex);
  } else {
    mutex = lookup->second.lock();
    if (!mutex) {
      mutex = std::make_shared<RwMutex>();
      lookup->second = mutex;
    }
  }
  return mutex;
}

BlockLockR BlockLockPool::getLockR(Addr block) {
  return { getMutex(block) };
}

BlockLockW BlockLockPool::getLockW(Addr block) {
  return { getMutex(block) };
}

} // namespace cheesebase

