// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "common.h"
#include "macros.h"
#include "sync.h"
#include <unordered_map>

namespace cheesebase {

class BlockLockPool;

template <class M, class L>
class BlockLock {
  friend BlockLockPool;

public:
  BlockLock() = default;
  MOVE_ONLY(BlockLock)

private:
  BlockLock(std::shared_ptr<M>&& mtx) : mtx_{ std::move(mtx) }, lck_{ *mtx_ } {}

  // Order is important here!
  // In destruction lck_ unlocks before mtx_ gets freed.
  std::shared_ptr<M> mtx_;
  L lck_;
};

using BlockLockR = BlockLock<RwMutex, ShLock<RwMutex>>;
using BlockLockW = BlockLock<RwMutex, ExLock<RwMutex>>;

class BlockLockPool {
public:
  BlockLockPool() = default;

  BlockLockR getLockR(Addr);
  BlockLockW getLockW(Addr);

private:
  std::shared_ptr<RwMutex> getMutex(Addr block);

  std::unordered_map<Addr, std::weak_ptr<RwMutex>> map_;
  Mutex mtx_;
};

} // namespace cheesebase
