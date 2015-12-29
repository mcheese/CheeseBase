cmake_minimum_required(VERSION 3.1)

set(COMMON
  common/common.h
  common/sync.h
  common/compiler.h
  common/structs.h
)

set(STORAGE
  storage/storage.h storage/storage.cc
  storage/cache.h storage/cache.cc
  storage/inc_prio_queue.h
)

set(BLOCKMAN
  blockman/allocator.h blockman/allocator.cc
  blockman/block_alloc.h blockman/block_alloc.cc
  blockman/block_locks.h blockman/block_locks.cc
)

set(MODEL
  model/model.h model/model.cc
  model/btree.h model/btree.cc
  model/parser.h
)

set(KEYCACHE
  keycache/keycache.h keycache/keycache.cc
  keycache/murmurhash3.h keycache/murmurhash3.cc
)

set(CORE
  core.h core.cc
  cheesebase.h cheesebase.cc
  cbindings.cc
)

source_group( src\\common   FILES ${COMMON})
source_group( src\\storage  FILES ${STORAGE})
source_group( src\\blockman FILES ${BLOCKMAN})
source_group( src\\model    FILES ${MODEL})
source_group( src\\keycache FILES ${KEYCACHE})
source_group( src           FILES ${CORE})

add_library(cheesebase ${CORE} ${KEYCACHE} ${COMMON} ${STORAGE} ${BLOCKMAN} ${MODEL})
target_link_libraries(cheesebase ${Boost_LIBRARIES})
