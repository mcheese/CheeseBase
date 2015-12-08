cmake_minimum_required(VERSION 3.1)

set(COMMON
  common/common.h
  common/sync.h
  common/compiler.h
)

set(STORAGE
  storage/storage.h storage/storage.cc
  storage/fileio.h storage/fileio.cc
  storage/cache.h storage/cache.cc
  storage/inc_prio_queue.h
  storage/disk_worker.h storage/disk_worker.cc
)

set(BLOCKMAN
  blockman/allocator.h blockman/allocator.cc
  blockman/block_alloc.h blockman/block_alloc.cc
  blockman/structs.h
)

source_group( src\\common   FILES ${COMMON})
source_group( src\\storage  FILES ${STORAGE})
source_group( src\\blockman FILES ${BLOCKMAN})

add_library(cheesebase ${COMMON} ${STORAGE} ${BLOCKMAN})
target_link_libraries(cheesebase ${Boost_LIBRARIES})

