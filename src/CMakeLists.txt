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

set(ALLOC
  alloc/allocator.h alloc/allocator.cc
  alloc/block_alloc.h alloc/block_alloc.cc
  alloc/structs.h
)

source_group( src\\common  FILES ${COMMON}  )
source_group( src\\storage FILES ${STORAGE} )
source_group( src\\alloc   FILES ${ALLOC}   )

add_library(cheesebase ${COMMON} ${STORAGE} ${ALLOC})
target_link_libraries(cheesebase ${Boost_LIBRARIES})

