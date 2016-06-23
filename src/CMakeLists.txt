cmake_minimum_required(VERSION 3.1)

set(SRC
  storage.cc
  cache.cc
  allocator.cc
  block_alloc.cc
  block_locks.cc
  model.cc
  disk_string.cc
  disk_btree.cc
  disk_btree_impl.cc
  keycache.cc
  murmurhash3.cc
  core.cc
  cheesebase.cc
)

add_library(cheesebase ${SRC})
target_link_libraries(cheesebase ${Boost_LIBRARIES})

