cmake_minimum_required(VERSION 3.1)

set(SRC
  common.h
  sync.h
  compiler.h
  structs.h
  storage.h storage.cc
  cache.h cache.cc
  allocator.h allocator.cc
  block_alloc.h block_alloc.cc
  block_locks.h block_locks.cc
  model.h model.cc
  disk_value.h
  disk_object.h disk_object.cc
  parser.h
  keycache.h keycache.cc
  murmurhash3.h murmurhash3.cc
  core.h core.cc
  cheesebase.h cheesebase.cc
  cbindings.cc
)

add_library(cheesebase ${SRC})
target_link_libraries(cheesebase ${Boost_LIBRARIES})

