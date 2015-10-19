cmake_minimum_required(VERSION 3.2)

set(COMMON
  common/common.h
  common/log.h
  common/log.cc
)

set(STORAGE
  storage/storage.h
  storage/storage.cc
  storage/fileio.h
  storage/fileio.cc
  storage/cache.h
  storage/cache.cc
)

source_group( src\\common  FILES ${COMMON}  )
source_group( src\\storage FILES ${STORAGE} )

add_library(cheesebase ${COMMON} ${STORAGE})
target_link_libraries(cheesebase ${Boost_LIBRARIES})

