cmake_minimum_required(VERSION 3.2)

set(COMMON
  common/common.h
)

set(STORAGE
  storage/storage.h
  storage/storage.cc
)

source_group( src\\common  FILES ${COMMON}  )
source_group( src\\storage FILES ${STORAGE} )

add_library(cheesebase ${COMMON} ${STORAGE})
include_directories(./)
