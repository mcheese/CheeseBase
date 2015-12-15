cmake_minimum_required(VERSION 3.1)

project(CheeseBase CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_definitions(-DBOOST_ALL_NO_LIB)
set(Boost_USE_STATIC_LIBS ON)

find_package(Boost COMPONENTS system thread filesystem REQUIRED)
include_directories(
  src
  external/GSL/include
  ${Boost_INCLUDE_DIRS}
)

add_subdirectory(src)
add_subdirectory(tools)

option(TESTS "compile unit tests" OFF)
if(TESTS)
  enable_testing()
  add_subdirectory(test)
endif(TESTS)

