cmake_minimum_required(VERSION 3.2)

project(CheeseBase CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

find_package(Boost COMPONENTS thread REQUIRED)
include_directories(
  src
  external/GSL/include
  ${Boost_INCLUDE_DIRS}
)

add_subdirectory(src)

option(TESTS "compile unit tests" OFF)
if(TESTS)
  enable_testing()
  add_subdirectory(test)
endif(TESTS)

