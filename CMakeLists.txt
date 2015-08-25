cmake_minimum_required(VERSION 2.8)

project(CheeseBase CXX)

add_subdirectory(src)

option(WITH_TESTS "compile unit tests" OFF)
if(WITH_TESTS)
  enable_testing()
  add_subdirectory(test)
endif(WITH_TESTS)
