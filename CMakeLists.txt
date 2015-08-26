cmake_minimum_required(VERSION 2.8)

project(CheeseBase CXX)

add_subdirectory(src)

option(TESTS "compile unit tests" OFF)
if(TESTS)
  enable_testing()
  add_subdirectory(test)
endif(TESTS)
