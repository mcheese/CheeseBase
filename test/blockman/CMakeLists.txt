cmake_minimum_required(VERSION 3.1)

add_executable(test-blockman
  main.cc
  alloc.cc
)

target_link_libraries(test-blockman cheesebase ${Boost_LIBRARIES})
add_test(test-blockman test-blockman)

