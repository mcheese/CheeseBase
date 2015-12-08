cmake_minimum_required(VERSION 3.1)

add_executable(test-alloc
  main.cc
  alloc.cc
)

target_link_libraries(test-alloc cheesebase ${Boost_LIBRARIES})
add_test(test-alloc test-alloc)

