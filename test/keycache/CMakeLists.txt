cmake_minimum_required(VERSION 3.1)

add_executable(test-keycache
  main.cc
  keycache.cc
)

target_link_libraries(test-keycache cheesebase ${Boost_LIBRARIES})
add_test(test-keycache test-keycache)

