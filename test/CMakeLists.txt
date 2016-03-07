cmake_minimum_required(VERSION 3.1)

add_executable(test-cheesebase
  main.cc
  alloc.cc
  btree.cc
  cache.cc
  cheesebase.cc
  keycache.cc
  parser.cc
  storage.cc
)

target_link_libraries(test-cheesebase cheesebase ${Boost_LIBRARIES})
add_test(test-cheesebase test-cheesebase)

