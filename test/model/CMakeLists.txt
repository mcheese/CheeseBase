cmake_minimum_required(VERSION 3.1)

add_executable(test-model
  main.cc
  parser.cc
  btree.cc
)

target_link_libraries(test-model cheesebase ${Boost_LIBRARIES})
add_test(test-model test-model)

