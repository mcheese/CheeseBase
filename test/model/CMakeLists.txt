cmake_minimum_required(VERSION 3.1)

add_executable(test-model
  main.cc
  model.cc
)

target_link_libraries(test-model cheesebase ${Boost_LIBRARIES})
add_test(test-model test-model)

