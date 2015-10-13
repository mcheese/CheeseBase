cmake_minimum_required(VERSION 3.2)

add_executable(test-storage 
  main.cc
  fileio.cc
)

target_link_libraries(test-storage cheesebase)
add_test(test-storage test-storage)

