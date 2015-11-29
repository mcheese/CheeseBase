cmake_minimum_required(VERSION 3.1)

add_executable(test-storage
  main.cc
  fileio.cc
  inc_prio_queue.cc
  cache.cc
)

target_link_libraries(test-storage cheesebase ${Boost_LIBRARIES})
add_test(test-storage test-storage)

