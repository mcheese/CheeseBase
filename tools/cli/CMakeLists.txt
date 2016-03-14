cmake_minimum_required(VERSION 3.1)

add_executable(cheesebase-cli
  main.cc
)

target_link_libraries(cheesebase-cli ${Boost_LIBRARIES} cheesebase)

