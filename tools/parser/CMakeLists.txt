cmake_minimum_required(VERSION 3.1)

add_executable(parser-benchmark
  parser.cc
)

target_link_libraries(parser-benchmark ${Boost_LIBRARIES} cheesebase)

