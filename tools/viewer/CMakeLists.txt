cmake_minimum_required(VERSION 3.1)

add_executable(db-viewer
  viewer.cc
)

target_link_libraries(db-viewer ${Boost_LIBRARIES})

