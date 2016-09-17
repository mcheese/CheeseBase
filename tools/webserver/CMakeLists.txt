cmake_minimum_required(VERSION 3.0)

add_executable(webserver server.cc)
target_link_libraries(webserver cheesebase ${Boost_LIBRARIES})
