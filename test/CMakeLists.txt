cmake_minimum_required(VERSION 2.8)

# googletest
set(gtest_force_shared_crt ON)
add_subdirectory(googletest)
include_directories(${gtest_SOURCE_DIR}/include)

# tests
add_executable(test-storage storage.cc)
target_link_libraries(test-storage gtest_main storage)
add_test(test-storage test-storage)

add_executable(test-test test.cc)
target_link_libraries(test-test gtest_main)
add_test(test-test test-test)

