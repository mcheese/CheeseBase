cmake_minimum_required(VERSION 2.8)

# tests
add_executable(test-storage storage.cc)
target_link_libraries(test-storage storage)
add_test(test-storage test-storage)

add_executable(test-test test.cc)
add_test(test-test test-test)

