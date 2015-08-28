cmake_minimum_required(VERSION 3.2)

set(storage_srcs
  storage.h storage.cc
)

add_library(storage ${storage_srcs})
