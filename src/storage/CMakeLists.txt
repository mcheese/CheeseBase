cmake_minimum_required(VERSION 2.8)

set(storage_srcs
  storage.h storage.cc
)

add_library(storage ${storage_srcs})

