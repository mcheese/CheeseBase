cmake_minimum_required(VERSION 2.8)

set(config_srcs
  config.h
)

add_custom_target(config SOURCES ${config_srcs})

