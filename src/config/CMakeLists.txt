cmake_minimum_required(VERSION 3.2)

set(config_srcs
  config.h
)

add_custom_target(config SOURCES ${config_srcs})

