// Licensed under the Apache License 2.0 (see LICENSE file).

// Common types, constants and functions.

#pragma once

#include <cstdint>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4245)
#include <gsl.h>
#pragma warning(pop)
#else
#include <gsl.h>
#endif

#define MOVE_ONLY(T) \
  T(T const&) = delete; \
  T& operator=(T const&) = delete; \
  T(T&&) = default; \
  T& operator=(T&&) = default;

#define DEF_EXCEPTION(NAME) \
struct NAME : public std::exception { \
  const char* what() const noexcept { return #NAME; } \
};                                 

using byte = unsigned char;

namespace cheesebase {

// size of a memory page
const size_t k_page_size_power{ 14 };
const size_t k_page_size{ 1u << k_page_size_power };

constexpr uint64_t page_nr(const uint64_t addr) noexcept
{
  return addr >> k_page_size_power;
};

constexpr uint64_t page_offset(const uint64_t addr) noexcept
{
  return addr & static_cast<uint64_t>(k_page_size - 1);
};

}
