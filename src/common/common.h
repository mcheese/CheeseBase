// Copyright 2015 Max 'Cheese'.
// Licensed under the Apache License 2.0 (see LICENSE file).

// Common types, constants and functions.

#pragma once

#include <cstdint>

using byte = unsigned char;

namespace cheesebase {

const size_t kPageSizePower{ 14 };
const size_t kPageSize{ 1u << kPageSizePower };

constexpr uint64_t pageNr(const uint64_t addr)
{
  return addr >> kPageSizePower;
};

constexpr size_t pageOffset(const uint64_t addr)
{
  return static_cast<size_t>(addr) & (kPageSize - 1);
};

}
