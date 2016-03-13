// Licensed under the Apache License 2.0 (see LICENSE file).

// Common types

#pragma once

#include <gsl.h>
#include <boost/cstdint.hpp>

namespace cheesebase {

using Byte = gsl::byte;

template <class T>
using Span = gsl::span<T, gsl::dynamic_range>;

using PageNr = uint64_t;
using Addr = uint64_t;
using Offset = uint64_t;

// Represents a write to disk.
struct Write {
  Addr addr;
  Span<const Byte> data;
};

using Writes = std::vector<Write>;

// Used by disk allocator
struct Block {
  Addr addr;
  size_t size;
};

} // namespace cheesebase

