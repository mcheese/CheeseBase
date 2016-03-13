// Licensed under the Apache License 2.0 (see LICENSE file).

// Common types, constants and functions.

#pragma once

#include "exceptions.h"

#include <boost/cstdint.hpp>
#include <gsl.h>
#include <vector>

namespace cheesebase {

using Byte = gsl::byte;

template <class T>
using Span = gsl::span<T, gsl::dynamic_range>;

const uint64_t k_magic = *(uint64_t const*)"CHSBSE01";

// size of a memory page
const size_t k_page_size_power{ 12 };
const size_t k_page_size{ 1u << k_page_size_power };

const size_t k_default_cache_size{ k_page_size * 1024 * 10 }; // 40 MB - test

using PageReadView = gsl::span<const Byte, k_page_size>;
using PageWriteView = gsl::span<Byte, k_page_size>;

using PageNr = uint64_t;
using Addr = uint64_t;
using Offset = uint64_t;

constexpr PageNr toPageNr(Addr addr) noexcept {
  return addr >> k_page_size_power;
}

constexpr Offset toPageOffset(Addr addr) noexcept {
  return addr & static_cast<uint64_t>(k_page_size - 1);
}

constexpr Addr toAddr(PageNr nr) noexcept { return nr * k_page_size; }

// Represents a write to disk.
struct Write {
  Addr addr;
  Span<const Byte> data;
};

using Writes = std::vector<Write>;

template <typename T>
void copySpan(Span<const T> from, Span<T> to) {
  Expects(from.size() <= to.size());
  auto output = to.begin();
  auto input = from.cbegin();
  while (input != from.cend() && output != to.end()) {
    *output = *input;
    ++output;
    ++input;
  }
  Ensures(input == from.cend());
}

} // namespace cheesebase
