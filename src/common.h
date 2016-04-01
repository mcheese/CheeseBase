// Licensed under the Apache License 2.0 (see LICENSE file).

// Constants and functions.

#pragma once

#include "types.h"

namespace cheesebase {

constexpr uint16_t kVersion{ 0x0001 };
constexpr uint64_t k_magic{ 0x0000455342534843 + // CHSBSExx
                            (static_cast<uint64_t>(kVersion) << 48) };

// size of a memory page
const size_t k_page_size_power{ 12 };
const size_t k_page_size{ 1u << k_page_size_power };

const size_t k_default_cache_size{ k_page_size * 1024 * 10 }; // 40 MB - test

using PageReadView = gsl::span<const Byte, k_page_size>;
using PageWriteView = gsl::span<Byte, k_page_size>;

constexpr PageNr toPageNr(Addr addr) noexcept {
  return addr >> k_page_size_power;
}

constexpr auto toPageOffset(Addr addr) noexcept {
  return static_cast<Span<Byte>::size_type>(addr & (k_page_size - 1));
}

constexpr Addr toAddr(PageNr nr) noexcept { return nr * k_page_size; }

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
