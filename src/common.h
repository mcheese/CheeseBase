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

using Byte = gsl::byte;

template <class T>
using Span = gsl::span<T, gsl::dynamic_range>;

struct PageNr {
  constexpr explicit PageNr(uint64_t nr) : value{ nr } {}
  uint64_t addr() const noexcept { return value * k_page_size; }

  //! Provide hash callable for use in unordered_map.
  struct Hash {
    size_t operator()(PageNr n) const noexcept {
      return std::hash<uint64_t>{}(n.value);
    }
  };

  bool operator==(PageNr o) const noexcept { return value == o.value; }
  bool operator!=(PageNr o) const noexcept { return value != o.value; }
  bool operator<(PageNr o) const noexcept { return value < o.value; }
  bool operator>(PageNr o) const noexcept { return value > o.value; }

  uint64_t value;
};

struct Addr {
  Addr() = default;
  constexpr explicit Addr(uint64_t addr) : value{ addr } {}

  //! Get \c PageNr
  PageNr pageNr() const noexcept { return PageNr(value >> k_page_size_power); }

  //! Get offset in page: address % page_size.
  auto pageOffset() const noexcept {
    return gsl::narrow_cast<std::ptrdiff_t>(value & (k_page_size - 1));
  }

  //! Provide hash callable for use in unordered_map.
  struct Hash {
    size_t operator()(Addr a) const noexcept {
      return std::hash<uint64_t>{}(a.value);
    }
  };

  //! True if address is 0.
  bool isNull() const noexcept { return value == 0; }

  bool operator==(Addr o) const noexcept { return value == o.value; }
  bool operator!=(Addr o) const noexcept { return value != o.value; }
  bool operator<(Addr o) const noexcept { return value < o.value; }
  bool operator>(Addr o) const noexcept { return value > o.value; }

  uint64_t value;
};

//! Signed wrapper for sizeof().
template <typename T>
constexpr auto ssizeof() {
  static_assert(sizeof(T) == static_cast<std::ptrdiff_t>(sizeof(T)),
                "Signed sizeof changes value!");
  return static_cast<std::ptrdiff_t>(sizeof(T));
}

//! Signed wrapper for sizeof().
template <typename T>
constexpr auto ssizeof(const T&) {
  return ssizeof<T>();
}

//! Represents a write to disk.
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

using PageReadView = gsl::span<const Byte, k_page_size>;
using PageWriteView = gsl::span<Byte, k_page_size>;

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
