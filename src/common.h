// Licensed under the Apache License 2.0 (see LICENSE file).

// Constants and functions.

#pragma once

#include "macros.h"
#include "exceptions.h"
#include <gsl.h>
#include <boost/cstdint.hpp>
#include <vector>

namespace cheesebase {


//! Power of size of one memory page: page-size = 2^this
const size_t k_page_size_power{ 12 };

//! Size of one memory page. Change power instead of this.
const size_t k_page_size{ 1u << k_page_size_power };

//! Maximum size of pages kept in cache. Memory usage will be higher than this.
const size_t k_default_cache_size{ k_page_size * 1024 * 10 }; // 40 MB - test

//! Byte type. Use \c Span to convert.
using Byte = gsl::byte;

//! Bounds checked memory view. Like a smart pointer+size.
template <class T, std::ptrdiff_t R = gsl::dynamic_range>
using Span = gsl::span<T, R>;

//! Represents number of memory page: floor(\c Addr / page-size).
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

//! Represents address in database.
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

//! Internal key type for object and array.
struct Key {
  static constexpr uint64_t sMaxKey{ (static_cast<uint64_t>(1) << 48) - 1 };

  explicit Key() = default;
  explicit Key(uint64_t key) : value{ key } {
    if (key > sMaxKey) {
      throw IndexOutOfRangeError();
    }
  }

  //! True if key is 0.
  bool isNull() const noexcept { return value == 0; }

  bool operator==(Key o) const noexcept { return value == o.value; }
  bool operator!=(Key o) const noexcept { return value != o.value; }
  bool operator<(Key o) const noexcept { return value < o.value; }
  bool operator>(Key o) const noexcept { return value > o.value; }
  bool operator<=(Key o) const noexcept { return value <= o.value; }
  bool operator>=(Key o) const noexcept { return value >= o.value; }

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

//! Collection of writes.
using Writes = std::vector<Write>;

// Used by disk allocator
struct Block {
  Addr addr;
  size_t size;
};

//! View of one read only memory page.
using PageReadView = gsl::span<const Byte, k_page_size>;
//! View of one read-write memory page.
using PageWriteView = gsl::span<Byte, k_page_size>;

//! Copy content of \c Spans.
/**
 * @param from  Source of the copy.
 * @param to    Target of the copy.
 */
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

constexpr uint64_t lowerBitmask(size_t n) {
  return (static_cast<uint64_t>(1) << n) - 1;
}

constexpr uint16_t kVersion{ 0x0001 };
constexpr uint64_t k_magic{ 0x0000455342534843 + // CHSBSExx
(static_cast<uint64_t>(kVersion) << 48) };

CB_PACKED(struct DskDatabaseHdr {
  uint64_t magic;
  Addr end_of_file;
  Addr free_alloc_pg;
  Addr free_alloc_t1;
  Addr free_alloc_t2;
  Addr free_alloc_t3;
  Addr free_alloc_t4;
});
static_assert(sizeof(DskDatabaseHdr) <= k_page_size / 2,
              "Database header should be smaller than half of the page size");

} // namespace cheesebase
