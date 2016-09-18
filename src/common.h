// Licensed under the Apache License 2.0 (see LICENSE file).

// Constants and functions.

#pragma once

#include "exceptions.h"
#include "macros.h"
#include <boost/cstdint.hpp>
#include <boost/variant.hpp>
#include <span>
#include <gsl>
#include <vector>

namespace cheesebase {

//! Power of size of one memory page: page-size = 2^this
const size_t k_page_size_power{ 12 };

//! Size of one memory page. Change power instead of this.
const size_t k_page_size{ 1u << k_page_size_power };

//! Maximum size of pages kept in cache. Memory usage will be higher than this.
const size_t k_default_cache_size{ k_page_size * 1024 * 10 }; // 40 MB - test

using Byte = gsl::byte;

constexpr uint64_t lowerBitmask(size_t n) {
  return (static_cast<uint64_t>(1) << n) - 1;
}

constexpr uint64_t upperBitmask(size_t n) { return ~lowerBitmask(64 - n); }

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

// Word on disk keeping an address and 1 byte generic magic byte.
template <char T>
struct DskNext {
  DskNext() = default;
  DskNext(Addr next) : data_{ (static_cast<uint64_t>(T) << 56) + next.value } {}

  void check() const {
    if ((data_ >> 56) != T) throw ConsistencyError();
  }
  Addr next() const {
    check();
    return Addr(data_ & lowerBitmask(56));
  }
  uint64_t data() const noexcept { return data_; }

  uint64_t data_;
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

// interpret byte span as writeable object of type T
template <typename T, std::ptrdiff_t Extend>
T& bytesAsType(gsl::span<Byte, Extend> span) {
  Expects(
      (Extend == gsl::dynamic_extent && span.size_bytes() >= ssizeof<T>()) ||
      Extend >= ssizeof<T>());
  return *reinterpret_cast<T*>(span.data());
}

// interpret byte span as object of type T
template <typename T, std::ptrdiff_t Extend>
const T& bytesAsType(gsl::span<const Byte, Extend> span) {
  Expects(
      (Extend == gsl::dynamic_extent && span.size_bytes() >= ssizeof<T>()) ||
      Extend >= ssizeof<T>());
  return *reinterpret_cast<const T*>(span.data());
}

// interpret byte span as span of writeable Ts
template <typename T, std::ptrdiff_t Extend>
gsl::span<T> bytesAsSpan(gsl::span<Byte, Extend> span) {
  Expects(span.size_bytes() >= ssizeof<T>());
  return { reinterpret_cast<T*>(span.data()),
           span.size_bytes() / ssizeof<T>() };
}

// interpret byte span as span of Ts
template <typename T, std::ptrdiff_t Extend>
gsl::span<const T> bytesAsSpan(gsl::span<const Byte, Extend> span) {
  Expects(span.size_bytes() >= ssizeof<T>());
  return { reinterpret_cast<const T*>(span.data()),
           span.size_bytes() / ssizeof<T>() };
}

//! Represents a write to disk.
struct Write {
  Addr addr;
  boost::variant<gsl::span<const Byte>, uint64_t> data;
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

} // namespace cheesebase
