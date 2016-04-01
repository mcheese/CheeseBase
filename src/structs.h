// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "common.h"
#include "macros.h"
#include "exceptions.h"
#include <gsl_util.h> // narrow_cast

namespace cheesebase {

enum class BlockType {
  pg = 'P', // 1 page (4k)
  t1 = '1', // 1/2 page (2k)
  t2 = '2', // 1/4 page (1k)
  t3 = '3', // 1/8 page (512)
  t4 = '4'  // 1/16 page (256)
};

constexpr size_t toBlockSize(BlockType t) {
  return t == BlockType::pg
             ? k_page_size
             : t == BlockType::t1
                   ? k_page_size / 2
                   : t == BlockType::t2
                         ? k_page_size / 4
                         : t == BlockType::t3
                               ? k_page_size / 8
                               : t == BlockType::t4 ? k_page_size / 16
                                                    : throw ConsistencyError(
                                                          "Invalid block type");
}

constexpr uint64_t lowerBitmask(size_t n) {
  return (static_cast<uint64_t>(1) << n) - 1;
}

CB_PACKED(struct DskBlockHdr {
  DskBlockHdr() = default;
  DskBlockHdr(BlockType type, Addr next)
      : data_{ (static_cast<uint64_t>(type) << 56) + next.value } {
    Expects(static_cast<uint64_t>(type) <= 0xff);
    Expects(next.value <= lowerBitmask(56));
  }

  Addr next() const noexcept { return Addr(data_ & lowerBitmask(56)); }

  BlockType type() const noexcept {
    return gsl::narrow_cast<BlockType>(data_ >> 56);
  }

  uint64_t data() const noexcept { return data_; }

  uint64_t data_;
});

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

using Key = uint64_t;
const Key kMaxKey = static_cast<Key>(-1);

CB_PACKED(struct DskKey {
  DskKey() = default;
  DskKey(uint32_t h, uint16_t i) : hash(h), index(i) {}
  DskKey(Key k) {
    index = gsl::narrow_cast<uint16_t>(k);
    hash = gsl::narrow_cast<uint32_t>(k >> 16);
  }
  Key key() const { return ((static_cast<uint64_t>(hash) << 16) + index); }
  uint32_t hash;
  uint16_t index;

  bool operator!=(const DskKey& o) {
    return this->hash != o.hash || this->index != o.index;
  }
  bool operator==(const DskKey& o) { return !(*this != o); }
});
static_assert(sizeof(DskKey) == 6, "Invalid disk key size");

CB_PACKED(using DskKeyCacheSize = uint16_t);
static_assert(sizeof(DskKeyCacheSize) == 2, "Invalid disk key cache size size");

CB_PACKED(struct DskValueHdr {
  uint8_t magic_byte;
  uint8_t type;
});
static_assert(sizeof(DskValueHdr) == 2, "Invalid disk value header size");

CB_PACKED(struct DskPair {
  DskKey key;
  DskValueHdr value;
});
static_assert(sizeof(DskPair) == 8, "invalid disk pair size");

} // namespace cheesebase
