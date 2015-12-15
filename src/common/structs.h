// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "common.h"
#include "compiler.h"
#include <gsl.h>

namespace cheesebase {

enum class BlockType {
  multi, // first block of a multi page linked allocation
  page,  // 1 page (4k)
  t1,    // 1/2 page (2k)
  t2,    // 1/4 page (1k)
  t3,    // 1/8 page (512)
  t4     // 1/16 page (256)
};

CB_PACKED(struct DskBlockHdr {
  DskBlockHdr() = default;
  DskBlockHdr(BlockType type, Addr next) {
    auto type_value = static_cast<uint64_t>(type);
    Expects(type_value <= 0xff);
    Expects(next < ((uint64_t)1 << 56));
    data = (type_value << 56) + (next & (((uint64_t)1 << 56) - 1));
  };

  PageNr next() const noexcept { return (data & (((uint64_t)1 << 56) - 1)); }

  BlockType type() const noexcept {
    return gsl::narrow_cast<BlockType>(data >> 56);
  }

  uint64_t data;
});

CB_PACKED(struct DskDatabaseHdr {
  char magic[8];
  uint64_t end_of_file;
  uint64_t free_alloc_page;
  uint64_t free_alloc_t1;
  uint64_t free_alloc_t2;
  uint64_t free_alloc_t3;
  uint64_t free_alloc_t4;
});

CB_PACKED(struct DskKey {
  DskKey(uint32_t h, uint16_t i) : hash(h), index(i) {};
  uint32_t hash;
  uint16_t index;

  bool operator!=(const DskKey& o) {
    return this->hash != o.hash || this->index != o.index;
  }
  bool operator==(const DskKey& o) {
    return !(*this != o);
  }
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
