#include "blockman/allocator.h"
#include "catch.hpp"
#include "common/common.h"
#include "keycache/keycache.h"
#include "storage/storage.h"

using namespace cheesebase;

bool contains(const std::vector<Write>& ws, Addr addr, uint64_t word) {
  for (const auto& w : ws) {
    if (w.addr == addr && w.data == gsl::as_bytes(gsl::span<uint64_t>(word)))
      return true;
  }
  return false;
}

TEST_CASE("KeyCache") {
  DskDatabaseHdr hdr;
  memset(&hdr, 0, sizeof(hdr));
  hdr.end_of_file = k_page_size;
  Storage store{ "test.db", OpenMode::create_always };
  KeyCache keys{ store };
  Allocator alloc{ hdr, store };

  auto ta = alloc.startTransaction();
  auto tk = keys.startTransaction(ta);

  SECTION("different strings produce different hashes") {
    auto k1 = tk.getKey("test string 1");
    auto k2 = tk.getKey("test string 2");
    REQUIRE(k1 != k2);
    REQUIRE(k1.index == 0); // both are inserted on empty cache
    REQUIRE(k2.index == 0); // so index should be 0
  }

  std::string teststr{ "test string" };

  SECTION("same strings produce same hashes") {
    auto k1 = tk.getKey(teststr);
    auto k2 = tk.getKey(teststr);
    REQUIRE(k1 == k2);
  }

  SECTION("KeyCache knows new keys after commit") {
    auto fail = keys.getKey(teststr);
    REQUIRE_FALSE(fail);
    auto k1 = tk.getKey(teststr);
    auto writes = tk.commit();

    auto alloc_writes = ta.commit();
    REQUIRE(contains(alloc_writes, k_page_size,
                     DskBlockHdr(BlockType::page, 0).data));
    REQUIRE(writes[0].addr == k_page_size + sizeof(DskBlockHdr));
    auto len = gsl::narrow_cast<DskKeyCacheSize>(teststr.size());
    REQUIRE(writes[0].data == gsl::as_bytes(gsl::span<DskKeyCacheSize>(len)));

    REQUIRE(writes[1].addr ==
            k_page_size + sizeof(DskBlockHdr) + sizeof(DskKeyCacheSize));
    REQUIRE(writes[1].data == gsl::as_bytes(gsl::span<const char>(teststr)));

    tk.end();

    auto key = keys.getKey("test string");
    REQUIRE(k1 == key.value());

    auto str = keys.getString(k1);
    REQUIRE(str == "test string");
  }

  SECTION("KeyCache does not know keys that where not committed") {
    auto k1 = tk.getKey("test string");
    tk.end();

    auto fail = keys.getKey("test string");
    REQUIRE_FALSE(fail);
    REQUIRE_THROWS_AS(keys.getString(k1), KeyCacheError);
  }
}
