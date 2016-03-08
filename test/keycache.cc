#ifdef _MSC_VER
#define _SCL_SECURE_NO_WARNINGS
#endif
#include "allocator.h"
#include "catch.hpp"
#include "common.h"
#include "keycache.h"
#include "storage.h"
#include <boost/filesystem.hpp>

#define private public
#include "core.h"
#undef private

using namespace cheesebase;

TEST_CASE("KeyCache single") {
  DskDatabaseHdr hdr;
  memset(&hdr, 0, sizeof(hdr));
  hdr.end_of_file = k_page_size;
  Storage store{ "test.db", OpenMode::create_always };

  // build first keycache block
  constexpr auto kc_pos = k_page_size / 2;
  DskBlockHdr cache_hdr{ BlockType::t1, 0 };
  uint64_t term{ 0 };
  store.storeWrite(
      Writes{ { kc_pos, gsl::as_bytes(gsl::span<DskBlockHdr>(cache_hdr)) },
              { kc_pos + sizeof(DskBlockHdr),
                gsl::as_bytes(gsl::span<uint64_t>(term)) } });

  KeyCache keys{ Block{ kc_pos, kc_pos }, store };
  Allocator alloc{ hdr, store };

  auto ta = alloc.startTransaction();
  auto tk = keys.startTransaction(ta);

  SECTION("different strings produce different hashes") {
    auto k1 = tk.getKey("test string 1");
    auto k2 = tk.getKey("test string 2");
    REQUIRE(k1 != k2);
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

TEST_CASE("KeyCache database") {
  boost::filesystem::remove("test.db");
  Database db{ "test.db" };

  auto ta = db.alloc_->startTransaction();
  auto tk = db.keycache_->startTransaction(ta);

  SECTION("different strings produce different hashes") {
    auto k1 = tk.getKey("test string 1");
    auto k2 = tk.getKey("test string 2");
    REQUIRE(k1 != k2);
  }

  std::string teststr{ "test string" };

  SECTION("same strings produce same hashes") {
    auto k1 = tk.getKey(teststr);
    auto k2 = tk.getKey(teststr);
    REQUIRE(k1 == k2);
  }

  SECTION("KeyCache knows new keys after commit") {
    auto fail = db.keycache_->getKey(teststr);
    REQUIRE_FALSE(fail);
    auto k1 = tk.getKey(teststr);
    auto writes = tk.commit();
    tk.end();

    auto key = db.keycache_->getKey(teststr);
    REQUIRE(k1 == key.value());

    auto str = db.keycache_->getString(k1);
    REQUIRE(str == teststr);
  }

  SECTION("KeyCache does not know keys that where not committed") {
    auto k1 = tk.getKey(teststr);
    tk.end();

    auto fail = db.keycache_->getKey(teststr);
    REQUIRE_FALSE(fail);
    REQUIRE_THROWS_AS(db.keycache_->getString(k1), KeyCacheError);
  }

  SECTION("committed keys are known after db restart") {
    auto k1 = tk.getKey(teststr);
    auto writes = tk.commit();
    auto writes2 = ta.commit();
    std::move(writes2.begin(), writes2.end(), std::back_inserter(writes));
    db.store_->storeWrite(writes);

    // destruct old db and open new
    tk.end();
    ta.end();
    db = Database();
    db = Database("test.db");
    auto key = db.keycache_->getKey(teststr);
    REQUIRE(key.value() == k1);
    auto str = db.keycache_->getString(k1);
    REQUIRE(str == teststr);
  }

  SECTION("not committed keys are not known after db restart") {
    auto k1 = tk.getKey(teststr);
    auto writes = tk.commit();
    auto writes2 = ta.commit();
    // do not call storeWrites

    // destruct old db and open new
    tk.end();
    ta.end();
    db = Database();
    db = Database("test.db");
    auto key = db.keycache_->getKey(teststr);
    REQUIRE_FALSE(key);
    REQUIRE_THROWS_AS(db.keycache_->getString(k1), KeyCacheError);
  }

  SECTION("many keys are inserted and tested") {
    // need to fill the initial allocation to force an extension
    const size_t amount = 1000;
    std::vector<std::string> vec;
    std::vector<Key> keys;
    keys.reserve(amount);
    vec.reserve(amount);
    for (size_t i = 0; i < amount; ++i) {
      vec.push_back(teststr + "#" + std::to_string(i));
      keys.push_back(tk.getKey(vec.back()));
    }

    auto writes = tk.commit();
    auto writes2 = ta.commit();
    std::move(writes2.begin(), writes2.end(), std::back_inserter(writes));
    db.store_->storeWrite(writes);

    // destruct old db and open new
    tk.end();
    ta.end();
    db = Database();
    db = Database("test.db");

    for (size_t i = 0; i < amount; ++i) {
      auto key = db.keycache_->getKey(vec[i]);
      REQUIRE(key.value() == keys[i]);
      auto str = db.keycache_->getString(keys[i]);
      REQUIRE(str == vec[i]);
    }
  }
}
