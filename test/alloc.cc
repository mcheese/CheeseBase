#ifdef _MSC_VER
#define _SCL_SECURE_NO_WARNINGS
#endif
#include "allocator.h"
#include "catch.hpp"
#include "common.h"
#include "storage.h"

using namespace cheesebase;

const Addr eof_addr = Addr(offsetof(DskDatabaseHdr, end_of_file));
const Addr pg_addr = Addr(offsetof(DskDatabaseHdr, free_alloc_pg));
const Addr t1_addr = Addr(offsetof(DskDatabaseHdr, free_alloc_t1));
const Addr t2_addr = Addr(offsetof(DskDatabaseHdr, free_alloc_t2));
const Addr t3_addr = Addr(offsetof(DskDatabaseHdr, free_alloc_t3));
const Addr t4_addr = Addr(offsetof(DskDatabaseHdr, free_alloc_t4));

const size_t pg_block = k_page_size - 50;
const size_t t1_block = k_page_size / 2 - 50;
const size_t t2_block = k_page_size / 4 - 50;
const size_t t3_block = k_page_size / 8 - 50;
const size_t t4_block = k_page_size / 16 - 50;

bool contains(const std::vector<Write>& ws, Addr addr, uint64_t word) {
  for (const auto& w : ws) {
    if (w.addr == addr && w.data.type() == typeid(uint64_t) &&
        boost::get<uint64_t>(w.data) == word)
      return true;
  }
  return false;
}

TEST_CASE("allocate and free blocks") {
  DskDatabaseHdr hdr;
  memset(&hdr, 0, sizeof(hdr));
  hdr.end_of_file.value = k_page_size;
  Storage store{ "test.db", OpenMode::create_always };
  store.storeWrite(
      Write({ Addr(0), gsl::as_bytes(gsl::span<DskDatabaseHdr>(hdr)) }));

  Allocator alloc{ hdr, store };

  auto t = alloc.startTransaction();

  auto b1 = t.alloc(t4_block);
  auto b2 = t.alloc(pg_block);
  auto b3 = t.alloc(t1_block);
  auto writes = t.commit();

  // layout
  //
  //   [~~~~~~~~~~~~~~~~~~~~~~~~~~ reserved ~~~~~~~~~~~~~~~~~~~~~~~~~~]
  //   [b1][  ][      ][              ][:::::::::::::: b3 ::::::::::::]
  //   [::::::::::::::::::::::::::::: b2 :::::::::::::::::::::::::::::]
  //
  REQUIRE(b1.size >= t4_block);
  REQUIRE(b1.addr.value == k_page_size);
  REQUIRE(b2.size >= pg_block);
  REQUIRE(b2.addr.value == k_page_size * 2);
  REQUIRE(b3.size >= t1_block);
  REQUIRE(b3.addr.value == k_page_size * 1.5);

  REQUIRE(contains(writes, t4_addr, k_page_size + k_page_size / 16));
  REQUIRE(contains(writes, t3_addr, k_page_size + k_page_size / 8));
  REQUIRE(contains(writes, t2_addr, k_page_size + k_page_size / 4));
  REQUIRE(contains(writes, t1_addr, 0));
  REQUIRE(contains(writes, eof_addr, k_page_size * 3));

  store.storeWrite(writes);
  t.end();
  t = alloc.startTransaction();

  auto b4 = t.alloc(t4_block);
  auto b5 = t.alloc(t2_block);
  t.free(b1);
  t.free(b3);
  auto b6 = t.alloc(t2_block);
  auto b7 = t.alloc(t1_block);
  writes = t.commit();

  // layout
  //
  // [~~~~~~~~~~~~~~~~~~~~~~~~~~ reserved ~~~~~~~~~~~~~~~~~~~~~~~~~~]
  // [  ][b4][      ][::::: b5 :::::][::::: b6 :::::][              ]
  // [::::::::::::::::::::::::::::: b2 :::::::::::::::::::::::::::::]
  // [::::::::::::: b7 :::::::::::::][                              ]
  //

  REQUIRE(b5.addr.value == k_page_size + k_page_size / 4);
  REQUIRE(b6.addr.value == k_page_size + k_page_size / 2);
  REQUIRE(b7.addr.value == k_page_size * 3);

  REQUIRE(contains(writes, t4_addr, k_page_size));
  REQUIRE(contains(writes, t2_addr,
                   k_page_size + k_page_size / 2 + k_page_size / 4));
  REQUIRE(contains(writes, t1_addr, k_page_size * 3 + k_page_size / 2));
  REQUIRE(contains(writes, eof_addr, k_page_size * 4));

  store.storeWrite(writes);
  t.end();
}
