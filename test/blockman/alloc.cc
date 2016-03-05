#ifdef _MSC_VER
#define _SCL_SECURE_NO_WARNINGS
#endif
#include "blockman/allocator.h"
#include "catch.hpp"
#include "common/common.h"
#include "storage/storage.h"

using namespace cheesebase;

const Addr eof_addr = offsetof(DskDatabaseHdr, end_of_file);
const Addr pg_addr = offsetof(DskDatabaseHdr, free_alloc_pg);
const Addr t1_addr = offsetof(DskDatabaseHdr, free_alloc_t1);
const Addr t2_addr = offsetof(DskDatabaseHdr, free_alloc_t2);
const Addr t3_addr = offsetof(DskDatabaseHdr, free_alloc_t3);
const Addr t4_addr = offsetof(DskDatabaseHdr, free_alloc_t4);

const size_t pg_block = k_page_size - 50;
const size_t t1_block = k_page_size / 2 - 50;
const size_t t2_block = k_page_size / 4 - 50;
const size_t t3_block = k_page_size / 8 - 50;
const size_t t4_block = k_page_size / 16 - 50;

bool contains(const std::vector<Write>& ws, Addr addr, uint64_t word) {
  for (const auto& w : ws) {
    if (w.addr == addr && w.data == gsl::as_bytes(gsl::span<uint64_t>(word)))
      return true;
  }
  return false;
}

TEST_CASE("allocate and free blocks") {
  DskDatabaseHdr hdr;
  memset(&hdr, 0, sizeof(hdr));
  hdr.end_of_file = k_page_size;
  Storage store{ "test.db", OpenMode::create_always };
  store.storeWrite(Write({ 0, gsl::as_bytes(gsl::span<DskDatabaseHdr>(hdr)) }));

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
  REQUIRE(b1.addr == k_page_size);
  REQUIRE(b2.size >= pg_block);
  REQUIRE(b2.addr == k_page_size * 2);
  REQUIRE(b3.size >= t1_block);
  REQUIRE(b3.addr == k_page_size * 1.5);

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
  t.free(b1.addr);
  t.free(b3.addr);
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

  REQUIRE(b5.addr == k_page_size + k_page_size / 4);
  REQUIRE(b6.addr == k_page_size + k_page_size / 2);
  REQUIRE(b7.addr == k_page_size * 3);

  REQUIRE(contains(writes, t4_addr, k_page_size));
  REQUIRE(contains(writes, t2_addr,
                   k_page_size + k_page_size / 2 + k_page_size / 4));
  REQUIRE(contains(writes, t1_addr, k_page_size * 3 + k_page_size / 2));
  REQUIRE(contains(writes, eof_addr, k_page_size * 4));

  store.storeWrite(writes);
  t.end();

  t = alloc.startTransaction();
  auto b8 = t.allocExtension(b5.addr, t1_block);
  auto b9 = t.allocExtension(b8.addr, t2_block);

  writes = t.commit();

  // layout
  //
  // [~~~~~~~~~~~~~~~~~~~~~~~~~~ reserved ~~~~~~~~~~~~~~~~~~~~~~~~~~]
  // [  ][b4][      ][::::: b5 :::b8][::::: b6 :::::][::::: b9 :::::]
  // [::::::::::::::::::::::::::::: b2 :::::::::::::::::::::::::::::]
  // [::::::::::::: b7 :::::::::::::][::::::::::::: b8 :::::::::::b9]
  //
  REQUIRE(b8.addr == k_page_size * 3.5);
  REQUIRE(b9.addr == k_page_size * 1.75);
  REQUIRE(contains(writes, t2_addr, 0));
  REQUIRE(contains(writes, t1_addr, 0));
  REQUIRE(contains(writes, b5.addr, DskBlockHdr(BlockType::t2, b8.addr).data()));
  REQUIRE(contains(writes, b8.addr, DskBlockHdr(BlockType::t1, b9.addr).data()));

  store.storeWrite(writes);
  t.end();

  t = alloc.startTransaction();
  t.free(b5.addr);

  writes = t.commit();


  // layout
  //
  // [~~~~~~~~~~~~~~~~~~~~~~~~~~ reserved ~~~~~~~~~~~~~~~~~~~~~~~~~~]
  // [  ][b4][      ][              ][::::: b6 :::::][              ]
  // [::::::::::::::::::::::::::::: b2 :::::::::::::::::::::::::::::]
  // [::::::::::::: b7 :::::::::::::][                              ]
  //
  REQUIRE(contains(writes, t2_addr, b9.addr));
  REQUIRE(contains(writes, t1_addr, b8.addr));
  REQUIRE(contains(writes, b9.addr, DskBlockHdr(BlockType::t2, b5.addr).data()));

  store.storeWrite(writes);
}

