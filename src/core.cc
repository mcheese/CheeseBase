// Licensed under the Apache License 2.0 (see LICENSE file).

#include "core.h"

#include "blockman/allocator.h"
#include "keycache/keycache.h"
#include "storage/storage.h"

#include <boost/filesystem.hpp>

namespace cheesebase {

Database::Database(const std::string& file) {
  DskDatabaseHdr hdr;

  if (boost::filesystem::exists(file)) {
    m_store = std::make_unique<Storage>(file, OpenMode::open_existing);
    auto page = m_store->loadPage(0);
    hdr = gsl::as_span<const DskDatabaseHdr>(
        page->subspan(0, sizeof(DskDatabaseHdr)))[0];

    auto kc_blk = gsl::as_span<DskBlockHdr>(page->subspan(k_page_size / 2))[0];
    if (hdr.magic != k_magic || hdr.free_alloc_pg % k_page_size != 0 ||
        hdr.free_alloc_t1 % k_page_size / 2 != 0 ||
        hdr.free_alloc_t2 % k_page_size / 4 != 0 ||
        hdr.free_alloc_t3 % k_page_size / 8 != 0 ||
        hdr.free_alloc_t4 % k_page_size / 16 != 0 ||
        hdr.end_of_file % k_page_size != 0 || hdr.end_of_file < k_page_size ||
        kc_blk.type() != BlockType::t1)
      throw DatabaseError("Invalid database header");

  } else {
    m_store = std::make_unique<Storage>(file, OpenMode::create_new);
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic = k_magic;
    hdr.end_of_file = k_page_size;

    // manually create first block of KeyCache
    DskBlockHdr cache_hdr{ BlockType::t1, 0 };
    DskKeyCacheSize cache_term = 0;
    m_store->storeWrite(Writes{
        Write{ 0, gsl::as_bytes(gsl::span<DskDatabaseHdr>(hdr)) },
        Write{ k_page_size / 2,
               gsl::as_bytes(gsl::span<DskBlockHdr>(cache_hdr)) },
        Write{ k_page_size / 2 + sizeof(DskBlockHdr),
               gsl::as_bytes(gsl::span<DskKeyCacheSize>(cache_term)) } });
  }


  m_alloc = std::make_unique<Allocator>(hdr, *m_store);
  m_keycache = std::make_unique<KeyCache>(
      Block{ k_page_size / 2, k_page_size / 2 }, *m_store);
}

} // namespace cheesebase
