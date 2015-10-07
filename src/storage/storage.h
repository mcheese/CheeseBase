// Licensed under the Apache License 2.0 (see LICENSE file).

// Manage storage of the database. Hold a DB file and provide load/store
// operations on the data as well as caching.

#pragma once

#include "common/common.h"

#include <string>
#include <shared_mutex>
#include <array>

namespace cheesebase {

using Page = std::array<byte, k_page_size>;

// Read-locked reference of a page.
class PageRef {
public:
  PageRef(const Page& page,
          std::shared_lock<std::shared_timed_mutex>&& lock)
    : m_page(page)
    , m_lock(std::move(lock))
  {};

  // non-copyable and movable
  PageRef(const PageRef&) = delete;
  PageRef& operator=(const PageRef&) = delete;
  PageRef(PageRef&&) = default;
  PageRef& operator=(PageRef&&) = default;

  const Page& operator*() const { return m_page; };
  const Page* operator->() const { return &m_page; };
  const Page& get() const { return m_page; };

private:
  const Page& m_page;
  std::shared_lock<std::shared_timed_mutex> m_lock;
};

// Disk representation of a database instance. Opens DB file and journal on
// construction. Provides load/store access backed by a cache.
class Storage {
public:
  enum class OpenMode {
    create_new,    // Creates new DB if it does not exist.
    create_always, // Creates new DB, always. Overwrite existing DB.
    open_existing, // Opens DB if it exists.
    open_always    // Opens DB, always. Creates new DB if it does not exist.
  };

  // Create a Storage associated with a DB and journal file. Opens an existing
  // database or creates a new one bases on "mode" argument.
  Storage(const std::string& filename, const OpenMode mode);

  // Flushes open writes and closes the DB file.
  ~Storage();

  // Get a DB page. Reads the requested page in the cache (if needed) and
  // returns a PageReadRef object holding a read-locked reference of the page.
  // The referenced page is guaranteed to be valid and unchanged for the
  // lifetime of the object.
  PageRef load(const uint64_t page_nr);

  // Write data to the DB. The write position can overlap multiple pages. Old
  // data is overwritten and the file extended if needed. The caller has to
  // handle consistency of the database.
  // The write is guaranteed to be all-or-nothing. On return of the function
  // the journal has been written and persistence of the write is guaranteed.
  void store(const uint64_t offset, const byte* const buf, const size_t size);

private:

};

} // namespace cheesebase
