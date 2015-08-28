// Copyright 2015 Max 'Cheese'.
// Licensed under the Apache License 2.0 (see LICENSE file).

// Manage storage of the database. Hold a DB file and provide load/store
// operations on the data as well as caching.

#pragma once

#include <string>
#include <cstdint>
#include <shared_mutex>

namespace cheesebase {

// Combination of pointer and lock.
class ReadPtr {
public:
  ReadPtr(const void* const ptr,
          std::shared_lock<std::shared_timed_mutex>&& lock)
    : ptr(ptr), lock(std::move(lock))
  {};

  // non-copyable, movable
  ReadPtr(const ReadPtr&) = delete;
  ReadPtr& operator=(const ReadPtr&) = delete;
  ReadPtr(ReadPtr&&) = default;
  ReadPtr& operator=(ReadPtr&&) = default;

  const void* const ptr;

private:
  std::shared_lock<std::shared_timed_mutex> lock;
};

// Disk representation of a database instance. Opens DB file and journal on
// construction. Provides load/store access backed by a cache.
class Storage {
public:
  enum class Mode { open, create };

  // Create a Storage associated with a DB and journal file. Opens an existing
  // database or creates a new one bases on "mode" argument.
  Storage(const std::string& filename, Mode mode);

  // Destroy the object. Flushes out writes and closes the DB file.
  ~Storage();

  // Get a memory page. Reads the requested page in the cache (if needed) and
  // returns a ReadPtr object holding a pointer and locked read mutex for the
  // memory page. The pointed data is guaranteed to be valid for the lifetime
  // of the ReadPtr object.
  ReadPtr load(const uint64_t page_nr);

  // Write data to the DB. The write position can overlap memory pages. Old
  // data is overwritten and the file extended if needed. The caller has to
  // handle consistency of the database.
  // The write is guaranteed to be all-or-nothing. On return of the function
  // the journal has been written and persistence of the write is guaranteed.
  void store(const uint64_t offset, const void* const buf, const size_t size);

private:

};

} // namespace cheesebase
