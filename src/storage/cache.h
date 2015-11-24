// Licensed under the Apache License 2.0 (see LICENSE file).

// Provides memory pages to read and write from. Recently requested pages are
// cached. Writable pages are marked and later written back to disk.

#pragma once

#include "disk_worker.h"

#include <unordered_map>

namespace cheesebase {

// Read-locked reference of a page.
class ReadRef {
public:
  ReadRef(PageReadView page, ShLock<UgMutex> lock)
    : m_page(page), m_lock(std::move(lock))
  {};

  MOVE_ONLY(ReadRef);

  PageReadView get() const { return m_page; };

private:
  const PageReadView m_page;
  ShLock<UgMutex> m_lock;
};

// Write-locked reference of a page.
class WriteRef {
public:
  WriteRef(PageWriteView page, bool& changed, UgLock<UgMutex> lock)
    : m_page(page), m_changed(changed), m_lock(std::move(lock))
  {};

  MOVE_ONLY(WriteRef);

  // Get a readable page view, does not change lock state
  PageReadView get_read() const { return m_page; };
  
  // Upgrade to exclusive access and return writable page view
  PageWriteView get_write()
  {
    if (!m_exclusive) upgrade();
    return m_page;
  }
  
  // Upgrade lock to exclusive access
  void upgrade()
  {
    if (!m_exclusive) {
      m_changed = true;
      m_exclusive = true;
      m_xlock = std::move(m_lock);
    }
  }

  // Downgrade lock to upgradeable access
  void downgrade()
  {
    if (m_exclusive) {
      m_exclusive = false;
      m_lock = std::move(m_xlock);
    }
  }

private:
  bool m_exclusive{ false };
  const PageWriteView m_page;
  bool& m_changed;
  UgLock<UgMutex> m_lock;
  ExLock<UgMutex> m_xlock;
};

class Cache {
public:
  Cache(const std::string& filename, OpenMode mode, size_t nr_pages);
  ~Cache();

  ReadRef read(PageNr page_nr);
  WriteRef write(PageNr page_nr);

private:
  struct Page {
    UgMutex mutex;
    gsl::span<byte> data;
    PageNr page_nr{ 0 };
    Page* less_recent;
    Page* more_recent;
    bool changed{ false };
  };

  // return specific page, creates it if not found
  template<class Lock>
  std::pair<Page&, Lock> get_page(PageNr page_nr);

  // return an unused page, may free the least recently used page
  std::pair<Page&, ExLock<UgMutex>>
    get_free_page(const ExLock<UgMutex>& map_lck);

  // mark p as most recently used (move to front of list)
  void bump_page(Page& p, const ExLock<Mutex>& lck);
  
  // ensure write to disk and remove from map
  void free_page(Page& p,
                 const ExLock<UgMutex>& page_lck,
                 const ExLock<UgMutex>& map_lck);

  DiskWorker m_disk_worker;
  std::vector<byte> m_memory;

  Mutex m_pages_mtx;
  std::unique_ptr<Page[]> m_pages;
  Page* m_least_recent;
  Page* m_most_recent;

  UgMutex m_map_mtx;
  std::unordered_map<PageNr, Page&> m_map;
};

} // namespace cheesebase
