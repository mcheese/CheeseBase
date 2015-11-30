// Licensed under the Apache License 2.0 (see LICENSE file).

// Provides memory pages to read and write from. Recently requested pages are
// cached. Writable pages are marked and later written back to disk.

#pragma once

#include "disk_worker.h"

#include <unordered_map>

namespace cheesebase {

// Locked reference of a page.
template <class View, class Lock>
class PageRef {
public:
  PageRef(View page, Lock lock)
      : m_page(page), m_lock(std::move(lock)){};

  MOVE_ONLY(PageRef);

  View get() const { return m_page; };
  View operator*() const { return m_page; };
  const View* operator->() const { return &m_page; };

private:
  const View m_page;
  Lock m_lock;
};

using ReadRef = PageRef<PageReadView, ShLock<RwMutex>>;
using WriteRef = PageRef<PageWriteView, ExLock<RwMutex>>;

class Cache {
public:
  Cache(const std::string& filename, OpenMode mode, size_t nr_pages);
  ~Cache();

  ReadRef read(PageNr page_nr);
  WriteRef write(PageNr page_nr);

private:
  struct Page {
    RwMutex mutex;
    gsl::span<Byte> data;
    PageNr page_nr{static_cast<PageNr>(-1)};
    Page* less_recent;
    Page* more_recent;
    bool changed{false};
  };

  // return specific page, creates it if not found
  template <class Lock>
  std::pair<Page&, Lock> get_page(PageNr page_nr);

  // return an unused page, may free the least recently used page
  std::pair<Page&, ExLock<RwMutex>>
  get_free_page(const ExLock<RwMutex>& map_lck);

  // mark p as most recently used (move to front of list)
  void bump_page(Page& p, const ExLock<Mutex>& lck);

  // ensure write to disk and remove from map
  void free_page(Page& p, const ExLock<RwMutex>& page_lck,
                 const ExLock<RwMutex>& map_lck);

  DiskWorker m_disk_worker;
  std::vector<Byte> m_memory;

  Mutex m_pages_mtx;
  std::unique_ptr<Page[]> m_pages;
  Page* m_least_recent;
  Page* m_most_recent;

  RwMutex m_map_mtx;
  std::unordered_map<PageNr, Page&> m_map;
};

} // namespace cheesebase
