// Licensed under the Apache License 2.0 (see LICENSE file).

// Queue read/write requests to the file that get executed by a single thread
// in optimal order.

#pragma once

#include "disk_worker.h"

#include <unordered_map>

namespace cheesebase {

// Read-locked reference of a page.
template<class View, class Lock>
class LockedRef {
public:
  LockedRef(View page, Lock lock) : m_page(page), m_lock(std::move(lock))
  {};

  MOVE_ONLY(LockedRef);

  const View& operator*() const { return m_page; };
  const View* operator->() const { return &m_page; };
  const View& get() const { return m_page; };

private:
  const View m_page;
  Lock m_lock;
};

using ReadRef = LockedRef<PageReadView, ShLock<UgMutex>>;
using WriteRef = LockedRef<PageWriteView, ExLock<UgMutex>>;


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
    int changed{ 0 };
  };

  // return specific page, creates it if not found
  template<class View, class Lock>
  LockedRef<View, Lock> get_page(PageNr page_nr);

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
