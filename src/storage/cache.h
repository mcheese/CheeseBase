// Licensed under the Apache License 2.0 (see LICENSE file).

// Queue read/write requests to the file that get executed by a single thread
// in optimal order.

#pragma once

#include "fileio.h"
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/locks.hpp>
#include <unordered_map>

namespace cheesebase {

using PageView = gsl::span<const byte, k_page_size>;
using PageWriteView = gsl::span<byte, k_page_size>;
using Mutex = boost::mutex;
using RwMutex = boost::shared_mutex;
using UgMutex = boost::upgrade_mutex;
template<class M> using ExLock = boost::unique_lock<M>;
template<class M> using ShLock = boost::shared_lock<M>;
template<class M> using UgLock = boost::upgrade_lock<M>;

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

using ReadRef = LockedRef<PageView, ShLock<UgMutex>>;
using WriteRef = LockedRef<PageWriteView, ExLock<UgMutex>>;


class Cache {
public:
  Cache(const std::string& filename, OpenMode mode, size_t nr_pages);
  ~Cache();

  ReadRef read(uint64_t page_nr);
  WriteRef write(uint64_t page_nr);

private:
  struct Page {
    UgMutex mutex;
    gsl::span<byte> data;
    uint64_t page_nr{ 0 };
    Page* less_recent;
    Page* more_recent;
    int changed{ 0 };
  };

  // return specific page, creates it if not found
  template<class View, class Lock>
  LockedRef<View, Lock> get_page(uint64_t page_nr);

  // return an unused page, may free the least recently used page
  std::pair<gsl::not_null<Page*>, ExLock<UgMutex>>
    get_free_page(const ExLock<UgMutex>& map_lck);

  // mark p as most recently used (move to front of list)
  void bump_page(gsl::not_null<Page*> p, const ExLock<Mutex>& lck);
  
  // ensure write to disk and remove from map
  void free_page(gsl::not_null<Page*> p,
                 const ExLock<UgMutex>& page_lck,
                 const ExLock<UgMutex>& map_lck);

  FileIO m_file;
  std::vector<byte> m_memory;

  Mutex m_pages_mtx;
  std::unique_ptr<Page[]> m_pages;
  Page* m_least_recent;
  Page* m_most_recent;

  UgMutex m_map_mtx;
  std::unordered_map<uint64_t, Page*> m_map;
};

} // namespace cheesebase
