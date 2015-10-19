// Licensed under the Apache License 2.0 (see LICENSE file).

// Queue read/write requests to the file that get executed by a single thread
// in optimal order.

#pragma once

#include "fileio.h"
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/locks.hpp>
#include <unordered_map>

namespace cheesebase {

using PageView = gsl::array_view<const byte, k_page_size>;
using PageWriteView = gsl::array_view<byte, k_page_size>;
using RwMutex = boost::upgrade_mutex;

// Read-locked reference of a page.
template<class View, class Lock>
class LockedRef {
public:
  LockedRef(View page, Lock&& lock) : m_page(page), m_lock(std::move(lock))
  {};

  MOVE_ONLY(LockedRef);

  const View& operator*() const { return m_page; };
  const View* operator->() const { return &m_page; };
  const View& get() const { return m_page; };

private:
  const View m_page;
  Lock m_lock;
};

using ReadRef = LockedRef<PageView, boost::shared_lock<RwMutex>>;
using WriteRef = LockedRef<PageWriteView, boost::unique_lock<RwMutex>>;


class Cache {
public:
  Cache(const std::string& filename, const OpenMode mode);
  ~Cache();

  ReadRef read(const uint64_t page_nr);
  WriteRef write(const uint64_t page_nr);

private:
  struct Page {
    std::unique_ptr<RwMutex> mtx{ std::make_unique<RwMutex>() };
    std::unique_ptr<std::array<byte, k_page_size>> data{
      std::make_unique<std::array<byte, k_page_size>>() };
    bool changed{ false };
  };

  FileIO m_file;
  RwMutex m_pages_mtx;
  std::unordered_map<uint64_t, Page> m_pages;
};

} // namespace cheesebase
