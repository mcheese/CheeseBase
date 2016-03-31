// Licensed under the Apache License 2.0 (see LICENSE file).

// Provides memory pages to read and write from. Recently requested pages are
// cached. Writable pages are marked and later written back to disk.

#pragma once

#include "common.h"
#include "sync.h"
#include <vector>

#include <unordered_map>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <fstream>
#include <list>

namespace cheesebase {

enum class OpenMode {
  create_new,    // Creates new DB if it does not exist.
  create_always, // Creates new DB, always. Overwrite existing DB.
  open_existing, // Opens DB if it exists.
  open_always    // Opens DB, always. Creates new DB if it does not exist.
};

namespace cache_detail {

namespace bi = boost::interprocess;

struct CachePage {
  static constexpr PageNr sUnusedPageNr = static_cast<PageNr>(-1);

  RwMutex mutex;
  boost::interprocess::mapped_region region;
  PageNr page_nr{ sUnusedPageNr };

  template <typename View>
  View getView() const {
    return View(static_cast<Byte*>(region.get_address()), region.get_size());
  }
};

class PageList {
public:
  using iterator = std::list<CachePage>::iterator;
  using const_iterator = std::list<CachePage>::iterator;

  PageList(size_t max_pages) : max_pages_{ max_pages } {};

  // mark p as most recently used (move to front of list)
  void bumpPage(iterator p);

  // return new or old+bumped page and a fitting exclusive lock
  std::pair<iterator, ExLock<RwMutex>> getPage();

  void flush();

private:
  Mutex mtx_;
  std::list<CachePage> pages_;
  size_t max_pages_;
};

class File {
public:
  File(const std::string& filename, OpenMode m);
  bi::mapped_region getRegion(PageNr page_nr);

private:
  uint64_t extendFile(uint64_t size);

  bi::file_mapping file_;
  std::ofstream fstream_;
  uint64_t size_{ 0 };
};

} // namespace cache_detail

////////////////////////////////////////////////////////////////////////////////

// Locked reference of a page.
template <class View>
class PageRef {
public:
  PageRef() = default;

  template <class L>
  PageRef(View page, L&& lock)
      : page_(page), lock_(std::forward<L>(lock)){};

  MOVE_ONLY(PageRef);

  View get() const { return page_; };
  View operator*() const { return page_; };
  const View* operator->() const { return &page_; };

private:
  View page_;
  ShLock<RwMutex> lock_;
};

using ReadRef = PageRef<PageReadView>;
using WriteRef = PageRef<PageWriteView>;

class Cache {
public:
  Cache(const std::string& filename, OpenMode mode, size_t nr_pages);
  ~Cache();

  PageRef<PageReadView> readPage(PageNr page_nr);
  PageRef<PageWriteView> writePage(PageNr page_nr);
  void flush();

private:
  // return specific page, creates it if not found
  template <typename View>
  PageRef<View> getPage(PageNr page_nr);

  // return an unused page, may free the least recently used page
  std::pair<cache_detail::PageList::iterator, ExLock<RwMutex>> getFreePage();



  // flushed page to disk
  void freePage(cache_detail::CachePage& p);

  cache_detail::File file_;
  cache_detail::PageList pages_;

  RwMutex map_mtx_;
  std::unordered_map<PageNr, cache_detail::PageList::iterator> map_;
};

} // namespace cheesebase
