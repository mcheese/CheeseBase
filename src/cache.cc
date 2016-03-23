// Licensed under the Apache License 2.0 (see LICENSE file).

#include "cache.h"
#include "exceptions.h"
#include <boost/filesystem.hpp>
#include <fstream>

namespace cheesebase {

namespace bi = boost::interprocess;
namespace fs = boost::filesystem;

uint64_t Cache::extendFile(uint64_t size) {
  Expects(size > size_);
  for (uint64_t i = 0; i < size - size_; ++i) fstream_.put('\xAA');
  if (fstream_.bad()) throw FileError("failed extending file");
  fstream_.flush();
  if (fstream_.bad()) throw FileError("failed extending file");
  return size;
}

Cache::Cache(const std::string& fn, OpenMode m, size_t nr_pages)
    : pages_{ nr_pages } {
  auto exists = fs::exists(fn);
  if (m == OpenMode::create_new && exists)
    throw FileError("file already exists");
  if (m == OpenMode::open_existing && !exists)
    throw FileError("file not found");
  if (m == OpenMode::create_always && exists) fs::remove(fn);

  fstream_.open(fn, std::ios_base::out | std::ios_base::binary |
                        std::ios_base::app);
  if (!fstream_.is_open()) throw FileError("could not open file");

  if (m == OpenMode::create_new || m == OpenMode::create_always || !exists)
    extendFile(k_page_size * 8);

  size_ = fstream_.tellp();

  file_ = bi::file_mapping(fn.c_str(), bi::read_write);
}

Cache::~Cache() { flush(); }

PageRef<PageReadView> Cache::readPage(PageNr page_nr) {
  return getPage<PageReadView>(page_nr);
}

PageRef<PageWriteView> Cache::writePage(PageNr page_nr) {
  return getPage<PageWriteView>(page_nr);
}

void PageList::bumpPage(const_iterator p) {
  Guard<Mutex> _ { mtx_ };
  pages_.splice(pages_.begin(), pages_, p);
}

std::pair<PageList::iterator, ExLock<RwMutex>> PageList::getPage() {
  Guard<Mutex> _ { mtx_ };

  if (pages_.size() < max_pages_) {
    pages_.emplace_front();
  } else {
    // move last to front
    pages_.splice(pages_.begin(), pages_, --(pages_.end()));
  }

  auto it = pages_.begin();
  return { it, ExLock<RwMutex>(it->mutex) };
}

std::pair<PageList::iterator, ExLock<RwMutex>> Cache::getFreePage() {
  auto pair = pages_.getPage();
  if(pair.first->page_nr != CachePage::sUnusedPageNr) {
    freePage(*pair.first);
  }
  return pair;
}

void Cache::freePage(CachePage& p) {
  p.region.flush(0, k_page_size, false);
  map_.erase(p.page_nr);
  p.region = {};
  p.page_nr = CachePage::sUnusedPageNr;
}

template <class View>
PageRef<View> Cache::getPage(PageNr page_nr) {
  // acquire read access for map
  ShLock<RwMutex> cache_s_lck{ map_mtx_ };

  auto p = map_.find(page_nr);
  if (p != map_.end()) {
    // page found, lock and return it
    pages_.bumpPage(p->second);
    return { p->second->getView<View>(), p->second->mutex };

  } else {
    // page not found, create it

    // switch to write lock on map
    cache_s_lck.unlock();
    ExLock<RwMutex> cache_x_lck{ map_mtx_ };

    // there might be a saved page now
    p = map_.find(page_nr);
    if (p != map_.end()) {
      pages_.bumpPage(p->second);
      return { p->second->getView<View>(), p->second->mutex };
    }

    // get a free page
    PageList::iterator page;
    ExLock<RwMutex> page_lock;
    std::tie(page, page_lock) = getFreePage();

    bool success;
    std::tie(std::ignore, success) = map_.emplace(page_nr, page);
    Expects(success);

    // just need exclusive page lock for writing content, unlock the cache
    cache_x_lck.unlock();

    page->page_nr = page_nr;
    if ((page_nr + 1) * k_page_size > size_) {
      size_ = extendFile((page_nr + 8) * k_page_size);
    }
    page->region = bi::mapped_region(file_, bi::read_write,
                                     page_nr * k_page_size, k_page_size);

    // downgrade the exclusive page lock to shared ATOMICALLY
    return { page->getView<View>(), std::move(page_lock) };
  }
}

void Cache::flush() {
  pages_.flush();
}

void PageList::flush() {
  Guard<Mutex> _ { mtx_ };
  for (auto& p : pages_) {
    if (!p.region.flush(0, k_page_size, false))
      throw FileError("failed to flush page");
  }
}

} // namespace cheesebase
