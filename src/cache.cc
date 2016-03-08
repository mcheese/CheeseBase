// Licensed under the Apache License 2.0 (see LICENSE file).

#include "cache.h"
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

Cache::Cache(const std::string& fn, OpenMode m, size_t nr_pages) {
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

  // fstream_.seekp(0, std::ios_base::end);
  size_ = fstream_.tellp();

  file_ = bi::file_mapping(fn.c_str(), bi::read_write);
  pages_ = std::make_unique<CachePage[]>(nr_pages);
  for (size_t i = 0; i < nr_pages; ++i) {
    pages_[i].less_recent = (i > 0 ? &pages_[i - 1] : nullptr);
    pages_[i].more_recent = (i < nr_pages - 1 ? &pages_[i + 1] : nullptr);
  }
  least_recent_ = &pages_[0];
  most_recent_ = &pages_[nr_pages - 1];
}

Cache::~Cache() { flush(); }

ReadRef Cache::readPage(PageNr page_nr) {
  auto p = getPage<ShLock<RwMutex>>(page_nr);
  return { p.first.data, std::move(p.second) };
}

WriteRef Cache::writePage(PageNr page_nr) {
  auto p = getPage<ExLock<RwMutex>>(page_nr);
  return { p.first.data, std::move(p.second) };
}

std::pair<CachePage&, ExLock<RwMutex>>
Cache::GetFreePage(const ExLock<RwMutex>& map_lck) {
  ExLock<Mutex> lck{ pages_mtx_ };
  auto& p = *least_recent_;
  std::pair<CachePage&, ExLock<RwMutex>> ret{ p, ExLock<RwMutex>(p.mutex) };
  freePage(p, ret.second, map_lck);
  bumpPage(p, lck);

  return ret;
}

void Cache::bumpPage(CachePage& p, const ExLock<Mutex>& lck) {
  Expects(lck.mutex() == &pages_mtx_);
  Expects(lck.owns_lock());

  // check if already most recent
  if (!p.more_recent) return;

  // remove from list
  if (p.less_recent) {
    Expects(p.less_recent->more_recent == &p);
    p.less_recent->more_recent = p.more_recent;
  } else {
    Expects(least_recent_ == &p);
    least_recent_ = p.more_recent;
  }
  Expects(p.more_recent->less_recent == &p);
  p.more_recent->less_recent = p.less_recent;

  // insert at start
  Expects(most_recent_ != &p);
  p.less_recent = most_recent_;
  most_recent_->more_recent = &p;
  p.more_recent = nullptr;
  most_recent_ = &p;
}

void Cache::freePage(CachePage& p, const ExLock<RwMutex>& page_lck,
                     const ExLock<RwMutex>& map_lck) {
  Expects(page_lck.mutex() == &p.mutex);
  Expects(page_lck.owns_lock());
  Expects(map_lck.mutex() == &map_mtx_);
  Expects(map_lck.owns_lock());

  if (p.changed == true) {
    p.region.flush(0, k_page_size, false);
    p.changed = false;
  }
  map_.erase(p.page_nr);
  p.region = {};
  p.page_nr = static_cast<PageNr>(-1);
}

template <class Lock>
std::pair<CachePage&, Lock> Cache::getPage(PageNr page_nr) {
  // acquire read access for map
  ShLock<RwMutex> cache_s_lck{ map_mtx_ };

  auto p = map_.find(page_nr);
  if (p != map_.end()) {
    // page found, lock and return it
    bumpPage(p->second, ExLock<Mutex>(pages_mtx_));
    return { p->second, Lock{ p->second.mutex } };
  } else {
    // page not found, create it

    // switch to write lock on map
    cache_s_lck.unlock();
    ExLock<RwMutex> cache_x_lck{ map_mtx_ };

    // there might be a saved page now
    p = map_.find(page_nr);
    if (p != map_.end()) {
      bumpPage(p->second, ExLock<Mutex>(pages_mtx_));
      return { p->second, Lock{ p->second.mutex } };
    }

    // get a free page
    auto new_page = GetFreePage(cache_x_lck);
    auto& page_x_lck = new_page.second;
    auto& page = new_page.first;
    auto inserted = map_.insert({ page_nr, page });
    Expects(inserted.second == true);

    // just need exclusive page lock for writing content, unlock the cache
    cache_x_lck.unlock();

    page.page_nr = page_nr;
    if ((page_nr + 1) * k_page_size > size_) {
      size_ = extendFile((page_nr + 8) * k_page_size);
    }
    page.region = bi::mapped_region(file_, bi::read_write,
                                    page_nr * k_page_size, k_page_size);
    page.data = gsl::span<Byte>(static_cast<Byte*>(page.region.get_address()),
                                k_page_size);
    // downgrade the exclusive page lock to shared ATOMICALLY
    return { page, Lock{ std::move(page_x_lck) } };
  }
}

void Cache::flush() {
  boost::lock_guard<Mutex> lck{ pages_mtx_ };
  for (auto p = least_recent_; p != nullptr; p = p->more_recent) {
    if (p->changed) {
      if (!p->region.flush(0, k_page_size, false))
        throw FileError("failed to flush page");
    }
  }
}

} // namespace cheesebase
