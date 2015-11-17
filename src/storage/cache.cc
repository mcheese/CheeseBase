// Licensed under the Apache License 2.0 (see LICENSE file).

#include "cache.h"

#include "common/log.h"
#include <boost/thread/locks.hpp>

namespace cheesebase { 

Cache::Cache(const std::string& fn, OpenMode m, size_t nr_pages)
  : m_file(fn, m)
  , m_memory(k_page_size * (nr_pages + 1) - 1)
  , m_pages(std::make_unique<Page[]>(nr_pages))
{
  auto overlap = reinterpret_cast<uintptr_t>(m_memory.data()) % k_page_size;
  auto padding = (overlap == 0 ? 0 : k_page_size - overlap);

  for (size_t i = 0; i < nr_pages; ++i) {
    m_pages[i].data =
      gsl::span<byte>(m_memory.data() + padding + (k_page_size * i),
                      k_page_size);

    m_pages[i].less_recent = (i > 0 ? &m_pages[i - 1] : nullptr);
    m_pages[i].more_recent = (i < nr_pages - 1 ? &m_pages[i + 1] : nullptr);
  }
  m_least_recent = &m_pages[0];
  m_most_recent = &m_pages[nr_pages - 1];
}

Cache::~Cache()
{}

ReadRef Cache::read(uint64_t page_nr)
{
  return get_page<PageView, ShLock<UgMutex>>(page_nr);
}

WriteRef Cache::write(uint64_t page_nr)
{
  return WriteRef{ m_map[page_nr]->data,
                   ExLock<UgMutex>{m_map[page_nr]->mutex} };
}

std::pair<gsl::not_null<Cache::Page*>, ExLock<UgMutex>>
Cache::get_free_page(const ExLock<UgMutex>& map_lck)
{
  ExLock<Mutex> lck{ m_pages_mtx };
  auto p = m_least_recent;
  std::pair<Page*, ExLock<UgMutex>> ret{ p, ExLock<UgMutex>(p->mutex) };
  free_page(p, ret.second, map_lck);
  bump_page(p, lck);

  return ret;
}

void Cache::bump_page(gsl::not_null<Page*> p, const ExLock<Mutex>& lck)
{
  Expects(lck.mutex() == &m_pages_mtx);
  Expects(lck.owns_lock());

  // check if already most recent
  if (!p->more_recent)
    return;

  // remove from list
  if (p->less_recent) {
    Expects(p->less_recent->more_recent == p);
    p->less_recent->more_recent = p->more_recent;
  } else {
    Expects(m_least_recent == p);
    m_least_recent = p->more_recent;
  }
  Expects(p->more_recent->less_recent == p);
  p->more_recent->less_recent = p->less_recent;

  // insert at start
  Expects(m_most_recent != p);
  p->less_recent = m_most_recent;
  m_most_recent->more_recent = p;
  p->more_recent = nullptr;
  m_most_recent = p;
}

void Cache::free_page(gsl::not_null<Page*> p,
                      const ExLock<UgMutex>& page_lck,
                      const ExLock<UgMutex>& map_lck)
{
  Expects(page_lck.mutex() == &p->mutex);
  Expects(page_lck.owns_lock());
  Expects(map_lck.mutex() == &m_map_mtx);
  Expects(map_lck.owns_lock());

  if (p->changed != 0) m_disk_worker->writeback(p);
  m_map.erase(p->page_nr);
  p->page_nr = 0;
}

template<class View, class Lock>
LockedRef<View, Lock> Cache::get_page(uint64_t page_nr)
{
  // acquire read access for map
  ShLock<UgMutex> cache_s_lck{ m_map_mtx };

  auto p = m_map.find(page_nr);
  if (p != m_map.end()) {
    // page found, lock and return it
    return{ p->second->data, Lock{p->second->mutex} };

  } else {
    // page not found, create it

    // switch to upgrade lock on map
    cache_s_lck.unlock();
    UgLock<UgMutex> cache_u_lck{ m_map_mtx };
    // now we have access which we can atomically upgrade to exclusive later

    // there might be a saved page now
    p = m_map.find(page_nr);
    if (p != m_map.end()) {
      return{ p->second->data, Lock{p->second->mutex} };
    }

    // upgrade to exclusive access and insert/construct the page
    ExLock<UgMutex> cache_x_lck{ std::move(cache_u_lck) };

    // get a free page
    auto emplaced = m_map.insert(page_nr, get_free_page(cache_x_lck));
    Expects(emplaced.second == true);
    auto& page = emplaced.first->second;

    // just need exclusive page lock for writing content, unlock the cache
    ExLock<UgMutex> page_x_lck{ page->mutex };
    cache_x_lck.unlock();

    m_file.read(page_nr * k_page_size, page->data);

    // downgrade the exclusive page lock to shared ATOMICALLY
    return{ page->data, ShLock<UgMutex>{ std::move(page_x_lck)} };
  }  
  return nullptr;
}

} // namespace cheesebase
