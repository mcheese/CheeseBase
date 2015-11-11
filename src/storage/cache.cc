// Licensed under the Apache License 2.0 (see LICENSE file).

#include "cache.h"

#include "common/log.h"
#include <boost/thread/locks.hpp>

namespace cheesebase { 

Cache::Cache(const std::string& fn, OpenMode m)
  : m_file(fn, m)
{}

Cache::~Cache()
{}

ReadRef Cache::read(uint64_t page_nr)
{
  boost::shared_lock<RwMutex> cache_s_lck{ m_pages_mtx };
  auto p = m_pages.find(page_nr);
  if (p != m_pages.end()) {
    return{ *p->second.data, boost::shared_lock<RwMutex>{*p->second.mtx} };
  } else {
    cache_s_lck.unlock();
    boost::upgrade_lock<RwMutex> cache_u_lck{ m_pages_mtx };
    // now we have access which we can atomically upgrade to exclusive later
    // there might be a saved page now
    p = m_pages.find(page_nr);
    if (p != m_pages.end()) {
      return{ *p->second.data, boost::shared_lock<RwMutex>{*p->second.mtx} };
    }
    // upgrade to exclusive access and insert/construct the page
    boost::unique_lock<RwMutex> cache_x_lck{ std::move(cache_u_lck) };
    // default constructs page
    auto emplaced_page = m_pages.emplace(page_nr, Page{});

    Expects(emplaced_page.second == true);
    // just need exclusive page lock for writing content, unlock the cache
    boost::unique_lock<RwMutex> page_x_lck{ *emplaced_page.first->second.mtx };
    cache_x_lck.unlock();

    m_file.read(page_nr * k_page_size, *emplaced_page.first->second.data);
    // downgrade the exclusive page lock to shared ATOMICALLY
    return{ *emplaced_page.first->second.data,
            boost::shared_lock<RwMutex>{ std::move(page_x_lck)} };
  }
}

WriteRef Cache::write(uint64_t page_nr)
{
  return WriteRef{ *m_pages[page_nr].data,
                   boost::unique_lock<RwMutex>{*m_pages[page_nr].mtx} };
}

} // namespace cheesebase
