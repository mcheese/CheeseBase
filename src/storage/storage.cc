// Licensed under the Apache License 2.0 (see LICENSE file).

#include "storage.h"

#include "fileio.h"

namespace cheesebase {

Storage::Storage(const std::string& filename, OpenMode mode)
    : m_cache(filename, mode, k_default_cache_size / k_page_size) {}

ReadRef Storage::load(PageNr page_nr) { return m_cache.read(page_nr); }

void Storage::store(Write write) {
  auto p = m_cache.write(page_nr(write.addr));
  copy(write.data, p.get_write().sub(page_offset(write.addr)));
}

void Storage::store(std::vector<Write>& transaction) {
  // TODO: write to journal here

  // sort the writes to minimize cache requests
  std::sort(transaction.begin(), transaction.end(),
            [](const Write& l, const Write& r) { return l.addr < r.addr; });

  auto it = transaction.begin();
  while (it != transaction.end()) {
    auto nr = page_nr(it->addr);
    auto ref = m_cache.write(nr);
    do {
      copy(it->data, ref.get_write().sub(page_offset(it->addr)));
      ++it;
    } while (it != transaction.end() && nr == page_nr(it->addr));
  }
}

} // namespace cheesebase
