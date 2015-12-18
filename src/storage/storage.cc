// Licensed under the Apache License 2.0 (see LICENSE file).

#include "storage.h"

#include "fileio.h"

namespace cheesebase {

Storage::Storage(const std::string& filename, OpenMode mode)
    : m_cache(filename, mode, k_default_cache_size / k_page_size) {}

ReadRef Storage::loadPage(PageNr page_nr) { return m_cache.readPage(page_nr); }

void Storage::storeWrite(Write write) {
  auto p = m_cache.writePage(toPageNr(write.addr));
  copySpan(write.data, p->subspan(toPageOffset(write.addr)));
}

void Storage::storeWrite(std::vector<Write> transaction) {
  // TODO: write to journal here

  // sort the writes to minimize cache requests
  std::sort(transaction.begin(), transaction.end(),
            [](const Write& l, const Write& r) { return l.addr < r.addr; });

  auto it = transaction.begin();
  while (it != transaction.end()) {
    auto nr = toPageNr(it->addr);
    auto ref = m_cache.writePage(nr);
    do {
      copySpan(it->data, ref->subspan(toPageOffset(it->addr)));
      ++it;
    } while (it != transaction.end() && nr == toPageNr(it->addr));
  }
}

} // namespace cheesebase
