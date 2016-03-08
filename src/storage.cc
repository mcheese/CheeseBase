// Licensed under the Apache License 2.0 (see LICENSE file).

#include "storage.h"

namespace cheesebase {

Storage::Storage(const std::string& filename, OpenMode mode)
    : cache_(filename, mode, k_default_cache_size / k_page_size) {}

ReadRef Storage::loadPage(PageNr page_nr) { return cache_.readPage(page_nr); }

void Storage::storeWrite(Write write) {
  auto p = cache_.writePage(toPageNr(write.addr));
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
    auto ref = cache_.writePage(nr);
    do {
      copySpan(it->data, ref->subspan(toPageOffset(it->addr)));
      ++it;
    } while (it != transaction.end() && nr == toPageNr(it->addr));
  }
}

} // namespace cheesebase
