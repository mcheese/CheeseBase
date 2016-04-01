// Licensed under the Apache License 2.0 (see LICENSE file).

#include "storage.h"

namespace cheesebase {

Storage::Storage(const std::string& filename, OpenMode mode)
    : cache_(filename, mode, k_default_cache_size / k_page_size) {}

PageRef<PageReadView> Storage::loadPage(PageNr page_nr) {
  return cache_.readPage(page_nr);
}

void Storage::storeWrite(Write write) {
  auto p = cache_.writePage(write.addr.pageNr());
  copySpan(write.data, p->subspan(write.addr.pageOffset()));
}

void Storage::storeWrite(std::vector<Write> transaction) {
  // TODO: write to journal here

  // sort the writes to minimize cache requests
  std::sort(transaction.begin(), transaction.end(),
            [](const Write& l, const Write& r) {
              return l.addr.value < r.addr.value;
            });

  auto it = transaction.begin();
  while (it != transaction.end()) {
    auto nr = it->addr.pageNr();
    auto ref = cache_.writePage(nr);
    do {
      copySpan(it->data, ref->subspan(it->addr.pageOffset()));
      ++it;
    } while (it != transaction.end() && nr.value == it->addr.pageNr().value);
  }
}

} // namespace cheesebase
