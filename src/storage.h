// Licensed under the Apache License 2.0 (see LICENSE file).

// Manage storage of the database. Hold a DB file and provide load/store
// operations on the data as well as caching.

#pragma once

#include "common.h"
#include "cache.h"

#include <string>

namespace cheesebase {

// Disk representation of a database instance. Opens DB file and journal on
// construction. Provides load/store access backed by a cache.
class Storage {
public:
  // Create a Storage associated with a DB and journal file. Opens an existing
  // database or creates a new one bases on "mode" argument.
  Storage(const std::string& filename, OpenMode mode);

  // Get a DB page. Reads the requested page in the cache (if needed) and
  // returns a PageReadRef object holding a read-locked reference of the page.
  // The referenced page is guaranteed to be valid and unchanged for the
  // lifetime of the object.
  PageRef<PageReadView> loadPage(PageNr page_nr);

  template <std::ptrdiff_t S>
  PageRef<Span<const Byte, S>> loadBlock(Addr addr) {
    auto ref = loadPage(addr.pageNr());
    return PageRef<Span<const Byte, S>>(ref->subspan(addr.pageOffset(), S),
                                        std::move(ref));
  }

  // Write data to the DB. Old data is overwritten and the file extended if
  // needed. The caller has to handle consistency of the database.
  // The write is guaranteed to be all-or-nothing. On return of the function
  // the journal has been written and persistence of the write is guaranteed.
  void storeWrite(Write write);

  void storeWrite(std::vector<Write> transaction);

private:
  Cache cache_;
};

} // namespace cheesebase
