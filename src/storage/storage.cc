// Licensed under the Apache License 2.0 (see LICENSE file).

#include "storage.h"

using namespace std;

namespace cheesebase {

Storage::Storage(const string& filename, const OpenMode mode)
{
  filename; mode;
}

Storage::~Storage()
{}

PageRef Storage::load(const uint64_t page_nr)
{
  page_nr;
  return PageRef{ PageView{ new byte[k_page_size], k_page_size },
                  shared_lock<shared_timed_mutex>{} };
}

void Storage::store(const uint64_t offset, gsl::array_view<const byte> data)
{
  offset; data;
}

} // namespace cheesebase
