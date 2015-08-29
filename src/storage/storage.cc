// Copyright 2015 Max 'Cheese'.
// Licensed under the Apache License 2.0 (see LICENSE file).

#include "storage.h"

using namespace std;

namespace cheesebase {

Storage::Storage(const std::string & filename, Mode mode)
{}

Storage::~Storage()
{}

ReadPtr Storage::load(const uint64_t page_nr)
{
  return ReadPtr(new byte[kPageSize], shared_lock<shared_timed_mutex>{});
}

void Storage::store(const uint64_t offset, const byte * const buf, const size_t size)
{}

} // namespace cheesebase
