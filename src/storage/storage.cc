// Copyright 2015 Max 'Cheese'.
// Licensed under the Apache License 2.0 (see LICENSE file).

#include "storage.h"

using namespace std;

namespace cheesebase {

Storage::Storage(const std::string & filename, Mode mode)
{
  filename; mode;
}

Storage::~Storage()
{}

PageReadRef Storage::load(const uint64_t page_nr)
{
  page_nr;
  return PageReadRef(*(new Page()), shared_lock<shared_timed_mutex>{});
}

void Storage::store(const uint64_t offset, const byte * const buf, const size_t size)
{
  offset; buf; size;
}

} // namespace cheesebase
