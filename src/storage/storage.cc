// Licensed under the Apache License 2.0 (see LICENSE file).

#include "storage.h"

#include "fileio.h"

namespace cheesebase {

Storage::Storage(const std::string& filename, OpenMode mode)
  : m_cache(filename, mode)
{}

Storage::~Storage()
{}

ReadRef Storage::load(uint64_t page_nr)
{
  return m_cache.read(page_nr);
}

void Storage::store(uint64_t offset, gsl::span<const byte> data)
{
  offset; data;
}

} // namespace cheesebase
