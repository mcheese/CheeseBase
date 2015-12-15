// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "common/common.h"

#include <string>
#include <memory>

namespace cheesebase {

DEF_EXCEPTION(DatabaseError);

class Storage;
class Allocator;
class KeyCache;

class Database {
public:
  Database(const std::string& name);

private:
  // for test cases
  Database() = default;
  std::unique_ptr<Storage> m_store;
  std::unique_ptr<Allocator> m_alloc;
  std::unique_ptr<KeyCache> m_keycache;
};

} // namespace cheesebase
