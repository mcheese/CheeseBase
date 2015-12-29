// Licensed under the Apache License 2.0 (see LICENSE file).

// API

#pragma once
#include <memory>
#include <string>

namespace cheesebase {
class Database;
} // namespace cheesebase

class CheeseBase {
public:
  CheeseBase(const std::string& db_name);
  ~CheeseBase();

  bool insert(const std::string& location, const std::string& json);
  bool update(const std::string& location, const std::string& json);
  bool upsert(const std::string& location, const std::string& json);
  std::string get(const std::string& location);
  bool remove(const std::string& location);

private:
  std::unique_ptr<cheesebase::Database> m_db;
};
