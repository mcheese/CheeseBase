#pragma once
#include "../model/model.h"

namespace cheesebase {

class Database;

namespace query {

class DbSession {
public:
  DbSession(Database& db) : db_{ db } {}

  model::Value getNamedVal(const std::string& name);

private:
  Database& db_;
};

} // namespace query
} // namespace cheesebase
