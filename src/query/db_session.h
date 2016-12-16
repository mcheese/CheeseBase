#pragma once
#include "../model/model.h"

namespace cheesebase {

class Database;

namespace query {

class DbSession {
public:
  DbSession(Database& db) : db_{ db } {}
  const model::Tuple& getRoot();

private:
  Database& db_;
  boost::optional<model::Tuple> val_;
};

} // namespace query
} // namespace cheesebase
