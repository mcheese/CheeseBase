// Licensed under the Apache License 2.0 (see LICENSE file).

// API

#pragma once
#include <memory>
#include <string>
#include <model.h>

namespace cheesebase {
class Database;
class CheeseBase;

using Location = std::vector<boost::variant<std::string, uint64_t>>;

class Query {
  friend CheeseBase;

public:
  Query operator[](std::string key);
  Query operator[](uint64_t index);

  void insert(const std::string& key, const model::Value&);
  void insert(const uint64_t, const model::Value&);
  void insert(const model::Value&);
  void update(const std::string& key, const model::Value&);
  void update(const uint64_t, const model::Value&);
  void update(const model::Value&);
  void upsert(const std::string& key, const model::Value&);
  void upsert(const uint64_t, const model::Value&);
  void upsert(const model::Value&);
  std::unique_ptr<model::Value> get() const;
  void remove();

private:
  Query(CheeseBase&, Location, std::string key);
  Query(CheeseBase&, Location, uint64_t index);

  CheeseBase& cb_;
  Location location_;
};

class CheeseBase {
  friend Query;

public:
  explicit CheeseBase(const std::string& db_name);
  ~CheeseBase();

  Query operator[](std::string key);
  Query operator[](uint64_t index);

  void insert(const std::string& key, const model::Value&,
              const Location& = {});
  void insert(uint64_t index, const model::Value&, const Location& = {});

  void update(const std::string& key, const model::Value&,
              const Location& = {});
  void update(uint64_t index, const model::Value&, const Location& = {});

  void upsert(const std::string& key, const model::Value&,
              const Location& = {});
  void upsert(uint64_t index, const model::Value&, const Location& = {});

  std::unique_ptr<model::Value> get(const Location&) const;

  void remove(const Location&);

private:
  std::unique_ptr<cheesebase::Database> db_;
};

} // namespace cheesebase
