#ifdef _MSC_VER
#define _SCL_SECURE_NO_WARNINGS
#endif
#include "catch.hpp"
#include "keycache.h"
#include "disk_object.h"
#include "parser.h"
#include <boost/filesystem.hpp>

#define private public
#include "core.h"
#undef private

using namespace cheesebase;

std::string stringGen(size_t s) {
  std::string ret;
  for (size_t i = 0; i < s; i++) {
    ret.push_back('a' + (i % 26));
  }
  return ret;
}

void insertTest(Database& db, const std::string& str) {
  auto input = model::Scalar(str);
  Addr root;
  {
    auto ta = db.startTransaction();
    disk::ObjectW tree{ ta };
    root = tree.addr();
    tree.insert(ta.key("X"), input, disk::Overwrite::Upsert);
    ta.commit(tree.getWrites());
  }
  {
    auto read = disk::ObjectR(db, root).getChildValue("X");
    REQUIRE(*read == input);
  }
}

TEST_CASE("insert string") {
  boost::filesystem::remove("test.db");
  Database db("test.db");

  SECTION("length: 5") { insertTest(db, stringGen(5)); }
  SECTION("length: 25") { insertTest(db, stringGen(25)); }
  SECTION("length: 50") { insertTest(db, stringGen(50)); }
  SECTION("length: 100") { insertTest(db, stringGen(100)); }
  SECTION("length: 500") { insertTest(db, stringGen(500)); }
  SECTION("length: 1000") { insertTest(db, stringGen(1000)); }
  SECTION("length: 5000") { insertTest(db, stringGen(5000)); }
  SECTION("length: 10000") { insertTest(db, stringGen(10000)); }
  SECTION("length: 20000") { insertTest(db, stringGen(20000)); }
  SECTION("length: 50000") { insertTest(db, stringGen(50000)); }
}

TEST_CASE("update string") {
  boost::filesystem::remove("test.db");
  Database db("test.db");

  auto longinput = model::Scalar(stringGen(100));
  auto shortinput = model::Scalar(stringGen(10));
  Addr root;
  {
    auto ta = db.startTransaction();
    disk::ObjectW tree{ ta };
    root = tree.addr();
    tree.insert(ta.key("LL"), longinput, disk::Overwrite::Upsert);
    tree.insert(ta.key("LS"), longinput, disk::Overwrite::Upsert);
    tree.insert(ta.key("SL"), shortinput, disk::Overwrite::Upsert);
    tree.insert(ta.key("SS"), shortinput, disk::Overwrite::Upsert);
    ta.commit(tree.getWrites());
  }
  {
    auto ta = db.startTransaction();
    disk::ObjectW tree{ ta, root };
    tree.insert(ta.key("LL"), longinput, disk::Overwrite::Upsert);
    tree.insert(ta.key("LS"), shortinput, disk::Overwrite::Upsert);
    tree.insert(ta.key("SL"), longinput, disk::Overwrite::Upsert);
    tree.insert(ta.key("SS"), shortinput, disk::Overwrite::Upsert);
    ta.commit(tree.getWrites());
  }
  {
    disk::ObjectR tree{ db, root };
    REQUIRE(*tree.getChildValue("LL") == longinput);
    REQUIRE(*tree.getChildValue("LS") == shortinput);
    REQUIRE(*tree.getChildValue("SL") == longinput);
    REQUIRE(*tree.getChildValue("SS") == shortinput);
  }
}
