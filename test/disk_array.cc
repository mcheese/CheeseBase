#include "catch.hpp"
#include "disk_object.h"
#include "disk_array.h"
#include "parser.h"
#include <boost/filesystem.hpp>

using namespace cheesebase;

TEST_CASE("array") {
  boost::filesystem::remove("test.db");
  Database db("test.db");

  SECTION("insert") {
    auto val = parseJson(R"( [ 1, 2, 3, { "A": "a", "B": "b" }, 5 ])");
    Addr root;
    {
      auto ta = db.startTransaction();
      auto tree = disk::ObjectW(ta);
      root = tree.addr();
      tree.insert(ta.key("arr"), *val, disk::Overwrite::Upsert);
      ta.commit(tree.getWrites());
    }
    {
      auto tree = disk::ObjectR(db, root);
      auto read = tree.getChildValue("arr");
      REQUIRE(*val == *read);
    }
  }

  SECTION("append") {
    Addr root;
    {
      auto ta = db.startTransaction();
      auto arr = disk::ArrayW(ta);
      root = arr.addr();
      ta.commit(arr.getWrites());
    }
    {
      auto ta = db.startTransaction();
      auto arr = disk::ArrayW(ta, root);
      auto i1 = arr.append(model::Scalar(std::string("a")));
      auto i2 = arr.append(model::Scalar(std::string("b")));
      auto i3 = arr.append(model::Scalar(std::string("c")));
      REQUIRE(i1 == 0);
      REQUIRE(i2 == 1);
      REQUIRE(i3 == 2);
      ta.commit(arr.getWrites());
    }
    {
      auto arr = disk::ArrayR(db, root);
      auto read = arr.getValue();
      auto val = parseJson(R"( [ "a", "b", "c" ] )");
      REQUIRE(*val == *read);
    }
    {
      auto ta = db.startTransaction();
      auto arr = disk::ArrayW(ta, root);
      arr.remove(1);
      arr.insert(4, model::Scalar(true), disk::Overwrite::Insert);
      arr.append(model::Scalar(false));
      ta.commit(arr.getWrites());
    }
    {
      auto arr = disk::ArrayR(db, root);
      auto read = arr.getValue();
      auto val = parseJson(R"( [ "a", null, "c", null, true, false ] )");
      REQUIRE(*val == *read);
    }
  }
}
