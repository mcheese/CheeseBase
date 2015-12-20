#ifdef _MSC_VER
#define _SCL_SECURE_NO_WARNINGS
#endif
#include "catch.hpp"
#include "model/btree.h"
#include "keycache/keycache.h"
#include <boost/filesystem.hpp>
#include "model/parser.h"

#define private public
#include "core.h"
#undef private

using namespace cheesebase;

const std::string input = R"(
    {
      "0": 1,
      "1": 1,
      "2": 1,
      "3": 1,
      "4": 1,
      "5": 1,
      "6": 1,
      "7": { "hello": 2, "how": 2, "are": 2, "you": 2, "doing": 2 },
      "8": 1,
      "9": 1,
      "A": 1,
      "B": 1,
      "C": 1,
      "D": 1,
      "E": 1,
      "F": 1
    }
)";

TEST_CASE("B+Tree") {
  boost::filesystem::remove("test.db");
  Database db("test.db");
  Addr root;
  auto doc = parseJson(input.begin(), input.end());

  {
    auto ta = db.startTransaction();
    auto node = btree::BtreeWritable(ta);
    root = node.addr();
    for (auto& c : doc) {
      node.insert(ta.key(c.first), *c.second);
    }
    ta.commit(node.getWrites());
  }

  {
    auto ta = db.startTransaction();
    auto node = btree::BtreeWritable(ta, root);
    for (auto& c : doc) {
      if (c.first < "A") node.insert(ta.key(c.first + "#2"), *c.second);
    }
    ta.commit(node.getWrites());
  }

  {
    auto read = btree::BtreeReadOnly(db, root).getObject();
    read.prettyPrint(std::cout) << std::endl;
  }
}
