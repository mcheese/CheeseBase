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

TEST_CASE("B+Tree") {
  boost::filesystem::remove("test.db");
  Database db("test.db");
  auto ta = db.startTransaction();

  std::string input = R"(
    {
      "b": "foo",
      "c": true,
      "d": null,
      "e": false,
      "f": "short string",
      "g": { "a": "a long short string", "xd":null, "sub object":
{ "heh": 1337, "lolasdasd": "1337", "f": "abcdefksdlabcjfldsoe"} }
    }
        )";

  auto doc = parseJson(input.begin(), input.end());

  auto node = btree::BtreeWritable(ta);
  for (auto& c : doc) {
    node.insert(ta.key(c.first), *c.second);
  }

  ta.commit(node.getWrites());
}
