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

const std::string input = R"(
    {
      "0": 1,
      "1": 2,
      "2": 3,
      "3": 4,
      "4": "blublub",
      "5": 6,
      "6": 7,
      "7": 8,
      "8": {"hey": 1, "just": false, "met":true ,
            "you": null, "and this is crazy": "but here is my #" },
      "9": 9,
      "A": 10,
      "B": "",
      "C": 12,
      "D": 13,
      "E": 14,
      "F": 15
    }
)";
//"8": { "hey": true, "bla": false, "what": null, "is": 1337.456, "up":
//"asdasdasdasdada"},
const std::string input_short = R"(
{"a": "hey", "b": 3, "c": null, "d": true, "e": 1}
)";

TEST_CASE("B+Tree insert") {
  boost::filesystem::remove("test.db");
  Database db("test.db");

  SECTION("short") {
    Addr root;
    auto parsed = parseJson(input_short.begin(), input_short.end());
    auto& doc = dynamic_cast<model::Object&>(*parsed);
    {
      auto ta = db.startTransaction();
      disk::ObjectW tree{ ta };
      root = tree.addr();
      for (auto& c : doc)
        tree.insert(ta.key(c.first), *c.second, disk::Overwrite::Upsert);
      ta.commit(tree.getWrites());
    }
    {
      auto read = disk::ObjectR(db, root).getObject();
      REQUIRE(read == doc);
    }
  }

  SECTION("splits") {
    Addr root;
    auto parsed = parseJson(input.begin(), input.end());
    auto& doc = dynamic_cast<model::Object&>(*parsed);
    {
      auto ta = db.startTransaction();
      disk::ObjectW node{ ta };
      root = node.addr();
      for (auto& c : doc) {
        node.insert(ta.key(c.first), *c.second, disk::Overwrite::Upsert);
      }
      ta.commit(node.getWrites());
    }

    {
      auto read = disk::ObjectR(db, root).getObject();
      REQUIRE(read == doc);
    }

    SECTION("extend without split") {
      {
        auto ta = db.startTransaction();
        disk::ObjectW node{ ta, root };
        for (auto& c : doc) {
          if (c.first < "A")
            node.insert(ta.key(c.first + "#2"), *c.second,
                        disk::Overwrite::Upsert);
        }
        ta.commit(node.getWrites());
      }
      {
        auto read = disk::ObjectR(db, root).getObject();
        REQUIRE(read != doc);
        for (auto& c : doc) {
          REQUIRE(*c.second == *read.getChild(c.first));
        }
        for (auto& c : doc) {
          if (c.first < "A")
            REQUIRE(*c.second == *read.getChild(c.first + "#2"));
        }
      }
    }
    SECTION("extend with split to 3 leafs") {
      {
        auto ta = db.startTransaction();
        disk::ObjectW node{ ta, root };
        for (auto& c : doc) {
          node.insert(ta.key(c.first + "#2"), *c.second,
                      disk::Overwrite::Upsert);
        }
        ta.commit(node.getWrites());
      }

      {
        auto read = disk::ObjectR(db, root).getObject();
        REQUIRE(read != doc);
        for (auto& c : doc) {
          REQUIRE(*c.second == *read.getChild(c.first));
          REQUIRE(*c.second == *read.getChild(c.first + "#2"));
        }
      }
    }
    const int times = 30;
    SECTION("extend with split to many leafs") {
      for (size_t i = 0; i < times; ++i) {
        auto ta = db.startTransaction();
        disk::ObjectW node{ ta, root };
        for (auto& c : doc) {
          node.insert(ta.key(c.first + "#" + std::to_string(i)), *c.second,
                      disk::Overwrite::Upsert);
        }
        ta.commit(node.getWrites());
      }

      SECTION("read all values") {
        auto read = disk::ObjectR(db, root).getObject();
        for (auto& c : doc) {
          REQUIRE(*c.second == *read.getChild(c.first));
          for (size_t i = 0; i < times; ++i) {
            REQUIRE(*c.second ==
                    *read.getChild(c.first + "#" + std::to_string(i)));
          }
        }
      }
      SECTION("read specific values") {
        for (auto& c : doc) {
          auto read = disk::ObjectR(db, root).getChildValue(c.first);
          REQUIRE(read);
          REQUIRE(*read == *c.second);
          for (size_t i = 0; i < times; ++i) {
            auto read = disk::ObjectR(db, root)
                            .getChildValue(c.first + "#" + std::to_string(i));
            REQUIRE(read);
            REQUIRE(*read == *c.second);
          }
        }
      }
    }
    SECTION("extend and merge") {
      {
        auto ta = db.startTransaction();
        disk::ObjectW tree{ ta, root };
        for (size_t i = 0; i < times; ++i) {
          for (auto& c : doc) {
            tree.insert(ta.key(c.first + "#" + std::to_string(i)), *c.second,
                        disk::Overwrite::Upsert);
          }
        }
        ta.commit(tree.getWrites());
      }

      SECTION("delete half") {
        // delete half
        {
          auto ta = db.startTransaction();
          disk::ObjectW tree{ ta, root };
          for (size_t i = 0; i < times / 2; ++i) {
            for (auto& c : doc) {
              auto del = tree.remove(c.first + "#" + std::to_string(i));
              REQUIRE(del);
            }
          }
          ta.commit(tree.getWrites());
        }
        // read and compare to expected
        {
          for (auto& c : doc) {
            auto read = disk::ObjectR(db, root).getChildValue(c.first);
            REQUIRE(read);
            REQUIRE(*read == *c.second);
            for (size_t i = 0; i < times / 2; ++i) {
              auto read = disk::ObjectR(db, root)
                              .getChildValue(c.first + "#" + std::to_string(i));
              if (i < times) {
                REQUIRE_FALSE(read);
              } else {
                REQUIRE(read);
                REQUIRE(*read == *c.second);
              }
            }
          }
        }
        {
          auto read = disk::ObjectR(db, root).getObject();
          for (auto& c : doc) {
            REQUIRE(*read.getChild(c.first) == *c.second);
            for (size_t i = 0; i < times / 2; ++i) {
              if (i < times) {
                REQUIRE_FALSE(read.getChild(c.first + "#" + std::to_string(i))
                                  .is_initialized());
              } else {
                REQUIRE(*read.getChild(c.first + "#" + std::to_string(i)) ==
                        *c.second);
              }
            }
          }
        }
      }

      SECTION("delete all") {
        {
          auto ta = db.startTransaction();
          disk::ObjectW tree{ ta, root };
          for (auto& c : doc) {
            tree.remove(c.first);
          }
          for (size_t i = 0; i < times; ++i) {
            for (auto& c : doc) {
              tree.remove(c.first + "#" + std::to_string(i));
            }
          }
          ta.commit(tree.getWrites());
        }
        // read and compare to expected
        {
          for (auto& c : doc) {
            auto read = disk::ObjectR(db, root).getChildValue(c.first);
            REQUIRE_FALSE(read);
            for (size_t i = 0; i < times / 2; ++i) {
              auto read = disk::ObjectR(db, root)
                              .getChildValue(c.first + "#" + std::to_string(i));
              REQUIRE_FALSE(read);
            }
          }
        }
        {
          auto read = disk::ObjectR(db, root).getObject();
          for (auto& c : doc) {
            REQUIRE_FALSE(read.getChild(c.first).is_initialized());
            for (size_t i = 0; i < times / 2; ++i) {
              REQUIRE_FALSE(read.getChild(c.first + "#" + std::to_string(i))
                                .is_initialized());
            }
          }
        }
      }
    }
  }
}
