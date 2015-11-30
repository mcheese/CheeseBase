#include "catch.hpp"

#include <storage/cache.h>

using namespace cheesebase;

SCENARIO("Reading and writing to cache.") {
  GIVEN("A cache") {
    Cache cache{"test.db", OpenMode::create_always, 8};

    WHEN("data is written to a page") {
      const PageNr page{5};
      const size_t offset{100};
      const std::string test{"ABCDEFGHIJKLMNOP"};
      {
        auto p = cache.write(page);
        copy({test}, p->sub(offset));
      }

      THEN("same data can be read") {
        auto p = cache.read(page);
        REQUIRE(
            std::equal(test.begin(), test.end(), p.get().sub(offset).cbegin()));
      }

      AND_WHEN("many other pages are read") {
        for (PageNr i = 0; i < 20; ++i) cache.read(i);

        THEN("same data can sill be read") {
          auto p = cache.read(page);
          REQUIRE(std::equal(test.begin(), test.end(),
                             p.get().sub(offset).cbegin()));
        }
      }
    }
  }
}
