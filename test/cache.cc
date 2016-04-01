#ifdef _MSC_VER
#define _SCL_SECURE_NO_WARNINGS
#endif
#include "catch.hpp"
#include "cache.h"

using namespace cheesebase;

SCENARIO("CACHE") {
  GIVEN("A cache") {
    Cache cache{ "test.db", OpenMode::create_always, 8 };

    WHEN("data is written to a page") {
      const PageNr page{ 5 };
      const size_t offset{ 100 };
      const std::string test{ "ABCDEFGHIJKLMNOP" };
      {
        auto p = cache.writePage(page);
        copySpan(gsl::as_bytes(gsl::span<const char>(test)),
                 p->subspan(offset));
      }

      THEN("same data can be read") {
        auto p = cache.readPage(page);
        REQUIRE(gsl::as_bytes(gsl::span<const char>(test)) ==
                p->subspan(offset, test.size()));
      }

      AND_WHEN("many other pages are read") {
        for (size_t i = 0; i < 20; ++i) cache.readPage(PageNr(i));

        THEN("same data can sill be read") {
          auto p = cache.readPage(page);
          REQUIRE(gsl::as_bytes(gsl::span<const char>(test)) ==
                  p->subspan(offset, test.size()));
        }
      }
    }
  }
}
