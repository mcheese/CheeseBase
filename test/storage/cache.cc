#include "catch.hpp"

#include <storage/cache.h>

using namespace cheesebase;

SCENARIO("Reading and writing to cache.")
{
  GIVEN("A cache")
  {
    Cache cache{ "test.db", OpenMode::create_always, 8 };
  }
}
