#define CATCH_CONFIG_MAIN
#include "catch.hpp"

TEST_CASE("Equal", "storage")
{
  REQUIRE(1 == 1);
}

TEST_CASE("Not Equal", "storage")
{
  REQUIRE(1 != 2);
}
