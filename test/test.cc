#define CATCH_CONFIG_MAIN
#include "catch.hpp"

TEST_CASE("Equal", "test")
{
  REQUIRE(1 == 1);
}

TEST_CASE("Not equal", "test")
{
  REQUIRE(1 != 2);
}

