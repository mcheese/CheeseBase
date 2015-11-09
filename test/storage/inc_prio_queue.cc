#include "catch.hpp"

#include "storage/inc_prio_queue.h"

using namespace cheesebase;

SCENARIO("Queue and enqueue")
{
  GIVEN("An empty IncPrioQueue")
  {
    IncPrioQueue<size_t, std::string> q{};
    THEN("its size is 0")
      REQUIRE(q.size() == 0);
    WHEN("a value is inserted")
    {
      const std::string test_value{ "test" };
      q.enqueue(0, test_value);
      THEN("its size is 1")
        REQUIRE(q.size() == 1);
      AND_WHEN("a value is retrieved")
      {
        auto ret = q.dequeue();
        THEN("it is the same value")
          REQUIRE(ret == test_value);
        THEN("its size is 0 again")
          REQUIRE(q.size() == 0);
      }
    }
  }
}
