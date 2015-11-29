#include "catch.hpp"

#include "storage/inc_prio_queue.h"

using namespace cheesebase;

SCENARIO("Queue and enqueue") {
  GIVEN("An empty IncPrioQueue") {
    IncPrioQueue<size_t, std::string> q{};
    THEN("its size is 0")
    REQUIRE(q.size() == 0);
    WHEN("a value is inserted") {
      const std::string test_value{"test"};
      q.enqueue(0, test_value);
      THEN("its size is 1")
      REQUIRE(q.size() == 1);
      AND_WHEN("a value is retrieved") {
        auto ret = q.dequeue();
        THEN("it is the same value")
        REQUIRE(ret == test_value);
        THEN("its size is 0 again")
        REQUIRE(q.size() == 0);
      }
    }

    WHEN("many values are inserted") {
      q.enqueue(10, "10");
      q.enqueue(12, "12");
      q.enqueue(5, "5");
      q.enqueue(7, "7");
      q.enqueue(1, "1");
      q.enqueue(8, "8");
      REQUIRE(q.size() == 6);
      // 1,5,7,8,10,12

      THEN("they extract in the correct order") {
        REQUIRE(q.dequeue() == "1");
        REQUIRE(q.dequeue() == "5");
        REQUIRE(q.dequeue() == "7");
        REQUIRE(q.size() == 3);

        AND_WHEN("new values are inserted") {
          q.enqueue(1, "1");
          q.enqueue(11, "11");
          q.enqueue(3, "3");
          q.enqueue(2, "2");
          REQUIRE(q.size() == 7);
          // 8,10,11,12,1,2,3

          THEN("they extract in the correct order") {
            REQUIRE(q.dequeue() == "8");
            REQUIRE(q.dequeue() == "10");
            REQUIRE(q.dequeue() == "11");
            REQUIRE(q.size() == 4);
            REQUIRE(q.exchange(12, "12*") == "12*"); // [12*],12,1,2,3
            REQUIRE(q.exchange(5, "5") == "12");     // [12],1,2,3,5
            REQUIRE(q.exchange(4, "4") == "1");      // [1],2,3,4,5
            REQUIRE(q.size() == 4);
            REQUIRE(q.dequeue() == "2");
            REQUIRE(q.dequeue() == "3");
            REQUIRE(q.dequeue() == "4");
            REQUIRE(q.dequeue() == "5");
            REQUIRE(q.size() == 0);
          }
        }
      }
    }
  }

  GIVEN("An empty IncPrioQueue with move-only-value") {
    IncPrioQueue<size_t, std::unique_ptr<std::string>> q{};
    WHEN("values are inserted") {
      q.enqueue(0, std::make_unique<std::string>("0"));
      q.enqueue(5, std::make_unique<std::string>("5"));
      q.enqueue(3, std::make_unique<std::string>("3"));
      REQUIRE(q.size() == 3);

      THEN("values can be retrieved in correct order") {
        REQUIRE(*q.dequeue() == "0");
        REQUIRE(*q.dequeue() == "3");
        REQUIRE(*q.exchange(2, std::make_unique<std::string>("2")) == "5");
        REQUIRE(*q.exchange(1, std::make_unique<std::string>("1")) == "1");
        REQUIRE(*q.dequeue() == "2");
        REQUIRE(q.size() == 0);
      }
    }
  }
}
