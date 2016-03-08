#include "catch.hpp"
#include "storage.h"
#include "common.h"

#include <array>
#include <algorithm>

using namespace cheesebase;
using namespace std;

SCENARIO("stored data can be read") {
  GIVEN("A Storage and data") {
    Storage store{ "test.db", OpenMode::create_always };
    const size_t size{ 500 };
    const size_t offset{ 50'000 };
    vector<Byte> data;
    data.reserve(size);
    for (size_t i = 0; i < size; ++i) data.push_back(static_cast<Byte>(i));

    WHEN("data is stored") {
      store.storeWrite({ offset, data });

      AND_WHEN("same data is loaded") {
        array<Byte, size> loaded;
        size_t to_read{ size };
        size_t pos{ 0 };
        while (to_read > 0) {

          auto page = store.loadPage(toPageNr(offset + pos));
          auto curr_read = std::min<size_t>(
              to_read, k_page_size - toPageOffset(offset + pos));

          copy_n(page->begin() + toPageOffset(offset + pos), curr_read,
                 loaded.begin() + pos);

          pos += curr_read;
          to_read -= curr_read;
        }

        THEN("data is equal") {
          REQUIRE(equal(data.begin(), data.end(), loaded.begin()));
        }
      }
    }
  }
}
