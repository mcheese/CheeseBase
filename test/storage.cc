#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "storage/storage.h"
#include "common/common.h"

#include <array>
#include <algorithm>

using namespace cheesebase;
using namespace std;

SCENARIO("stored data can be read")
{
  GIVEN("A Storage and data")
  {
    Storage store{ "test", Storage::OpenMode::create_always };
    const size_t size{ 500 };
    const size_t offset{ 50'000 };
    vector<byte> data;
    data.reserve(size);
    for (size_t i = 0; i < size; ++i)
      data.push_back(static_cast<byte>(i));


    WHEN("data is stored")
    {
      store.store(offset, data.data(),
                  data.size());

      AND_WHEN("same data is loaded")
      {
        array<byte, size> loaded;
        size_t to_read{ size };
        size_t pos{ 0 };
        while (to_read > 0) {
          auto page = store.load(page_nr(offset + pos));
          auto curr_read = std::min<size_t>(
                               to_read,
                               k_page_size - page_offset(offset + pos));

          copy_n(page->begin() + page_offset(offset + pos),
                 curr_read,
                 loaded.begin() + pos);

          pos += curr_read;
          to_read -= curr_read;
        }

        THEN("data is equal")
        {
          REQUIRE(equal(data.begin(), data.end(), loaded.begin()));
        }
      }
    }
  }
}
