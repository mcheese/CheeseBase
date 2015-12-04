#include "catch.hpp"

#include "storage/fileio.h"
#include <gsl.h>
#include <random>

using namespace cheesebase;

void put_random_bytes(gsl::span<Byte> memory) {
  std::random_device rd;
  std::mt19937 mt{ rd() };

  std::uniform_int_distribution<short> dist(std::numeric_limits<Byte>::min(),
                                            std::numeric_limits<Byte>::max());
  for (auto& b : memory) b = static_cast<Byte>(dist(mt));
}

const size_t page_size = 1024 * 4;
gsl::span<Byte> page_align(gsl::span<Byte> in) {
  Expects(in.size_bytes() >= page_size * 2 - 1);
  auto inc_ptr = (uintptr_t)in.data() + page_size - 1;
  auto offset = page_size - 1 - (inc_ptr % page_size);
  auto new_size = in.size_bytes() - offset;
  return in.subspan(offset, new_size - (new_size % page_size));
}

SCENARIO("Writing and reading from files") {
  GIVEN("A FileIO object with an new file") {
    FileIO fileio{ "temp", OpenMode::create_always, true };
    REQUIRE(fileio.size() == 0);

    WHEN("file is resized") {
      const size_t size{ 2 * page_size };
      fileio.resize(size);
      THEN("the size is changed")
      REQUIRE(fileio.size() == size);
    }

    WHEN("data is written") {
      const size_t size{ page_size };
      const size_t offset{ 5 * page_size };
      std::array<Byte, page_size + size> data_buffer;
      auto data = page_align(data_buffer);
      put_random_bytes(data);

      fileio.write(offset, data);
      REQUIRE(fileio.size() == size + offset);
      std::array<Byte, page_size - 1 + size> read_buffer;
      auto read = page_align(read_buffer);

      AND_WHEN("same data is read") {
        REQUIRE(data != read);
        fileio.read(offset, read);
        THEN("read data is equal to written data")
        REQUIRE(data == read);
      }
    }

    WHEN("multiple data chunks are written asynchronous") {
      const size_t size{ page_size };
      const size_t offset{ page_size * 2 };
      const size_t n{ 4 };
      std::array<Byte, size * n + page_size - 1> data_buffer;
      auto data = page_align(data_buffer);
      put_random_bytes(data);
      AsyncReq reqs[n];
      for (size_t i = 0; i < n; ++i) {
        reqs[i] =
            fileio.write_async(offset + size * i, data.subspan(size * i, size));
      }
      for (auto& e : reqs) { e.wait(); }
      REQUIRE(fileio.size() == offset + size * n);

      AND_WHEN("same data chunks are read asynchronous") {
        std::array<Byte, size * n + page_size - 1> read_buffer;
        auto read = page_align(read_buffer);
        REQUIRE(data != read);
        for (size_t i = 0; i < n; ++i) {
          reqs[i] = fileio.read_async(offset + size * i,
                                      read.subspan(size * i, size));
        }
        for (auto& e : reqs) { e.wait(); }
        THEN("read data is equal to written data")
        REQUIRE(data == read);
      }
    }
  }
}
