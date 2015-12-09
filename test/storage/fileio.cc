#ifdef _MSC_VER
#define _SCL_SECURE_NO_WARNINGS
#endif
#include "catch.hpp"
#include "storage/fileio.h"
#include <boost/align/aligned_alloc.hpp>
#include <gsl.h>
#include <random>

using namespace cheesebase;

void put_random_bytes(gsl::span<Byte> memory) {
  std::random_device rd;
  std::mt19937 mt{ rd() };

  std::uniform_int_distribution<int> dist(std::numeric_limits<int>::min(),
                                          std::numeric_limits<int>::max());
  for (auto& i : memory) i = (Byte)((char)dist(mt));
}

const size_t page_size = k_page_size;

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
      auto data_buffer =
          (Byte*)boost::alignment::aligned_alloc(page_size, size);
      auto data = gsl::span<Byte>(data_buffer, size);
      put_random_bytes(data);

      fileio.write(offset, data);
      REQUIRE(fileio.size() == size + offset);
      auto read_buffer =
          (Byte*)boost::alignment::aligned_alloc(page_size, size);
      auto read = gsl::span<Byte>(read_buffer, size);

      AND_WHEN("same data is read") {
        REQUIRE(data != read);
        fileio.read(offset, read);
        THEN("read data is equal to written data")
        REQUIRE(data == read);
      }
      boost::alignment::aligned_free(data_buffer);
      boost::alignment::aligned_free(read_buffer);
    }

    WHEN("multiple data chunks are written asynchronous") {
      const size_t size{ page_size };
      const size_t offset{ page_size * 2 };
      const size_t n{ 4 };
      auto data_buffer =
          (Byte*)boost::alignment::aligned_alloc(page_size, size * n);
      auto data = gsl::span<Byte>(data_buffer, size * n);
      put_random_bytes(data);
      AsyncReq reqs[n];
      for (size_t i = 0; i < n; ++i) {
        reqs[i] =
            fileio.writeAsync(offset + size * i, data.subspan(size * i, size));
      }
      for (auto& e : reqs) { e.wait(); }
      REQUIRE(fileio.size() == offset + size * n);

      AND_WHEN("same data chunks are read asynchronous") {
        auto read_buffer =
            (Byte*)boost::alignment::aligned_alloc(page_size, size * n);
        auto read = gsl::span<Byte>(read_buffer, size * n);
        REQUIRE(data != read);
        for (size_t i = 0; i < n; ++i) {
          reqs[i] =
              fileio.readAsync(offset + size * i, read.subspan(size * i, size));
        }
        for (auto& e : reqs) { e.wait(); }
        THEN("read data is equal to written data")
        REQUIRE(data == read);
        boost::alignment::aligned_free(read_buffer);
      }
      boost::alignment::aligned_free(data_buffer);
    }
  }
}
