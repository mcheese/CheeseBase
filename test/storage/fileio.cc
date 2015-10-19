#include "catch.hpp"

#include "storage/fileio.h"

#include <random>

using namespace std;
using namespace cheesebase;

vector<byte> get_random_vector(const size_t size)
{
  vector<byte> v;
  v.reserve(size);
  random_device rd;
  mt19937 mt{ rd() };

  uniform_int_distribution<short> dist(numeric_limits<byte>::min(), numeric_limits<byte>::max());
  for (int i = 0; i < size; ++i)
    v.push_back(static_cast<byte>(dist(mt)));
  return v;
}

SCENARIO("Writing and reading from files")
{
  GIVEN("A FileIO object with an new file")
  {
    FileIO fileio{ "temp", OpenMode::create_always };
    REQUIRE(fileio.size() == 0);
    
    WHEN("file is resized")
    {
      const size_t size{ 1234 };
      fileio.resize(size);
      THEN("the size is changed")
        REQUIRE(fileio.size() == size);
    }

    WHEN("data is written")
    {
      const size_t size{ 3000 };
      const size_t offset{ 5000 };
      auto data = get_random_vector(size);
      fileio.write(offset, data);
      REQUIRE(fileio.size() == size + offset);
      vector<byte> read;
      read.resize(size);

      AND_WHEN("same data is read")
      {
        REQUIRE(!equal(data.begin(), data.end(), read.begin()));
        fileio.read(offset, read);
        THEN("read data is equal to written data")
          REQUIRE(equal(data.begin(), data.end(), read.begin()));
      }
    }

    WHEN("multiple data chunks are written asynchronous")
    {
      const size_t size{ 1024*32 };
      const size_t offset{ 2048 };
      const size_t n{ 4 };
      auto data = get_random_vector(size * n);
      AsyncReq reqs[n];
      for (size_t i = 0; i < n; ++i) {
        reqs[i] = fileio.write_async(offset + size*i, { data.data() + size*i, size });
      }
      for (auto& e : reqs) {
        e.wait();
      }
      REQUIRE(fileio.size() == offset + size*n);

      AND_WHEN("same data chunks are read asynchronous")
      {
        vector<byte> read;
        read.resize(size*n);
        for (size_t i = 0; i < n; ++i) {
          reqs[i] = fileio.read_async(offset + size*i, { read.data() + size*i, size });
        }
        for (auto& e : reqs) {
          e.wait();
        }
        THEN("read data is equal to written data")
          REQUIRE(equal(data.begin(), data.end(), read.begin()));
      }
    }
  }
}
