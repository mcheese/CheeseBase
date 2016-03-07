#ifdef _MSC_VER
#define _SCL_SECURE_NO_WARNINGS
#endif
#include "catch.hpp"
#include "cheesebase.h"
#include <iostream>
#include <boost/filesystem.hpp>

using namespace cheesebase;

TEST_CASE("JSON") {
  std::string input = R"(
{
    "glossary": {
        "title": "example glossary",
        "GlossDiv": {
            "title": "S",
            "GlossList": {
                "GlossEntry": {
                    "ID": "SGML",
                    "SortAs": "SGML",
                    "GlossTerm": "Standard Genera",
                    "Acronym": "SGML",
                    "Abbrev": "ISO 8879:1986",
                    "GlossDef": {
                        "para": "A meta-markup"
                    },
                    "GlossSee": "markup"
                }
            }
        }
    },
     "test": 123,
    "1": {
      "first_name": "Beverly",
      "last_name": "Riley",
      "email": "briley0@phoca.cz",
      "country": "France",
      "ip_address": "254.153.188.156"
    }, "2": {
      "first_name": "Irene",
      "last_name": "Rodriguez",
      "email": "irodrez1@trivisor.com",
      "country": "Pakistan",
      "ip_address": "176.226.201.230"
    }, "3": {
      "id": 3,
      "first_name": "Rebecca",
      "last_name": "Porter",
      "email": "rporter2@tinypic.com",
      "country": "Russia",
      "ip_address": "118.47.83.13"
    }, "4": 12312, "5": null, "6": true, "7": false, "8": null
  }
)";
  boost::filesystem::remove("test.db");
  {
    CheeseBase cb{ "test.db" };
    cb.insert("test", input);
    cb.update("test", "false");
    REQUIRE(cb.get("test") == "false");
  }
}
