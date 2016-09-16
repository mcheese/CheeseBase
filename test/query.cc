#include "catch.hpp"
#include "parser.h"
#include "query/eval.h"

using namespace cheesebase;

inline void check(const std::string& q, const std::string v) {
  INFO( q << "\nexpected:\n" << v);
  REQUIRE(evalQuery(parseQuery(q)) == evalQuery(parseQuery(v)));
};

TEST_CASE("Arithmetic") {
  check(R"( 2 + 3 )", R"( 5 )");
  check(R"( 2 - 3 )", R"( -1 )");
  check(R"( 2 * 3 )", R"( 6 )");
  check(R"( 4 / 2 )", R"( 2 )");
  check(R"( 2 + 3 * 4 )", R"( 14 )");
  check(R"( 2 * 3 + 4 )", R"( 10 )");
  check(R"( 3 % 2 )", R"( 1 )");
  check(R"( (1 + 2) * 3 )", R"( 9 )");
  check(R"( 1 * (2 / 2) )", R"( 1 )");
  check(R"( (2*(1 + 1 - 1 * 3))/2 )", R"( -1 )");
  check(R"( -1 )", R"( -1 )");
  check(R"( --1 )", R"( 1 )");
  check(R"( -(1 + 2) )", R"( -3 )");
  check(R"( (1 + -2) )", R"( -1 )");
}

TEST_CASE("Just SELECT") {
  SECTION("SELECT ...") {
    check(R"( SELECT "Hello World" AS x )",
          R"( [{ "x": "Hello World" }] )");

    check(R"( SELECT "Hello World" AS x, 123 AS y )",
          R"( [{ "x": "Hello World", "y": 123 }] )");
  }

  SECTION("SELECT ELEMENT ...") {
    check(R"( SELECT ELEMENT 123 )",
          R"( [123] )");
  }

  SECTION("SELECT ATTRIBUTE ...") {
    check(R"( SELECT ATTRIBUTE "abc" : "hi" )",
          R"( { "abc" : "hi" } )");
  }
}

TEST_CASE("FROM collection") {
  SECTION("SELECT ...") {
    check(R"( SELECT hello AS x FROM [ "Hello World" ] AS hello )",
          R"( [{ "x": "Hello World" }] )");

    check(R"( SELECT x, nr FROM [ "first", "second" ] AS x AT nr )",
          R"( [{ "x": "first", "nr": 1 },
               { "x": "second", "nr": 2 }] )");
  }

  SECTION("SELECT ELEMENT ...") {
    check(R"( SELECT ELEMENT { "key" : x, "pos" : y }
              FROM [ 123, 456 ] AS x AT y )",
          R"( [ { "key": 123, "pos": 1 },
                { "key": 456, "pos": 2 } ] )");
  }

  SECTION("SELECT ATTRIBUTE ...") {
    check(R"( SELECT ATTRIBUTE x : y FROM [ "a", "b", "c" ] AS x AT y  )",
          R"( { "a": 1, "b": 2, "c": 3 } )");
  }
}

TEST_CASE("FROM tuple") {
    check(R"( SELECT ELEMENT [x,y] FROM { "a":1, "b":2 } AS { x:y } )",
          R"( [ ["a",1], ["b",2] ] )");

}

TEST_CASE("Path navigation") {
  SECTION("Array") {
    check(R"( [0,1,2,3,4][2] )",
          R"( 2 )");
    check(R"( [0,[1,[2]]][1][1][0] )",
          R"( 2 )");
  }

  SECTION("Tuple") {
    check(R"( { "a": 1, "b":2 }.b )",
          R"( 2 )");
    check(R"( { "a": { "v": 1 }, "b": { "v": 2 } }.b.v )",
          R"( 2 )");
  }

  SECTION("SELECT .. FROM ..") {
    check(R"( SELECT ELEMENT x.a[1] FROM [ { "a": [1,2] } ] AS x )",
          R"( [ 2 ] )");
  }
}

TEST_CASE("Subqueries") {
  check(R"( SELECT ELEMENT a[2]
            FROM [(SELECT ELEMENT nr FROM [0,1,2,3,4] AS nr)] AS a )",
        R"( [ 2 ] )");
}

TEST_CASE("WHERE") {
  check(R"( SELECT ELEMENT x FROM [3,1,5,2,4] AS x WHERE true )",
        R"( [3,1,5,2,4] )");

  check(R"( SELECT ELEMENT x FROM [3,1,5,2,4] AS x WHERE false )",
        R"( [] )");

  check(R"( SELECT ELEMENT x FROM [3,1,5,2,4] AS x WHERE x > 2 )",
        R"( [3,5,4] )");

  check(R"( SELECT ELEMENT x FROM [3,1,5,2,4] AS x WHERE x % 2 = 1 )",
        R"( [3,1,5] )");
}

TEST_CASE("ORDER BY") {
  check(R"( SELECT ELEMENT x FROM [3,1,2] AS x ORDER BY x )",
        R"( [1,2,3] )");

  check(R"( SELECT ELEMENT x FROM [3,1,2] AS x ORDER BY x ASC )",
        R"( [1,2,3] )");

  check(R"( SELECT ELEMENT x FROM [3,1,2] AS x ORDER BY x DESC )",
        R"( [3,2,1] )");

  check(R"( SELECT ELEMENT x
            FROM [[2,3],[5,6],[7,3],[4,3],[5,5],[5,4],[4,4]] AS x
            ORDER BY x[0] DESC, x[1] )",
      R"( [
            [ 7, 3 ],
            [ 5, 4 ],
            [ 5, 5 ],
            [ 5, 6 ],
            [ 4, 3 ],
            [ 4, 4 ],
            [ 2, 3 ]
          ] )");
}

TEST_CASE("LIMIT") {
  check(R"( SELECT ELEMENT x FROM [1,2,3,4,5,6] AS x LIMIT 3 )",
        R"( [1,2,3] )");

  check(R"( SELECT ELEMENT x FROM [6,5,3,1,2,4] AS x ORDER BY x LIMIT 3 )",
        R"( [1,2,3] )");

  check(R"( SELECT ELEMENT x FROM [1,2,3] AS x LIMIT 100 )",
        R"( [1,2,3] )");
}

TEST_CASE("OFFSET") {
  check(R"( SELECT ELEMENT x FROM [1,2,3,4,5,6] AS x OFFSET 3 )",
        R"( [4,5,6] )");

  check(R"( SELECT ELEMENT x FROM [6,5,3,1,2,4] AS x
            ORDER BY x LIMIT 2 OFFSET 2 )",
        R"( [3,4] )");
}

TEST_CASE("JOIN") {
  check(R"( SELECT ELEMENT [l,r] FROM [1,2,3] AS l INNER [4,5,6] AS r )",
        R"( [[1,4], [1,5], [1,6],
             [2,4], [2,5], [2,6],
             [3,4], [3,5], [3,6]] )");
  check(R"( SELECT ELEMENT [l,r] FROM [[],[1],[2,3]] AS l INNER l AS r )",
        R"( [[[1],1], [[2,3],2], [[2,3],3]] )");

  check(R"( SELECT ELEMENT [l,r] FROM [[],[1],[2,3]] AS l LEFT l AS r )",
        R"( [ [[],missing], [[1],1], [[2,3],2], [[2,3],3]] )");

  check(R"( SELECT ELEMENT [l,r] FROM [1,2] AS l FULL OUTER [1,2] AS r ON l > r )",
        R"( [ [1,missing], [2,1], [missing,2] ] )");

  check(R"( SELECT ELEMENT [l,r] FROM [1,2,3] AS l INNER JOIN [1,2] AS r ON l > r )",
        R"( [ [2,1], [3,1], [3,2] ] )");

  check(R"( SELECT ELEMENT [l,r] FROM [1,2,3] AS l LEFT JOIN [1,2] AS r ON l > r )",
        R"( [ [1,missing], [2,1], [3,1], [3,2] ] )");

  check(R"( SELECT ELEMENT [r,l] FROM [1,2] AS l RIGHT JOIN [1,2,3] AS r ON l < r )",
        R"( [ [1,missing], [2,1], [3,1], [3,2] ] )");

  check(R"( SELECT ELEMENT [l,r] FROM [1,2] AS l FULL JOIN [1,2] AS r ON l > r )",
        R"( [ [1,missing], [2,1], [missing,2] ] )");
}

TEST_CASE("GROUP BY") {
  check(R"( SELECT ELEMENT [x,group] FROM [1,2,3,4,5] AS y GROUP BY y%2 AS x )",
        R"( [ [0, [ { y:2 }, { y:4 } ]],
              [1, [ { y:1 }, { y:3 }, { y:5 } ]] ] )");

  check(R"( SUM([1,2,3,4]) )", R"( 10 )");

  check(R"( SUM([]) )", "0");

  check(R"( SELECT ELEMENT { "odd": odd, "sum": SUM(x) }
             FROM [1,2,3,4,5,6,7] AS x GROUP BY x % 2 AS odd )",
        R"( [{ "odd" : 0, "sum" : 12 }, { "odd" : 1, "sum" : 16 }] )");

  check(R"( SELECT ELEMENT { "odd": odd, "sum": SUM(x+1) }
            FROM [1,2,3,4,5,6,7] AS x GROUP BY x % 2 AS odd )",
        R"( [{ "odd" : 0, "sum" : 15 }, { "odd" : 1, "sum" : 20 }] )");
}
