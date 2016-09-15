#include "parser.h"
//#include "query_parser.h"
#include "model/parser.h"
#include "exceptions.h"

namespace cheesebase {

/*
query::Expr parseQuery(const std::string& q) {
  auto iter = std::begin(q);
  auto end = std::end(q);

  query::Expr query;
  auto success =
      x3::phrase_parse(iter, end, parser::query, x3::ascii::space, query);

  if (!success) throw ParserError { "Parse unsuccessful" };
  if (iter != end) throw ParserError { "Did not consume all input" };
  return query;
}
*/

model::Value parseValue(const std::string& q) {
  auto iter = std::begin(q);
  auto end = std::end(q);

  model::Value val;
  auto success =
      x3::phrase_parse(iter, end, parser::value, x3::ascii::space, val);

  if (!success) throw ParserError { "Parse unsuccessful" };
  if (iter != end) throw ParserError { "Did not consume all input" };

  return val;
}

} // namespace cheesebase
