#pragma once
//#include "query_ast.h"
#include "model/model.h"
#include <memory>
#include <boost/optional.hpp>

namespace cheesebase {

//query::Expr parseQuery(const std::string& str);
model::Value parseValue(const std::string& str);
inline model::Value parseJson(const std::string& str) { return parseValue(str); }

} // namespace cheesebase
