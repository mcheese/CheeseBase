#pragma once
#include "../ast.h"
#include "env.h"

namespace cheesebase {
namespace query {
namespace eval {

model::Value evalExpr(const Expr& expr, const Env& env);

} // namespace eval
} // namespace query
} // namespace cheesebase
