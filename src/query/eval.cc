#include "eval.h"
#include "ast.h"
#include "eval/env.h"
#include "eval/expr.h"

namespace cheesebase {

model::Value evalQuery(const query::Expr& expr) {
  return query::eval::evalExpr(expr,
                               query::eval::Env{ model::Tuple(), nullptr });
}

} // cheesebase
