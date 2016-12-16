#include "eval.h"
#include "ast.h"
#include "eval/env.h"
#include "eval/expr.h"

namespace cheesebase {
namespace query {

model::Value evalQuery(const query::Expr& expr, DbSession* session) {
  return query::eval::evalExpr(
      expr, query::eval::Env{ {}, nullptr }, session);
}

} // namespace query
} // namespace cheesebase
