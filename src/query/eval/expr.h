#pragma once
#include "env.h"
#include "../ast.h"
#include "../db_session.h"

namespace cheesebase {
namespace query {
namespace eval {

model::Value evalExpr(const Expr& expr, const Env& env, DbSession* session);

} // namespace eval
} // namespace query
} // namespace cheesebase
