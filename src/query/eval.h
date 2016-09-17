#pragma once
#include "db_session.h"
#include "../model/model.h"

namespace cheesebase {
namespace query {
struct Expr;

model::Value evalQuery(const query::Expr& query, DbSession* db = nullptr);

} // namespace query
} // namespace cheesebase
