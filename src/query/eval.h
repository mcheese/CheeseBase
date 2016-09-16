#pragma once
#include "../model/model.h"

namespace cheesebase {
namespace query {
struct Expr;
} // namespace query

model::Value evalQuery(const query::Expr& query);

} // namespace cheesebase
