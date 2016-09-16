#pragma once
#include "../ast.h"
#include "env.h"

namespace cheesebase {
namespace query {
namespace eval {

using Bindings_base = std::vector<model::Tuple>;
struct Bindings : public Bindings_base {
  using Bindings_base::Bindings_base;
  using Bindings_base::operator=;

  bool has_order_{ false };
};

model::Value evalSfw(const SfwQuery& sfw, const Env& env);

} // namespace eval
} // namespace query
} // namespace cheesebase
