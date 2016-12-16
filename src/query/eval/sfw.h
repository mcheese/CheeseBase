#pragma once
#include "../ast.h"
#include "../db_session.h"
#include "env.h"
#include <boost/container/flat_set.hpp>

namespace cheesebase {
namespace query {
namespace eval {

using Bindings_base = std::vector<model::Tuple_base>;
struct Bindings : public Bindings_base {
  using Bindings_base::Bindings_base;
  using Bindings_base::operator=;

  bool has_order_{ false };
  boost::container::flat_set<Var> names_;
};

model::Value evalSfw(const SfwQuery& sfw, const Env& env, DbSession* session);

} // namespace eval
} // namespace query
} // namespace cheesebase
