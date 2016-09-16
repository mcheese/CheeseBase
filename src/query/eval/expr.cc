#include "expr.h"
#include "operators.h"
#include "functions.h"
#include "from.h"
#include "sfw.h"
#include "conf.h"

namespace cheesebase {
namespace query {
namespace eval {
namespace {

// literal value
model::Value evalExpr(const model::Value& val, const Env& env) { return val; }

// select-from-where
model::Value evalExpr(const SfwQuery& sfw, const Env& env) {
  return evalSfw(sfw, env);
}

// variable name
model::Value evalExpr(const Var& var, const Env& env) {
  for (auto e = &env; e != nullptr; e = e->next) {
    auto lookup = e->self.find(var);
    if (lookup != std::end(e->self)) return lookup->second;
  }
  return model::Missing();
}

// { <name>:<expr>, ... }
model::Value evalExpr(const Tuple& tuple, const Env& env) {
  model::Tuple output;
  for (auto& e : tuple) {
    output.emplace(e.first, evalExpr(e.second, env));
  }
  return std::move(output);
}

// [ <expr>, ... ]
model::Value evalExpr(const Array& array, const Env& env) {
  model::Collection output;
  output.has_order_ = true;
  for (auto& e : array) {
    output.emplace_back(evalExpr(e, env));
  }
  return std::move(output);
}

// {{ <expr>, <expr>, ... }}
model::Value evalExpr(const Bag& bag, const Env& env) {
  model::Collection output;
  output.has_order_ = false;
  for (auto& e : bag) {
    output.emplace_back(evalExpr(e, env));
  }
  return std::move(output);
}

// <expr>.<name>
model::Value evalExpr(const TupleNav& nav, const Env& env) {
  auto base = evalExpr(nav.base_, env);

  // base has to be a tuple

  auto tuple = boost::get<model::Shared<model::Tuple>>(&base);
  if (tuple == nullptr) {
    switch (kConf.tuple_nav.type_mismatch) {
    case Config::NavFailure::missing:
      return model::Missing{};
    case Config::NavFailure::null:
      return model::Null{};
    default:
      throw QueryError("Tuple navigation '." + nav.key_ +
                       "' failed: non-tuple on left side");
    }
  }

  // find the member in the tuple

  auto lookup = (*tuple)->find(nav.key_);
  if (lookup == std::end(**tuple)) {
    switch (kConf.tuple_nav.absent) {
    case Config::NavFailure::missing:
      return model::Missing{};
    case Config::NavFailure::null:
      return model::Null{};
    default:
      throw QueryError("Tuple navigation '." + nav.key_ +
                       "' failed: name not found");
    }
  }

  return lookup->second;
}

// <expr>[<expr>]
model::Value evalExpr(const ArrayNav& nav, const Env& env) {
  auto base = evalExpr(nav.base_, env);
  auto key = evalExpr(nav.idx_, env);

  // key needs to be a whole number

  auto number = boost::get<model::Number>(&key);
  if (number == nullptr || *number != static_cast<size_t>(*number)) {
    switch (kConf.array_nav.type_mismatch) {
    case Config::NavFailure::missing:
      return model::Missing{};
    case Config::NavFailure::null:
      return model::Null{};
    default:
      throw QueryError("Array navigation failed: non-integer as subscript");
    }
  }

  auto index = static_cast<size_t>(*number);

  // base needs to be an array

  auto coll = boost::get<model::Shared<model::Collection>>(&base);
  if (coll == nullptr ||
      (!kConf.array_nav.allow_bag && (*coll)->has_order_ == false)) {
    switch (kConf.array_nav.type_mismatch) {
    case Config::NavFailure::missing:
      return model::Missing{};
    case Config::NavFailure::null:
      return model::Null{};
    default:
      throw QueryError("Array navigation '[" + std::to_string(index) +
                       "]' failed: non-array on left side");
    }
  }

  // find index in array

  if (index >= (*coll)->size()) {
    switch (kConf.array_nav.absent) {
    case Config::NavFailure::missing:
      return model::Missing{};
    case Config::NavFailure::null:
      return model::Null{};
    default:
      throw QueryError("Array navigation '[" + std::to_string(index) +
                       "]' failed: index out of bounds");
    }
  }

  return (**coll)[index];
}

// <expr> <op> <expr>
model::Value evalExpr(const InfixOp& infix, const Env& env) {
  return evalOperator(infix.op_, evalExpr(infix.left_, env),
                      evalExpr(infix.right_, env));
}

// <op> <expr>
model::Value evalExpr(const PrefixOp& prefix, const Env& env) {
  return evalOperator(prefix.op_, evalExpr(prefix.val_, env));
}

// <func>(<arg0> .. <argn>)
model::Value evalExpr(const Function& func, const Env& env) {
  return evalFunction(func, env);
}

} // anonymous namespace

model::Value evalExpr(const Expr& expr, const Env& env) {
  return boost::apply_visitor([&env](auto& ctx) { return evalExpr(ctx, env); },
                              expr);
}

} // namespace eval
} // namespace query
} // namespace cheesebase
