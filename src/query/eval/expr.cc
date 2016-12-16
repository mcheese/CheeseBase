#include "expr.h"
#include "conf.h"
#include "from.h"
#include "functions.h"
#include "operators.h"
#include "sfw.h"

namespace cheesebase {
namespace query {
namespace eval {
namespace {

// literal value
model::Value evalExpr(const model::Value& val, const Env&, DbSession*) {
  return val;
}

// select-from-where
model::Value evalExpr(const SfwQuery& sfw, const Env& env, DbSession* session) {
  return evalSfw(sfw, env, session);
}

// variable name
model::Value evalExpr(const Var& var, const Env& env, DbSession* session) {
  for (auto e = &env; e != nullptr; e = e->next) {
    auto lookup = e->self.find(var);
    if (lookup != std::end(e->self)) return lookup->second;
  }

  if (!session) return model::Missing{};

  return session->getRoot().at(var);
}

// { <name>:<expr>, ... }
model::Value evalExpr(const Tuple& tuple, const Env& env, DbSession* session) {
  model::Tuple output;
  for (auto& e : tuple) {
    output.emplace(e.first, evalExpr(e.second, env, session));
  }
  return std::move(output);
}

// [ <expr>, ... ]
model::Value evalExpr(const Array& array, const Env& env, DbSession* session) {
  model::Collection output;
  output.has_order_ = true;
  for (auto& e : array) {
    output.emplace_back(evalExpr(e, env, session));
  }
  return std::move(output);
}

// {{ <expr>, <expr>, ... }}
model::Value evalExpr(const Bag& bag, const Env& env, DbSession* session) {
  model::Collection output;
  output.has_order_ = false;
  for (auto& e : bag) {
    output.emplace_back(evalExpr(e, env, session));
  }
  return std::move(output);
}

// <expr>.<name>
model::Value evalExpr(const TupleNav& nav, const Env& env, DbSession* session) {
  auto base = evalExpr(nav.base_, env, session);

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

  const auto nav_failure = [&nav]() -> model::Value {
    switch (kConf.tuple_nav.absent) {
    case Config::NavFailure::missing:
      return model::Missing{};
    case Config::NavFailure::null:
      return model::Null{};
    default:
      throw QueryError("Tuple navigation '." + nav.key_ +
                       "' failed: name not found");
    }
  };

  try {
    return (*tuple)->at(nav.key_);
  } catch (std::out_of_range&) {
    return nav_failure();
  } catch (UnknownKeyError&) {
    return nav_failure();
  }
}

// <expr>[<expr>]
model::Value evalExpr(const ArrayNav& nav, const Env& env, DbSession* session) {
  auto base = evalExpr(nav.base_, env, session);
  auto key = evalExpr(nav.idx_, env, session);

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

  const auto nav_failure = [&index]() -> model::Value {
    switch (kConf.array_nav.absent) {
    case Config::NavFailure::missing:
      return model::Missing{};
    case Config::NavFailure::null:
      return model::Null{};
    default:
      throw QueryError("Array navigation '[" + std::to_string(index) +
                       "]' failed: index out of bounds");
    }
  };

  try {
    return (**coll).at(index);
  } catch (std::out_of_range&) {
    return nav_failure();
  } catch (IndexOutOfRangeError&) {
    return nav_failure();
  }
}

// <expr> <op> <expr>
model::Value evalExpr(const InfixOp& infix, const Env& env,
                      DbSession* session) {
  return evalOperator(infix.op_, evalExpr(infix.left_, env, session),
                      evalExpr(infix.right_, env, session));
}

// <op> <expr>
model::Value evalExpr(const PrefixOp& prefix, const Env& env,
                      DbSession* session) {
  return evalOperator(prefix.op_, evalExpr(prefix.val_, env, session));
}

// <func>(<arg0> .. <argn>)
model::Value evalExpr(const Function& func, const Env& env,
                      DbSession* session) {
  return evalFunction(func, env, session);
}

} // anonymous namespace

model::Value evalExpr(const Expr& expr, const Env& env, DbSession* session) {
  return boost::apply_visitor(
      [&env, session](auto& ctx) { return evalExpr(ctx, env, session); }, expr);
}

} // namespace eval
} // namespace query
} // namespace cheesebase
