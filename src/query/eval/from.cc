#include "from.h"
#include "../../exceptions.h"
#include "expr.h"
#include <algorithm>

namespace cheesebase {
namespace query {
namespace eval {
namespace {

Bindings evalFrom(const FromEmpty&, const Env& env, DbSession* session) {
  return { model::Tuple() };
}

// <coll> AS <name> AT <idx>
Bindings evalFrom(const FromCollection& from, const Env& env,
                  DbSession* session) {
  Bindings output;

  auto val = evalExpr(from.expr_, env, session);
  auto collection = boost::get<model::Shared<model::Collection>>(&val);
  if (collection == nullptr)
    throw QueryError("FROM collection: non-collection");

  output.has_order_ = (*collection)->has_order_;

  int at = 1;
  for (auto& e : **collection) {
    if (boost::get<model::Missing>(&e) == nullptr) {
      output.emplace_back();
      auto& binding = output.back();

      binding.emplace(from.as_, std::move(e));
      if (!from.at_.empty()) {
        binding.emplace(from.at_, model::Number(at++));
      }
    }
  }

  return output;
}

// <tuple> AS <key>:<val>
Bindings evalFrom(const FromTuple& from, const Env& env, DbSession* session) {
  Bindings output{};
  output.has_order_ = false;

  auto val = evalExpr(from.expr_, env, session);
  auto tuple = boost::get<model::Shared<model::Tuple>>(&val);
  if (tuple == nullptr) throw QueryError("FROM tuple: non-tuple");

  for (auto& e : **tuple) {
    output.emplace_back();
    auto& binding = output.back();

    binding.emplace(from.as_name_, model::String(std::move(e.first)));
    binding.emplace(from.as_value_, std::move(e.second));
  }

  return output;
}

// <left> INNER CORRELATE <right>
Bindings evalFrom(const FromInner& from, const Env& env, DbSession* session) {
  Bindings output;
  output.has_order_ = false;

  auto left = evalFrom(from.left_, env, session);
  for (auto& l : left) {
    auto right = eval::evalFrom(from.right_, l + env, session);
    output.reserve(output.size() + right.size());
    for (auto& r : right) {
      r.insert(std::begin(l), std::end(l));
      output.emplace_back(std::move(r));
    }
  }

  return output;
}

// <left> LEFT OUTER CORRELATE <right>
Bindings evalFrom(const FromLeft& from, const Env& env, DbSession* session) {
  Bindings output;
  output.has_order_ = false;

  auto left = evalFrom(from.left_, env, session);
  for (auto& l : left) {
    auto right = eval::evalFrom(from.right_, l + env, session);
    output.reserve(output.size() + right.size());

    if (!right.empty()) {
      for (auto& r : right) {
        r.insert(std::begin(l), std::end(l));
        output.emplace_back(std::move(r));
      }
    } else {
      output.emplace_back(l);
    }
  }

  return output;
}

// <left> FULL OUTER CORRELATE <right> ON <cond>
Bindings evalFrom(const FromFull& from, const Env& env, DbSession* session) {
  Bindings output;
  output.has_order_ = false;

  auto left = evalFrom(from.left_, env, session);
  auto right = evalFrom(from.right_, env, session);
  std::vector<bool> right_used(right.size(), false);

  for (auto& l : left) {
    bool left_used = false;

    for (size_t r_idx = 0; r_idx < right.size(); r_idx++) {
      auto& r = right[r_idx];

      Env r_env{ r, &env };
      auto cond_val = evalExpr(from.cond_, l + r_env, session);
      auto cond_bool = boost::get<model::Bool>(&cond_val);
      if (cond_bool && *cond_bool) {
        output.emplace_back();
        output.back().insert(std::begin(r), std::end(r));
        output.back().insert(std::begin(l), std::end(l));

        right_used[r_idx] = true;
        left_used = true;
      }
    }

    if (!left_used) {
      output.emplace_back();
      output.back().insert(std::begin(l), std::end(l));
    }
  }

  for (size_t r_idx = 0; r_idx < right.size(); r_idx++) {
    if (!right_used[r_idx]) {
      auto& r = right[r_idx];
      output.emplace_back();
      output.back().insert(std::begin(r), std::end(r));
    }
  }

  return output;
}

template <typename T>
Bindings evalFrom(T, const Env& env, DbSession* session) {
  return {};
}

} // anonymous namespace

Bindings evalFrom(const From& from, const Env& env, DbSession* session) {
  return boost::apply_visitor(
      [&env, session](auto& ctx) { return evalFrom(ctx, env, session); }, from);
}

} // eval
} // query
} // cheesebase
