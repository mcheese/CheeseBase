#include "../../exceptions.h"
#include "../ast.h"
#include "expr.h"
#include <algorithm>
#include <numeric>
#include <cmath>
#include <unordered_map>

namespace cheesebase {
namespace query {
namespace eval {

inline model::Value funcSum(const model::Collection& input) {
  const auto val_sum = [](double base, const model::Value& val) {
    auto num = boost::get<model::Number>(&val);

    if (!num) {
      if (val.get().type() == typeid(model::Missing) ||
          val.get().type() == typeid(model::Null)) {
        return base;
      }
      throw QueryError("sum(): unsupported type");
    }

    return base + *num;
  };

  return std::accumulate(std::begin(input), std::end(input), 0.0, val_sum);
}

inline model::Value funcMax(const model::Collection& input) {
  auto el = std::max_element(std::begin(input), std::end(input));
  return el != std::end(input) ? *el : model::Missing();
}

inline model::Value funcFloor(const std::vector<model::Value>& args) {
  if (args.size() != 1) throw QueryError("floor(): expects 1 argument");
  auto num = boost::get<model::Number>(&args[0]);
  if (!num) throw QueryError("floor(): expects Number");
  return std::floor(*num);
}

inline model::Value evalFunction(const Function& func, const Env& env,
                                 DbSession* session) {
  typedef model::Value (*FuncPtr)(const std::vector<model::Value>&);
  typedef model::Value (*FuncAggrPtr)(const model::Collection&);
  static const std::unordered_map<std::string, FuncPtr> funcs{ { "floor",
                                                                 &funcFloor } };
  static const std::unordered_map<std::string, FuncAggrPtr> aggr_funcs{
    { "sum", &funcSum }, { "max", &funcMax }
  };

  std::string lower_name;
  std::transform(std::begin(func.name_), std::end(func.name_),
                 std::back_inserter(lower_name),
                 [](char c) { return std::tolower(c); });

  auto func_lookup = funcs.find(lower_name);
  if (func_lookup != std::end(funcs)) {
    std::vector<model::Value> args;
    args.reserve(func.arguments_.size());
    for (auto& e : func.arguments_) {
      args.emplace_back(evalExpr(e, env, session));
    }
    return func_lookup->second(args);
  }

  auto aggr_lookup = aggr_funcs.find(lower_name);
  if (aggr_lookup != std::end(aggr_funcs)) {
    if (func.arguments_.size() != 1) {
      throw QueryError("Aggregate function:" + func.name_ +
                       " expects 1 argument");
    }

    // If the most recently added environment has a "group" member there was a
    // GROUP BY operation. In this case a collection is created from evaluating
    // the expression in each element of that group to support SQL style
    // aggregate functions.
    auto group_lookup = env.self.find("group");
    if (group_lookup != std::end(env.self)) {
      auto group = boost::get<model::SCollection>(&group_lookup->second);
      if (group) {
        model::Collection coll;
        coll.reserve((*group)->size());
        for (auto& b : **group) {
          auto tuple = boost::get<model::STuple>(&b);
          if (!tuple) {
            throw QueryError(
                "Invalid element in group used by aggregate function");
          }

          coll.emplace_back(
              evalExpr(func.arguments_[0], **tuple + env, session));
        }

        return aggr_lookup->second(coll);
      }
    }

    // Since it is not a use after GROUP BY the argument has to be a collection.
    auto val = evalExpr(func.arguments_[0], env, session);
    auto coll = boost::get<model::SCollection>(&val);
    if (!coll) {
      throw QueryError(
          "Aggregate function expects collection or use with GROUP BY");
    }
    return aggr_lookup->second(**coll);
  }

  throw QueryError("Unknown function: " + func.name_);
}

} // namespace eval
} // namespace query
} // namespace cheesebase
