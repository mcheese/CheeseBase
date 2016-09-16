#include "sfw.h"
#include "from.h"
#include "expr.h"
#include "operators.h"
#include "../../exceptions.h"

namespace cheesebase {
namespace query {
namespace eval {
namespace {

void applyWhere(const Where& where, Bindings& bindings, const Env& env) {
  auto last =
      std::remove_if(std::begin(bindings), std::end(bindings),
                     [&where, &env](const model::Tuple& ctx) {
                       auto val = evalExpr(where.expr_, { ctx, &env });
                       if (auto b = boost::get<model::Bool>(&val)) {
                         return !(*b);
                       }
                       throw QueryError{ "WHERE: non-boolean expression" };
                     });
  bindings.resize(std::distance(std::begin(bindings), last));
}

void applyGroupBy(const GroupBy& group_by, Bindings& bindings, const Env& env) {
  using GroupKey = std::vector<model::Value>;
  const auto cmp = [](const GroupKey& a, const GroupKey& b) {
    if (a.size() != b.size()) return a.size() < b.size();
    auto diff =
        std::mismatch(std::begin(a), std::end(a), std::begin(b), std::end(b));

    // they can only both be end, which means the ranges are equal
    if (diff.first == std::end(a) || diff.second == std::end(b)) return false;
    return *diff.first < *diff.second;
  };

  std::map<GroupKey, model::Collection, decltype(cmp)> groups{ cmp };

  for (auto& b : bindings) {
    GroupKey key;
    key.reserve(group_by.size());
    for (auto& term : group_by) {
      key.emplace_back(evalExpr(term.expr_, b + env));
    }

    groups[key].emplace_back(std::move(b));
  }

  bindings.clear();
  bindings.has_order_ = false;

  for (auto& g : groups) {
    auto& key = g.first;
    auto& vals = g.second;

    bindings.emplace_back();
    bindings.back().emplace("group", std::move(vals));

    for (size_t i = 0; i < group_by.size(); i++) {
      if (!group_by[i].as_.empty()) {
        bindings.back().emplace(group_by[i].as_, key[i]);
      }
    }
  }
}

void applyOrderBy(const OrderBy& order_by, Bindings& bindings, const Env& env) {
  std::sort(std::begin(bindings), std::end(bindings),
            [&](const auto& a, const auto& b) {
              // optimization possible: cache expr value for each binding
              for (auto& term : order_by) {
                auto val_a = evalExpr(term.expr_, a + env);
                auto val_b = evalExpr(term.expr_, b + env);
                if (opLt(val_a, val_b)) return (term.desc_ ? false : true); // <
                if (opGt(val_a, val_b)) return (term.desc_ ? true : false); // >
              }
              return false; // equal
            });

  bindings.has_order_ = true;
}

void applyLimitOffset(boost::optional<Limit> limit,
                      boost::optional<Offset> offset, Bindings& bindings,
                      const Env& env) {
  size_t offset_nr = 0;
  if (offset) {
    auto val = evalExpr(offset->expr_, env);
    auto nr = boost::get<model::Number>(&val);
    if (!nr || *nr != static_cast<size_t>(*nr))
      throw QueryError{ "OFFSET: require positive integer" };
    offset_nr = static_cast<size_t>(*nr);
  }

  size_t limit_nr = bindings.size();
  if (limit) {
    auto val = evalExpr(limit->expr_, env);
    auto nr = boost::get<model::Number>(&val);
    if (!nr || *nr != static_cast<size_t>(*nr))
      throw QueryError{ "LIMIT: require positive integer" };
    limit_nr = static_cast<size_t>(*nr);
  }

  auto beg = std::begin(bindings) + offset_nr;
  auto end = beg + std::min(bindings.size() - offset_nr, limit_nr);
  if (beg > std::begin(bindings)) {
    std::move(beg, end, std::begin(bindings));
  }

  bindings.resize(std::distance(beg, end));
}

// SELECT ELEMENT <expr>
model::Value applySelect(const SelectElement& sel, const Env& env,
                         Bindings&& input) {
  model::Collection output;
  output.has_order_ = input.has_order_;

  for (auto& tuple : input) {
    output.emplace_back(evalExpr(sel.expr_, { tuple, &env }));
  }

  return std::move(output);
}

// SELECT ATTRIBUTE <name> : <expr>
model::Value applySelect(const SelectAttribute& sel, const Env& env,
                         Bindings&& input) {
  model::Tuple output;

  for (auto& tuple : input) {
    auto attr = evalExpr(sel.attr_, { tuple, &env });
    auto name = boost::get<model::String>(&attr);
    if (name == nullptr) {
      throw QueryError("SELECT ATTRIBUTE: expected string as name");
    }

    auto value = evalExpr(sel.value_, { tuple, &env });

    if (boost::get<model::Missing>(&value) == nullptr) {
      output.emplace(std::move(*name), std::move(value));
    }
  }

  return std::move(output);
}

model::Value applySelect(const Select& sel, const Env& env,
                         Bindings&& bindings) {
  return boost::apply_visitor(
      [&env, &bindings](auto& ctx) {
        return applySelect(ctx, env, std::move(bindings));
      },
      sel);
}

} // anonymous namespace

model::Value evalSfw(const SfwQuery& sfw, const Env& env) {
  auto bindings = evalFrom(sfw.from_, env);

  if (sfw.where_) applyWhere(*sfw.where_, bindings, env);
  if (sfw.group_by_) applyGroupBy(*sfw.group_by_, bindings, env);
  if (sfw.order_by_) applyOrderBy(*sfw.order_by_, bindings, env);
  if (sfw.limit_ || sfw.offset_)
    applyLimitOffset(sfw.limit_, sfw.offset_, bindings, env);

  return applySelect(sfw.select_, env, std::move(bindings));
}

} // namespace eval
} // namespace query
} // namespace cheesebase
