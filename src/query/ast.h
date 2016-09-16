#pragma once

#include "../model/model.h"
#include <memory>
#include <map>
#include <boost/spirit/home/x3/support/ast/variant.hpp>
#include <boost/optional.hpp>

namespace x3 = boost::spirit::x3;

namespace cheesebase {
namespace query {

struct Var : std::string {
  using std::string::string;
  using std::string::operator=;
};

struct SfwQuery;
struct Expr;

using String = std::string;

using Tuple_base = std::map<String, Expr>;
using Tuple_member = std::pair<String, Expr>;
struct Tuple : Tuple_base {
  using Tuple_base::Tuple_base;
  using Tuple_base::operator=;
};

using Bag_base = std::vector<Expr>;
struct Bag : Bag_base {
  using Bag_base::Bag_base;
  using Bag_base::operator=;
};

using Array_base = std::vector<Expr>;
struct Array : Bag_base {
  using Bag_base::Bag_base;
  using Bag_base::operator=;
};

struct Function {
  std::string name_;
  std::vector<Expr> arguments_;
};

struct TupleNav;
struct ArrayNav;
struct InfixOp;
struct PrefixOp;

struct Expr : x3::variant<Var, x3::forward_ast<SfwQuery>, Tuple, Bag, Array,
                          x3::forward_ast<TupleNav>, x3::forward_ast<ArrayNav>,
                          x3::forward_ast<InfixOp>, x3::forward_ast<PrefixOp>,
                          Function, model::Value> {
  Expr() = default;
  using base_type::base_type;
  using base_type::operator=;
};

struct TupleNav {
  Expr base_;
  String key_;
};

struct ArrayNav {
  Expr base_;
  Expr idx_;
};

enum class PrefixOperator { neg };

struct PrefixOp {
  PrefixOperator op_;
  Expr val_;
};

enum class Operator {
  plus,
  minus,
  mul,
  div,
  modulo,
  lt,
  le,
  gt,
  ge,
  eq,
  neq
};

struct InfixOp {
  Expr left_;
  Operator op_;
  Expr right_;
};

////////////////////////////////////////////////////////////////////////////////
// Select

struct SelectElement {
  SelectElement() = default;
  SelectElement(Expr expr) : expr_{ std::move(expr) } {}

  Expr expr_;
};

struct SelectAttribute {
  Expr attr_;
  Expr value_;
};

struct Select : x3::variant<SelectElement, SelectAttribute> {
  using base_type::base_type;
  using base_type::operator=;
};

////////////////////////////////////////////////////////////////////////////////
// From

struct From;

struct FromEmpty {};

struct FromCollection {
  Expr expr_;
  Var as_;
  Var at_;
};

struct FromTuple {
  Expr expr_;
  Var as_name_;
  Var as_value_;
};

struct FromInner {
  x3::forward_ast<From> left_;
  x3::forward_ast<From> right_;
};

struct FromLeft {
  x3::forward_ast<From> left_;
  x3::forward_ast<From> right_;
};

struct FromFull {
  x3::forward_ast<From> left_;
  x3::forward_ast<From> right_;
  Expr cond_;
};

struct From : x3::variant<FromEmpty, FromCollection, FromTuple, FromInner,
                          FromLeft, FromFull> {
  using base_type::base_type;
  using base_type::operator=;
};

////////////////////////////////////////////////////////////////////////////////

struct Where {
  Where() = default;
  Where(Expr expr) : expr_{ std::move(expr) } {}

  Expr expr_;
};

struct GroupBy_term {
  Expr expr_;
  Var as_;
};

struct GroupBy : public std::vector<GroupBy_term> {
  using std::vector<GroupBy_term>::vector;
  using std::vector<GroupBy_term>::operator=;
};

struct Having {};
struct Set {};

struct Limit {
  Limit() = default;
  Limit(Expr expr) : expr_{ std::move(expr) } {}

  Expr expr_;
};

struct Offset {
  Offset() = default;
  Offset(Expr expr) : expr_{ std::move(expr) } {}

  Expr expr_;
};

struct OrderBy_term {
  Expr expr_;
  bool desc_{ false };
};

struct OrderBy : public std::vector<OrderBy_term> {
  using std::vector<OrderBy_term>::vector;
  using std::vector<OrderBy_term>::operator=;
};

struct SfwQuery {
  Select select_;
  From from_;
  boost::optional<Where> where_;
  boost::optional<GroupBy> group_by_;
  boost::optional<Having> having_;
  boost::optional<Set> set_;
  boost::optional<OrderBy> order_by_;
  boost::optional<Limit> limit_;
  boost::optional<Offset> offset_;
};


} // namespace query
} // napespace cheesebase
