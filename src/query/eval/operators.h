#include "expr.h"
#include "../ast.h"
#include "../../exceptions.h"
#include <cmath>

namespace cheesebase {
namespace query {
namespace eval {

////////////////////////////////////////////////////////////////////////////////
// Operators

inline model::Value opPlus(const model::Value& l, const model::Value& r) {
  auto a = boost::get<model::Number>(&l);
  auto b = boost::get<model::Number>(&r);

  if (a && b) {
    return *a + *b;
  }

  throw QueryError{ "Operator +: invalid operands" };
}

inline model::Value opMinus(const model::Value& l, const model::Value& r) {
  auto a = boost::get<model::Number>(&l);
  auto b = boost::get<model::Number>(&r);

  if (a && b) {
    return *a - *b;
  }

  throw QueryError{ "operator -: invalid operands" };
}

inline model::Value opMul(const model::Value& l, const model::Value& r) {
  auto a = boost::get<model::Number>(&l);
  auto b = boost::get<model::Number>(&r);

  if (a && b) {
    return *a * *b;
  }

  throw QueryError{ "operator *: invalid operands" };
}

inline model::Value opDiv(const model::Value& l, const model::Value& r) {
  auto a = boost::get<model::Number>(&l);
  auto b = boost::get<model::Number>(&r);

  if (a && b) {
    if (*b == 0) throw QueryError{ "operator /: division by zero" };
    return *a / *b;
  }

  throw QueryError{ "operator /: invalid operands" };
}

inline model::Value opModulo(const model::Value& l, const model::Value& r) {
  auto a = boost::get<model::Number>(&l);
  auto b = boost::get<model::Number>(&r);

  if (a && b) {
    if (*b == 0) throw QueryError{ "operator %: division by zero" };
    return std::fmod(*a,*b);
  }

  throw QueryError{ "operator %: invalid operands" };
}

inline model::Bool opLt(const model::Value& l, const model::Value& r) {
  auto a = boost::get<model::Number>(&l);
  auto b = boost::get<model::Number>(&r);

  if (a && b) {
    return *a < *b;
  }

  throw QueryError{ "operator <: invalid operands" };
}

inline model::Bool opLe(const model::Value& l, const model::Value& r) {
  auto a = boost::get<model::Number>(&l);
  auto b = boost::get<model::Number>(&r);

  if (a && b) {
    return *a <= *b;
  }

  throw QueryError{ "operator <=: invalid operands" };
}

inline model::Bool opGt(const model::Value& l, const model::Value& r) {
  auto a = boost::get<model::Number>(&l);
  auto b = boost::get<model::Number>(&r);

  if (a && b) {
    return *a > *b;
  }

  throw QueryError{ "operator >: invalid operands" };
}

inline model::Bool opGe(const model::Value& l, const model::Value& r) {
  auto a = boost::get<model::Number>(&l);
  auto b = boost::get<model::Number>(&r);

  if (a && b) {
    return *a >= *b;
  }

  throw QueryError{ "operator >=: invalid operands" };
}

inline model::Bool opEq(const model::Value& l, const model::Value& r) {
  return l == r;
}

inline model::Bool opNeq(const model::Value& l, const model::Value& r) {
  return !(l == r);
}

inline model::Value evalOperator(Operator op, const model::Value& l,
                                 const model::Value& r){
  switch (op) {
  case Operator::plus:
    return opPlus(l, r);
  case Operator::minus:
    return opMinus(l, r);
  case Operator::mul:
    return opMul(l, r);
  case Operator::div:
    return opDiv(l, r);
  case Operator::modulo:
    return opModulo(l, r);
  case Operator::lt:
    return opLt(l, r);
  case Operator::le:
    return opLe(l, r);
  case Operator::gt:
    return opGt(l, r);
  case Operator::ge:
    return opGe(l, r);
  case Operator::eq:
    return opEq(l, r);
  case Operator::neq:
    return opNeq(l, r);
  }
}

inline model::Value opNeg(const model::Value& v) {
  auto a = boost::get<model::Number>(&v);

  if (a) {
    return -(*a);
  }

  throw QueryError{ "operator -(unary): invalid operand" };
}

inline model::Value evalOperator(PrefixOperator op, const model::Value& v) {
  switch(op) {
    case PrefixOperator::neg:
      return opNeg(v);
  }
}

} // namespace eval
} // namespace query
} // namespace cheesebase
