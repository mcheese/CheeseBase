#pragma once

#include "query_ast.h"
#include <ostream>

namespace cheesebase {
namespace query {

class DotPrinter {
public:
  DotPrinter(std::ostream& os) : os_{ os } { os_ << "digraph g {\n"; }
  ~DotPrinter() { os_ << "}\n"; }

  size_t operator()(const Expr& q) { return boost::apply_visitor(*this, q); }

  size_t operator()(const Var& q) { return printNode(q, "box"); }

  size_t operator()(const Select& q) {
    return boost::apply_visitor(*this, q);
  }

  size_t operator()(const SelectAttribute& q) {
    auto n = printNode("SelectAttribute");
    printEdge(n, (*this)(q.attr_));
    printEdge(n, (*this)(q.value_));
    return n;
  }

  size_t operator()(const SelectElement& q) {
    auto n = printNode("SelectExpr");
    printEdge(n, (*this)(q.expr_));
    return n;
  }

  size_t operator()(const From& q) { return boost::apply_visitor(*this, q); }

  size_t operator()(const FromEmpty&) {
    return printNode("FromEmpty");
  }

  size_t operator()(const FromCollection& q) {
    auto n = printNode("FromCollection");
    printEdge(n, (*this)(q.expr_));
    printEdge(n, (*this)(q.as_));
    printEdge(n, (*this)(q.at_));
    return n;
  }

  size_t operator()(const FromTuple& q) {
    auto n = printNode("FromTuple");
    printEdge(n, (*this)(q.expr_));
    printEdge(n, (*this)(q.as_name_));
    printEdge(n, (*this)(q.as_value_));
    return n;
  }

  size_t operator()(const FromInner& q) {
    auto n = printNode("FromInner");
    printEdge(n, (*this)(q.left_));
    printEdge(n, (*this)(q.right_));
    return n;
  }

  size_t operator()(const FromLeft& q) {
    auto n = printNode("FromLeft");
    printEdge(n, (*this)(q.left_));
    printEdge(n, (*this)(q.right_));
    return n;
  }

  size_t operator()(const FromFull& q) {
    auto n = printNode("FromLeft");
    printEdge(n, (*this)(q.left_));
    printEdge(n, (*this)(q.right_));
    printEdge(n, (*this)(q.cond_));
    return n;
  }

  size_t operator()(const Where& q) {
    auto n = printNode("Where");
    printEdge(n, (*this)(q.expr_));
    return n;
  }

  size_t operator()(const OrderBy& q) {
    auto n = printNode("OrderBy");
    for (auto& e : q) {
      auto o = printNode(e.desc_ ? "Desc" : "Asc");
      printEdge(n, o);
      printEdge(o, (*this)(e.expr_));
    }
    return n;
  }

  size_t operator()(const GroupBy& q) {
    auto n = printNode("GroupBy");
    for (auto& e : q) {
      auto k = printNode("", "point");
      printEdge(n, k);
      printEdge(k, (*this)(e.expr_));
      if (!e.as_.empty()) printEdge(k, (*this)(e.as_));
    }
    return n;
  }

  size_t operator()(const Limit& q) {
    auto n = printNode("Limit");
    printEdge(n, (*this)(q.expr_));
    return n;
  }

  size_t operator()(const Offset& q) {
    auto n = printNode("Offset");
    printEdge(n, (*this)(q.expr_));
    return n;
  }

  size_t operator()(const SfwQuery& q) {
    auto n = printNode("SfwQuery");
    printEdge(n, (*this)(q.select_));
    printEdge(n, (*this)(q.from_));
    if (q.where_) printEdge(n, (*this)(*q.where_));
    if (q.group_by_) printEdge(n, (*this)(*q.group_by_));
    if (q.order_by_) printEdge(n, (*this)(*q.order_by_));
    if (q.limit_) printEdge(n, (*this)(*q.limit_));
    if (q.offset_) printEdge(n, (*this)(*q.offset_));
    return n;
  }

  size_t operator()(const query::Array& q) {
    auto n =  printNode("Array");
    for (auto& v : q) {
      printEdge(n, (*this)(v));
    }
    return n;
  }

  size_t operator()(const query::Bag& q) {
    auto n =  printNode("Bag");
    for (auto& v : q) {
      printEdge(n, (*this)(v));
    }
    return n;
  }

  size_t operator()(const query::Tuple& q) {
    auto n =  printNode("Tuple");
    for (auto& v : q) {
      auto k = printNode("", "point");
      printEdge(n, k);
      printEdge(k, (*this)(v.first));
      printEdge(k, (*this)(v.second));
    }
    return n;
  }

  size_t operator()(const query::TupleNav& q) {
    auto n = printNode("TupleNav");
    printEdge(n, (*this)(q.base_));
    printEdge(n, (*this)(q.key_));
    return n;
  }

  size_t operator()(const query::ArrayNav& q) {
    auto n = printNode("ArrayNav");
    printEdge(n, (*this)(q.base_));
    printEdge(n, (*this)(q.idx_));
    return n;
  }

  size_t operator()(const query::InfixOp& q) {
    auto n = printNode([&q]() {
      switch (q.op_) {
      case Operator::plus:
        return "+";
      case Operator::minus:
        return "-";
      case Operator::mul:
        return "*";
      case Operator::div:
        return "/";
      case Operator::modulo:
        return "%";
      case Operator::lt:
        return "<";
      case Operator::le:
        return "<=";
      case Operator::gt:
        return ">";
      case Operator::ge:
        return ">=";
      case Operator::eq:
        return "==";
      case Operator::neq:
        return "!=";
      }
    }());
    printEdge(n, (*this)(q.left_));
    printEdge(n, (*this)(q.right_));
    return n;
  }

  size_t operator()(const query::PrefixOp& q) {
    auto n = printNode([&q]() {
      switch (q.op_) {
      case PrefixOperator::neg:
        return "-";
      }
    }());
    printEdge(n, (*this)(q.val_));
    return n;
  }

  size_t operator()(const query::Function& q) {
    auto n = printNode(q.name_);
    for (auto& e : q.arguments_) {
      printEdge(n, (*this)(e));
    }
    return n;
  }

  size_t operator()(const model::Value& q) {
    return boost::apply_visitor(*this, q);
  }

  size_t operator()(const model::Missing&) { return printNode("missing", "oval"); }
  size_t operator()(const model::Null&) { return printNode("null", "oval"); }
  size_t operator()(const model::Number& q) {
    return printNode(std::to_string(q), "oval");
  }
  size_t operator()(const model::Bool& q) {
    return printNode(q ? "true" : "false", "oval");
  }
  size_t operator()(const model::String& q) { return printNode("\\\"" + q + "\\\"", "oval"); }
  size_t operator()(const model::Shared<model::Tuple>& q) {
    auto n =  printNode("Tuple", "oval");
    for (auto& v : *q) {
      auto k = printNode("", "point");
      printEdge(n, k);
      printEdge(k, (*this)(v.first));
      printEdge(k, (*this)(v.second));
    }
    return n;
  }
  size_t operator()(const model::Shared<model::Collection>& q) {
    auto n =  printNode(q->has_order_ ? "Array" : "Bag", "oval");
    for (auto& v : *q) {
      printEdge(n, (*this)(v));
    }
    return n;
  }

private:
  size_t printNode(const std::string& name, const char* shape = "plaintext") {
    os_ << "  " << id_ << " [label=\"" << name << "\" shape=" << shape
        << "];\n";
    return id_++;
  }

  void printEdge(size_t from, size_t to) {
    os_ << "  " << from << " -> " << to << ";\n";
  }

  std::ostream& os_;
  size_t id_{ 0 };
};

}
}
