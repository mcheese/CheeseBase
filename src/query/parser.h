#pragma once

#include "ast.h"
#include "../model/parser.h"
#include "../exceptions.h"

#include <boost/fusion/include/adapt_struct.hpp>
#include <boost/spirit/home/x3.hpp>
#include <boost/optional.hpp>

namespace x3 = boost::spirit::x3;

BOOST_FUSION_ADAPT_STRUCT(cheesebase::query::SelectAttribute,
                          (cheesebase::query::Expr, attr_)
                          (cheesebase::query::Expr, value_))

BOOST_FUSION_ADAPT_STRUCT(cheesebase::query::FromCollection,
                          (cheesebase::query::Expr, expr_)
                          (cheesebase::query::Var, as_)
                          (cheesebase::query::Var, at_))

BOOST_FUSION_ADAPT_STRUCT(cheesebase::query::GroupBy_term,
                          (cheesebase::query::Expr, expr_)
                          (cheesebase::query::Var, as_))

BOOST_FUSION_ADAPT_STRUCT(cheesebase::query::FromTuple,
                          (cheesebase::query::Expr, expr_)
                          (cheesebase::query::Var, as_name_)
                          (cheesebase::query::Var, as_value_))

BOOST_FUSION_ADAPT_STRUCT(cheesebase::query::SfwQuery,
                          (cheesebase::query::Select, select_)
                          (cheesebase::query::From, from_)
                          (boost::optional<cheesebase::query::Where>, where_)
                          (boost::optional<cheesebase::query::GroupBy>, group_by_)
                          (boost::optional<cheesebase::query::OrderBy>, order_by_)
                          (boost::optional<cheesebase::query::Limit>, limit_)
                          (boost::optional<cheesebase::query::Offset>, offset_))

BOOST_FUSION_ADAPT_STRUCT(cheesebase::query::Function,
                          (std::string, name_)
                          (std::vector<cheesebase::query::Expr>, arguments_))

namespace cheesebase {
namespace parser {

///////////////////////////////////////////////////////////////////////////////
// Rules

const x3::rule<class root_query, query::Expr> query = "query";

#define CB_RULE(a, b) const x3::rule<class b, a> b = #b

CB_RULE(query::Expr, expr);
CB_RULE(query::Expr, expr0);
CB_RULE(query::Expr, expr1);
CB_RULE(query::Expr, expr2);
CB_RULE(query::Expr, expr3);
CB_RULE(query::Expr, expr4);
CB_RULE(query::Expr, expr5);
CB_RULE(query::Expr, expr6);
CB_RULE(query::Expr, expr7);

CB_RULE(query::PrefixOperator, prefix1);
CB_RULE(query::Operator, infix1);
CB_RULE(query::Operator, infix2);
CB_RULE(query::Operator, infix3);
CB_RULE(query::Operator, infix4);

CB_RULE(query::Function, function);

CB_RULE(std::string, name);
CB_RULE(query::Var, var);

CB_RULE(query::SfwQuery, sfw_query);

CB_RULE(query::Select, select);
CB_RULE(query::SelectElement, select_element);
CB_RULE(query::Expr, select_element_expr);
CB_RULE(query::Tuple, select_element_list);
CB_RULE(query::Tuple_member, select_element_member);
CB_RULE(query::SelectAttribute, select_attribute);

CB_RULE(query::Where, where);
CB_RULE(query::OrderBy, order_by);
CB_RULE(query::OrderBy_term, order_by_term);
CB_RULE(query::Limit, limit);
CB_RULE(query::Offset, offset);
CB_RULE(query::GroupBy, group_by);
CB_RULE(query::GroupBy_term, group_by_term);

CB_RULE(query::From, from);
CB_RULE(query::From, from_item);
CB_RULE(query::From, from_item0);
CB_RULE(query::From, from_item1);
CB_RULE(query::From, from_item2);
CB_RULE(query::From, from_item3);
CB_RULE(query::From, from_item4);
CB_RULE(query::From, from_item5);
CB_RULE(query::From, from_item6);

CB_RULE(query::FromCollection, from_collection);
CB_RULE(query::FromTuple, from_tuple);
CB_RULE(query::FromInner, from_inner);
CB_RULE(query::FromLeft, from_left);
CB_RULE(query::FromFull, from_full);

CB_RULE(query::Tuple, tuple);
CB_RULE(query::Tuple_member, tuple_member);
CB_RULE(query::Array, array);
CB_RULE(query::Bag, bag);

#undef CB_RULE

///////////////////////////////////////////////////////////////////////////////
// Grammar
using x3::_attr;
using x3::_val;
using x3::lit;

const auto query_def =  sfw_query | expr;
const auto name_def = x3::lexeme[x3::lit('`') >> +(~x3::char_('`')) >> '`'] |
                      x3::lexeme[x3::alpha >> *(x3::alnum | x3::char_('_'))];
const auto var_def = name;

const auto function_def = name >> '(' >> -(expr % ',') >> ')';

// operator by precedence

template <typename T, T Op>
const auto sem_op = [](auto& ctx) { _val(ctx) = Op; };

using query::Operator;
using query::PrefixOperator;

const auto prefix1_def = lit('-')[sem_op<PrefixOperator, PrefixOperator::neg>];
const auto infix1_def = lit('*')[sem_op<Operator, Operator::mul>] |
                        lit('/')[sem_op<Operator, Operator::div>] |
                        lit('%')[sem_op<Operator, Operator::modulo>];
const auto infix2_def = lit('+')[sem_op<Operator, Operator::plus>] |
                        lit('-')[sem_op<Operator, Operator::minus>];
const auto infix3_def = lit("<=")[sem_op<Operator, Operator::le>] |
                        lit('<')[sem_op<Operator, Operator::lt>] |
                        lit(">=")[sem_op<Operator, Operator::ge>] |
                        lit('>')[sem_op<Operator, Operator::gt>];
const auto infix4_def = lit("==")[sem_op<Operator, Operator::eq>] |
                        lit('=')[sem_op<Operator, Operator::eq>] |
                        lit("!=")[sem_op<Operator, Operator::neq>] |
                        lit("<>")[sem_op<Operator, Operator::neq>];

// semantic action helper

const auto sem_assign = [](auto& ctx) {
  x3::_val(ctx) = std::move(x3::_attr(ctx));
};

template <typename Node>
const auto sem_rotate_3 = [](auto& ctx) {
  _val(ctx) =
      Node{ std::move(_val(ctx)), std::move(boost::fusion::at_c<0>(_attr(ctx))),
            std::move(boost::fusion::at_c<1>(_attr(ctx))) };
};

template <typename Node>
const auto sem_rotate_2 = [](auto& ctx) {
  _val(ctx) = Node{ std::move(_val(ctx)), std::move(_attr(ctx)) };
};

// expr by precedence

const auto expr0_def = x3::lit("(") >> sfw_query >> ")" | function | tuple |
                       array | bag | scalar | var;

const auto expr1_def = expr0 | (lit('(') >> expr >> ')');

const auto expr2_def = expr1[sem_assign] >>
                       *((lit('.') > name[sem_rotate_2<query::TupleNav>]) |
                         (lit('[') > expr[sem_rotate_2<query::ArrayNav>] >
                          ']'));

const auto expr3_def =
    (prefix1 >> expr)[([](auto& ctx) {
      _val(ctx) =
          query::PrefixOp{ boost::fusion::at_c<0>(_attr(ctx)),
                           std::move(boost::fusion::at_c<1>(_attr(ctx))) };
    })] |
    expr2[sem_assign];

const auto expr4_def = expr3[sem_assign] >>
                           *((infix1 >> expr3)[sem_rotate_3<query::InfixOp>]);
const auto expr5_def = expr4[sem_assign] >>
                           *((infix2 >> expr4)[sem_rotate_3<query::InfixOp>]);
const auto expr6_def = expr5[sem_assign] >>
                           *((infix3 >> expr5)[sem_rotate_3<query::InfixOp>]);
const auto expr7_def = expr6[sem_assign] >>
                           *((infix4 >> expr6)[sem_rotate_3<query::InfixOp>]);

const auto expr_def = expr7;

// select-from-where-query

const auto select_attribute_def = x3::lit("ATTRIBUTE") > expr > ':' > expr;
const auto select_element_expr_def = select_element_list;
const auto select_element_list_def = select_element_member % ',';
const auto select_element_member_def =
    expr[([](auto& ctx) { x3::_val(ctx).second = x3::_attr(ctx); })] >>
    (("AS" >> name)[([](auto& ctx) { x3::_val(ctx).first = x3::_attr(ctx); })] |
     x3::eps[([](auto& ctx) {
       auto name = boost::get<query::Var>(&_val(ctx).second);
       if (name == nullptr)
         throw ParserError{ "Could not derive name for SELECT pair" };
       _val(ctx).first = *name;
     })]);

const auto select_element_def =
    (x3::lit("ELEMENT") > expr) | select_element_expr;

const auto select_def = x3::lit("SELECT") > (select_attribute | select_element);

const auto where_def = x3::lit("WHERE") > expr;

const auto from_def = x3::lit("FROM") > from_item;
const auto from_item_def = from_item6;
const auto from_item0_def =
    from_collection | from_tuple | (lit('(') > from_item > lit(')'));

const auto from_item1_def =
    from_item0[sem_assign] >>
    *((lit("INNER JOIN") > from_collection > "ON" > expr)[([](auto& ctx) {
      auto& from = boost::fusion::at_c<0>(_attr(ctx));
      query::FromCollection right;
      right.as_ = from.as_;
      query::SfwQuery sfw;
      sfw.select_ = query::SelectElement(query::Expr(from.as_));
      sfw.from_ = std::move(from);
      sfw.where_ = boost::fusion::at_c<1>(_attr(ctx));
      right.expr_ = std::move(sfw);
      _val(ctx) = query::FromInner{ std::move(_val(ctx)),
                                    query::From(std::move(right)) };
    })]);

const auto from_item2_def =
    from_item1[sem_assign] >>
    *((lit("LEFT JOIN") > from_collection > "ON" > expr)[([](auto& ctx) {
      auto& from = boost::fusion::at_c<0>(_attr(ctx));
      query::FromCollection right;
      right.as_ = from.as_;
      query::SfwQuery sfw;
      sfw.select_ = query::SelectElement(query::Expr(from.as_));
      sfw.from_ = std::move(from);
      sfw.where_ = boost::fusion::at_c<1>(_attr(ctx));
      right.expr_ = std::move(sfw);
      _val(ctx) = query::FromLeft{ std::move(_val(ctx)),
                                   query::From(std::move(right)) };
    })]);

const auto from_item3_def =
    from_item2[sem_assign] >>
    *((lit("RIGHT JOIN") > from_item > "ON" > expr)[([](auto& ctx) {
      auto& from = boost::get<query::FromCollection>(_val(ctx));
      query::FromCollection right;
      right.as_ = from.as_;
      query::SfwQuery sfw;
      sfw.select_ = query::SelectElement(query::Expr(from.as_));
      sfw.from_ = std::move(from);
      sfw.where_ = boost::fusion::at_c<1>(_attr(ctx));
      right.expr_ = std::move(sfw);
      _val(ctx) =
          query::FromLeft{ std::move(boost::fusion::at_c<0>(_attr(ctx))),
                           query::From(std::move(right)) };
    })]);

const auto from_item4_def = from_item3[sem_assign] >>
                            *((lit("INNER") >> -lit("CORRELATE") >>
                               from_item3)[sem_rotate_2<query::FromInner>]);
const auto from_item5_def = from_item4[sem_assign] >>
                            *((lit("LEFT") >> -lit("OUTER") >> -lit("CORRELATE") >>
                               from_item4)[sem_rotate_2<query::FromLeft>]);
const auto from_item6_def =
    from_item5[sem_assign] >>
    *((lit("FULL") > (lit("JOIN") | (-lit("OUTER") > -lit("CORRELATE"))) >
       from_item5 > lit("ON") > expr)[sem_rotate_3<query::FromFull>]);

const auto from_collection_def = expr >> "AS" >> var >> -("AT" >> var);
const auto from_tuple_def = expr >> "AS" >> ('{' > var > ':' > var > '}');

const auto order_by_term_def =
    expr[([](auto& ctx) { _val(ctx).expr_ = _attr(ctx); })] >>
    -(lit("ASC") | lit("DESC")[([](auto& ctx) { _val(ctx).desc_ = true; })]);

const auto order_by_def = lit("ORDER BY") > (order_by_term % ',');

const auto limit_def = lit("LIMIT") > expr;

const auto offset_def = lit("OFFSET") > expr;

const auto group_by_term_def = expr >> -("AS" > var);
const auto group_by_def = lit("GROUP BY") > (group_by_term % ',');

const auto sfw_query_def = select > -(from > -where > -group_by > -order_by >
                                      -limit > -offset);

// collection constructor

const auto tuple_member_def = (name | string) >> ':' >> expr;

const auto tuple_def = x3::lit('{') > -(tuple_member % ',') > '}';

const auto array_def = x3::lit('[') > -(expr % ',') > ']';

const auto bag_def = x3::lit("{{") > -(expr % ',') > "}}";

///////////////////////////////////////////////////////////////////////////////

BOOST_SPIRIT_DEFINE(expr, expr0, expr1, expr2, expr3, expr4, expr5, expr6,
                    expr7, function, prefix1, infix1, infix2, infix3, infix4,
                    query, from, from_item, from_item0, from_item1, from_item2,
                    from_item3, from_item4, from_item5, from_item6,
                    from_collection, from_tuple, var, select, select_attribute,
                    select_element, select_element_expr, select_element_list,
                    select_element_member, where, order_by, order_by_term,
                    group_by_term, group_by, limit, offset, name, sfw_query,
                    tuple_member, tuple, bag, array)

} // namespace parser
} // namespace cheesebase
