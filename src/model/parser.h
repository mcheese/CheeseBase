#pragma once

#include "model.h"

#include <boost/fusion/adapted/std_pair.hpp>
#include <boost/fusion/include/adapt_struct.hpp>
#include <boost/spirit/home/x3.hpp>
#include <boost/optional.hpp>

namespace x3 = boost::spirit::x3;

namespace cheesebase {
namespace parser {

///////////////////////////////////////////////////////////////////////////////
// Rules

const x3::rule<class null, model::Null> null = "null";
const x3::rule<class missing, model::Missing> missing = "missing";
const x3::rule<class number, model::Number> number = "number";
const x3::rule<class boolean, model::Bool> boolean = "boolean";
const x3::rule<class string, model::String> string = "string";
const x3::rule<class scalar, model::Value> scalar = "scalar";
const x3::rule<class tuple_mem, model::Tuple_member> tuple_mem = "tuple_mem";
const x3::rule<class tuple_val, model::Tuple> tuple_val =  "tuple_val";
const x3::rule<class collection, model::Collection> collection = "collection";
const x3::rule<class value, model::Value> value = "value";

///////////////////////////////////////////////////////////////////////////////
// Grammar

const auto null_def = x3::lit("null") >> x3::attr(model::Null());
const auto missing_def = x3::lit("missing") >> x3::attr(model::Missing());
const auto number_def = x3::double_;
const auto boolean_def = x3::lit("true") >> x3::attr(true) |
                         "false" >> x3::attr(false);
const auto string_def = x3::lexeme['"' >> *(~x3::char_('"')) >> '"'];
const auto scalar_def = missing | null | number | boolean | string;
const auto tuple_mem_def = string > ':' > value;
const auto tuple_val_def = x3::lit('{') > -(tuple_mem % ',') > '}';
const auto collection_def = x3::lit('[') > -(value % ',') > ']';
const auto value_def = tuple_val | collection | scalar;

///////////////////////////////////////////////////////////////////////////////

BOOST_SPIRIT_DEFINE(null, missing, number, boolean, string, scalar, tuple_mem,
                    tuple_val, collection, value);

} // namespace parser
} // namespace cheesebase
