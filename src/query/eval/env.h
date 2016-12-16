#pragma once
#include "../../model/model.h"

namespace cheesebase {
namespace query {
namespace eval {

struct Env {
  const model::Tuple_base& self;
  const Env* next;
};

// Concat bindings to environment.
// Lifetime of result may not exceed lifetime of operands.
inline Env operator+(const model::Tuple_base& l, const Env& r) { return { l, &r }; }

} // namespace eval
} // namespace query
} // namespace cheesebase
