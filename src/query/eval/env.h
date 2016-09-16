#pragma once
#include "../../model/model.h"

namespace cheesebase {
namespace query {
namespace eval {

struct Env {
  const model::Tuple& self;
  const Env* next;
};

// Concat bindings to environment.
// Lifetime of result may not exceed lifetime of operands.
inline Env operator+(const model::Tuple& l, const Env& r) { return { l, &r }; }

} // namespace eval
} // namespace query
} // namespace cheesebase
