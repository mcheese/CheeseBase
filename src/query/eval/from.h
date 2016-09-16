#pragma once
#include "../ast.h"
#include "sfw.h"
#include "env.h"

namespace cheesebase {
namespace query {
namespace eval {

Bindings evalFrom(const From& from, const Env& env);

} // namespace eval
} // namespace query
} // namespace cheesebase
