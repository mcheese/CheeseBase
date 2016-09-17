#pragma once
#include "../ast.h"
#include "../db_session.h"
#include "sfw.h"
#include "env.h"

namespace cheesebase {
namespace query {
namespace eval {

Bindings evalFrom(const From& from, const Env& env, DbSession* session);

} // namespace eval
} // namespace query
} // namespace cheesebase
