#pragma once

namespace cheesebase {
namespace query {
namespace eval {

struct Config {
  enum class NavFailure { error, missing, null };

  struct {
    NavFailure absent{ NavFailure::error };
    NavFailure type_mismatch{ NavFailure::error };
  } tuple_nav;

  struct {
    NavFailure absent{ NavFailure::error };
    NavFailure type_mismatch{ NavFailure::error };
    bool allow_bag{ true };
  } array_nav;

} const kConf{};

} // namespace eval
} // namespace query
} // namespace cheesebase
