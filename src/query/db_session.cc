#include "db_session.h"
#include "../core.h"
#include "../seri/object.h"

namespace cheesebase {
namespace query {

model::Value DbSession::getNamedVal(const std::string& name) {
  // TODO: store disk Values, lazy Value
  disk::ObjectR root_obj{ db_, kRoot };
  return root_obj.getChildValue(name);
}


} // namespace query
} // namespace cheesebase
