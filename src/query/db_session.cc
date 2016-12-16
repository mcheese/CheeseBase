#include "db_session.h"
#include "../core.h"
#include "../seri/object.h"

namespace cheesebase {
namespace query {

const model::Tuple& DbSession::getRoot() {
  if (!val_) val_ = model::Tuple(std::make_unique<disk::ObjectR>(db_, kRoot));

  return val_.value();
}


} // namespace query
} // namespace cheesebase
