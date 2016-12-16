#include "object.h"
#include "../model/model.h"

namespace cheesebase {
namespace disk {

model::Value ObjectR::getValue() { return tree_.getObject(); }

model::Value ObjectR::getChildValue(const std::string& key) {
  auto k = db_.getKey(key);
  if (!k) return model::Missing{};
  return tree_.getChildValue(*k);
}

std::unique_ptr<disk::ValueW>
ObjectR::getChildCollectionW(Transaction& ta, const std::string& key) {
  auto k = db_.getKey(key);
  if (!k) throw UnknownKeyError();
  return tree_.getChildCollectionW(ta, *k);
}

std::unique_ptr<disk::ValueR>
ObjectR::getChildCollectionR(const std::string& key) {
  auto k = db_.getKey(key);
  if (!k) throw UnknownKeyError();
  return tree_.getChildCollectionR(*k);
}

model::Tuple ObjectR::getObject() { return tree_.getObject(); }

} // namespace disk
} // namespace cheesebase
