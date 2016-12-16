#include "array.h"
#include "../model/model.h"

namespace cheesebase {
namespace disk {

model::Value ArrayR::getValue() { return getArray(); }

model::Value ArrayR::getChildValue(uint64_t index) {
  if (index > Key::sMaxKey) throw IndexOutOfRangeError();
  return tree_.getChildValue(Key(index));
}

std::unique_ptr<ValueW> ArrayR::getChildCollectionW(Transaction& ta,
                                                    uint64_t index) {
  if (index > Key::sMaxKey) throw IndexOutOfRangeError();
  return tree_.getChildCollectionW(ta, Key(index));
}

std::unique_ptr<ValueR> ArrayR::getChildCollectionR(uint64_t index) {
  if (index > Key::sMaxKey) throw IndexOutOfRangeError();
  return tree_.getChildCollectionR(Key(index));
}

model::Collection ArrayR::getArray() {
  model::Collection val;
  val.has_order_ = true;

  auto arr = tree_.getArray();
  auto last = arr.rbegin();
  if (last != arr.rend()) {
    val.resize(last->first + 1, model::Missing{});

    for (auto& e : arr) {
      val[e.first] = std::move(e.second);
    }
  }

  return val;
}

} // namespace disk
} // namespace cheesebase
