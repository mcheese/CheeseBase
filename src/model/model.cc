#include "model.h"

namespace cheesebase {
namespace model {

bool Tuple::operator==(const Tuple& r) const {
  return this->size() == r.size() &&
         std::equal(std::begin(*this), std::end(*this), std::begin(r));
}

bool Tuple::operator<(const Tuple& r) const {
  if (this->size() != r.size()) return this->size() < r.size();

  auto diff = std::mismatch(std::begin(*this), std::end(*this), std::begin(r),
                            std::end(r));
  if (diff == std::make_pair(std::end(*this), std::end(r))) return false;
  return *diff.first < *diff.second;
}

bool Collection::operator==(const Collection& r) const {
  return this->size() == r.size() &&
         std::equal(std::begin(*this), std::end(*this), std::begin(r));
}

bool Collection::operator<(const Collection& r) const {
  if (this->size() != r.size()) return this->size() < r.size();

  auto diff = std::mismatch(std::begin(*this), std::end(*this), std::begin(r),
                            std::end(r));
  if (diff == std::make_pair(std::end(*this), std::end(r))) return false;
  return *diff.first < *diff.second;
}

bool Value::operator==(const Value& r) const { return this->get() == r.get(); }

bool Value::operator<(const Value& r) const {
  if (get().which() != r.get().which()) return get().which() < r.get().which();

  return boost::apply_visitor(
      [&r](auto& ctx) { return ctx < boost::get<decltype(ctx)>(r.get()); },
      *this);
}

} // namespace model
} // namespace cheesebase
