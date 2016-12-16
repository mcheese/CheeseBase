#include "model.h"

namespace cheesebase {
namespace model {

Value& Tuple_lazy::at(const String& k) {
  auto lookup = cache_.find(k);
  if (lookup != std::end(cache_))
    return lookup->second;
  auto emplace = cache_.emplace(k, ref_->getChildValue(k));
  return emplace.first->second;
}

const Value& Tuple_lazy::at(const String& k) const {
  auto lookup = cache_.find(k);
  if (lookup != std::end(cache_))
    return lookup->second;
  auto emplace = cache_.emplace(k, ref_->getChildValue(k));
  return emplace.first->second;
}

Tuple_base& Tuple::getBase() const {
  auto base = boost::get<Tuple_base>(&impl_);
  if (base) {
    return *base;
  }

  impl_ = boost::get<Tuple_lazy>(impl_).ref_->getObject().impl_;
  return boost::get<Tuple_base>(impl_);
}

Value& Tuple::operator[](const String& k) {
  auto base = boost::get<Tuple_base>(&impl_);
  if (base) return (*base)[k];

  return boost::get<Tuple_lazy>(impl_).at(k);
}

Value& Tuple::at(const String& k) {
  auto base = boost::get<Tuple_base>(&impl_);
  if (base) return base->at(k);

  return boost::get<Tuple_lazy>(impl_).at(k);
}

const Value& Tuple::at(const String& k) const {
  auto base = boost::get<Tuple_base>(&impl_);
  if (base) return base->at(k);

  return boost::get<Tuple_lazy>(impl_).at(k);
}

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

Value& Collection_lazy::at(size_t k) {
  auto lookup = cache_.find(k);
  if (lookup != std::end(cache_))
    return lookup->second;
  auto emplace = cache_.emplace(k, ref_->getChildValue(k));
  return emplace.first->second;
}

const Value& Collection_lazy::at(size_t k) const {
  auto lookup = cache_.find(k);
  if (lookup != std::end(cache_))
    return lookup->second;
  auto emplace = cache_.emplace(k, ref_->getChildValue(k));
  return emplace.first->second;
}

Collection_base& Collection::getBase() const {
  auto base = boost::get<Collection_base>(&impl_);
  if (base) {
    return *base;
  }

  impl_ = boost::get<Collection_lazy>(impl_).ref_->getArray().impl_;
  return boost::get<Collection_base>(impl_);
}

Value& Collection::operator[](size_t k) {
  auto base = boost::get<Collection_base>(&impl_);
  if (base) return (*base)[k];

  return boost::get<Collection_lazy>(impl_).at(k);
}

Value& Collection::at(size_t k) {
  auto base = boost::get<Collection_base>(&impl_);
  if (base) return base->at(k);

  return boost::get<Collection_lazy>(impl_).at(k);
}

const Value& Collection::at(size_t k) const {
  auto base = boost::get<Collection_base>(&impl_);
  if (base) return base->at(k);

  return boost::get<Collection_lazy>(impl_).at(k);
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

void fetchAll(const Value& val) {
  if (auto tuple = boost::get<STuple>(&val)) {
    (*tuple)->fetch();
    for (auto& e : **tuple) fetchAll(e.second);
  }

  if(auto coll = boost::get<SCollection>(&val)) {
    (*coll)->fetch();
    for (auto& e : **coll) fetchAll(e);
  }
}

} // namespace model
} // namespace cheesebase
