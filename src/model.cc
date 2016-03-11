// Licensed under the Apache License 2.0 (see LICENSE file).

#include "model.h"
namespace cheesebase {
namespace model {

size_t valueExtraWords(uint8_t t) {
  if (t & 0b10000000) {
    size_t l = (t & 0b00111111);
    return (l == 0 ? 0 : (l - 1) / 8 + 1);
  }
  switch (t) {
  case model::ValueType::object:
  case model::ValueType::list:
  case model::ValueType::number:
  case model::ValueType::string:
    return 1;
  case model::ValueType::boolean_true:
  case model::ValueType::boolean_false:
  case model::ValueType::null:
    return 0;
  default:
    throw ConsistencyError("Unknown value type");
  }
}

void Object::append(Map<Key, PValue> a) {
  if (childs_.empty()) { childs_ = std::move(a); } else {
    for (auto& e : a) childs_.insert(std::move(e));
  }
}

void Object::append(std::pair<Key, PValue> p) { childs_.insert(std::move(p)); }

void Object::append(Key k, PValue v) {
  childs_.emplace(std::move(k), std::move(v));
}

boost::optional<const Value&> Object::getChild(Key k) const {
  auto lookup = childs_.find(k);
  if (lookup != childs_.end()) return *lookup->second;
  return boost::none;
}

std::ostream& Object::print(std::ostream& os) const {
  auto beg = std::begin(childs_);
  auto end = std::end(childs_);
  for (auto it = beg; it != end; ++it) {
    if (it != beg) os << ", ";
    os << it->first << " : ";
    it->second->print(os);
  }
  return os;
}

std::ostream& Object::prettyPrint(std::ostream& os, size_t depth) const {
  auto indent = std::string(depth, ' ');
  os << "{";
  auto beg = std::begin(childs_);
  auto end = std::end(childs_);
  for (auto it = beg; it != end; ++it) {
    if (it != beg) os << ",";
    os << '\n' << indent << "  \"" << it->first << "\": ";
    it->second->prettyPrint(os, depth + 2);
  }
  return os << '\n' << indent << '}';
}

ValueType Object::type() const { return ValueType::object; }

std::vector<uint64_t> Object::extraWords() const {
  return std::vector<uint64_t>({ 0 });
}

Map<Key, PValue>::const_iterator Object::begin() const {
  return childs_.cbegin();
}

Map<Key, PValue>::const_iterator Object::end() const { return childs_.cend(); }

bool Object::operator==(const Value& o) const {
  auto other = dynamic_cast<const Object*>(&o);
  if (other == nullptr) return false;
  for (const auto& c : childs_) {
    auto partner = other->getChild(c.first);
    if (!partner.is_initialized() || *c.second != partner.value()) return false;
  }
  return true;
}

std::ostream& operator<<(std::ostream& os, Null) { return os << "null"; }

std::ostream& operator<<(std::ostream& os, Bool b) {
  return os << (b ? "true" : "false");
}

std::ostream& Scalar::print(std::ostream& os) const { return os << data_; }

std::ostream& Scalar::prettyPrint(std::ostream& os, size_t depth) const {
  if (data_.type() == boost::typeindex::type_id<Bool>())
    return os << (boost::get<Bool>(data_) ? "true" : "false");
  auto q = (data_.type() == boost::typeindex::type_id<String>() ? "\"" : "");
  return os << q << data_ << q;
}

ValueType Scalar::type() const {
  if (data_.type() == boost::typeindex::type_id<Number>()) {
    return ValueType::number;
  }
  if (data_.type() == boost::typeindex::type_id<String>()) {
    auto len = boost::get<String>(data_).size();
    if (len > k_short_string_limit) { return ValueType::string; } else {
      return ValueType(gsl::narrow_cast<uint8_t>(0b10000000 + len));
    }
  }
  if (data_.type() == boost::typeindex::type_id<Bool>()) {
    return (boost::get<Bool>(data_) ? ValueType::boolean_true
                                    : ValueType::boolean_false);
  }
  if (data_.type() == boost::typeindex::type_id<Null>()) {
    return ValueType::null;
  }
  throw ModelError("Invalid scalar type");
}

std::vector<uint64_t> Scalar::extraWords() const {
  std::vector<uint64_t> ret;

  if (data_.type() == boost::typeindex::type_id<Number>()) {
    ret.push_back(*((uint64_t*)&boost::get<Number>(data_)));
  } else if (data_.type() == boost::typeindex::type_id<String>()) {
    auto& str = boost::get<String>(data_);
    if (str.size() > k_short_string_limit) {
      ret.push_back(0);
    } else {
      uint64_t word = 0;
      size_t i = 0;
      for (uint64_t c : str) {
        word += c << 56;
        if (++i == 8) {
          ret.push_back(word);
          word = 0;
          i = 0;
        } else { word >>= 8; }
      }
      if (i > 0) {
        word >>= 8 * (7 - i);
        ret.push_back(word);
      }
    }
  }

  return ret;
}

bool Scalar::operator==(const Value& o) const {
  auto other = dynamic_cast<const Scalar*>(&o);
  if (other == nullptr) return false;
  return data_ == other->data_;
}

Array::Array(std::vector<PValue>&& childs) {
  Index i = 0;
  for (auto& c : childs) { childs_.emplace(i++, std::move(c)); }
}

void Array::append(PValue v) {
  if (childs_.empty()) { childs_.emplace(0, std::move(v)); } else {
    auto max_key = childs_.rbegin()->first;
    childs_.emplace(max_key + 1, std::move(v));
  }
}

void Array::append(std::pair<Index, PValue> p) { childs_.insert(std::move(p)); }

void Array::append(Index idx, PValue val) {
  childs_.emplace(idx, std::move(val));
}

boost::optional<const Value&> Array::getChild(Index k) const {
  auto lookup = childs_.find(k);
  if (lookup != childs_.end()) return *lookup->second;
  return boost::none;
}

Map<Index, PValue>::const_iterator Array::begin() const {
  return childs_.cbegin();
}

Map<Index, PValue>::const_iterator Array::end() const { return childs_.cend(); }

std::ostream& Array::print(std::ostream& os) const {
  os << "[";
  auto beg = std::begin(childs_);
  auto end = std::end(childs_);
  Index i = 0;
  for (auto it = beg; it != end; ++it) {
    if (it != beg) os << ",";
    while (i++ < it->first) os << "null,";
    it->second->print(os);
  }
  return os << ']';
}

std::ostream& Array::prettyPrint(std::ostream& os, size_t depth) const {
  auto indent = std::string(depth, ' ');
  os << "[";
  auto beg = std::begin(childs_);
  auto end = std::end(childs_);
  Index i = 0;
  for (auto it = beg; it != end; ++it) {
    if (it != beg) os << ",";
    os << '\n' << indent << "  ";
    while (i++ < it->first) {
      os << "null,";
      os << '\n' << indent << "  ";
    }
    it->second->prettyPrint(os, depth + 2);
  }
  return os << '\n' << indent << ']';
}

ValueType Array::type() const { return ValueType::list; }

std::vector<uint64_t> Array::extraWords() const {
  return std::vector<uint64_t>({ 0 });
}

bool Array::operator==(const Value& o) const {
  auto other = dynamic_cast<const Array*>(&o);
  if (other == nullptr) return false;

  auto l = childs_.begin();
  auto r = other->childs_.begin();

  while (l != childs_.end() && r != other->childs_.end()) {
    if (l->first == r->first) {
      if (*l->second != *r->second) { return false; }
      l++;
      r++;
    } else if (*l->second == Scalar(Null())) {
      l++;
    } else if (*r->second == Scalar(Null())) { r++; } else {
      return false;
    }
  }
  while (l != childs_.end()) {
    if (*(l++)->second != Scalar(Null())) return false;
  }
  while (r != other->childs_.end()) {
    if (*(r++)->second != Scalar(Null())) return false;
  }
  return true;
}

} // namespace model
} // namespace cheesebase
