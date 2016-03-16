// Licensed under the Apache License 2.0 (see LICENSE file).

#include "model.h"
#include "exceptions.h"
#include <sstream>
#include <iomanip>

namespace cheesebase {
namespace model {

const Value& Value::operator[](Key) const { throw ModelError(); }
const Value& Value::operator[](Index) const { throw ModelError(); }

std::string Value::toString() const {
  std::stringstream ss;
  print(ss);
  return ss.str();
}

void Object::append(Map<Key, PValue> a) {
  if (childs_.empty()) {
    childs_ = std::move(a);
  } else {
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

const Value& Object::operator[](Key k) const {
  auto c = getChild(k);
  if (!c) throw ModelError("Key not found");
  return *c;
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

Type Object::type() const { return Type::Object; }

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

std::string escapeJson(const std::string& s) {
  std::ostringstream o;
  for (auto c = s.cbegin(); c != s.cend(); c++) {
    switch (*c) {
    case '"':
      o << "\\\"";
      break;
    case '\\':
      o << "\\\\";
      break;
    case '\b':
      o << "\\b";
      break;
    case '\f':
      o << "\\f";
      break;
    case '\n':
      o << "\\n";
      break;
    case '\r':
      o << "\\r";
      break;
    case '\t':
      o << "\\t";
      break;
    default:
      if ('\x00' <= *c && *c <= '\x1f') {
        o << "\\u" << std::hex << std::setw(4) << std::setfill('0') << (int)*c;
      } else {
        o << *c;
      }
    }
  }
  return o.str();
}

std::ostream& Scalar::print(std::ostream& os) const {
  return prettyPrint(os, 0);
}

std::ostream& Scalar::prettyPrint(std::ostream& os, size_t depth) const {
  if (data_.type() == boost::typeindex::type_id<Bool>())
    return os << (boost::get<Bool>(data_) ? "true" : "false");

  if (data_.type() == boost::typeindex::type_id<String>()) {
    return os << '"' << escapeJson(boost::get<String>(data_)) << '"';
  }

  return os << data_;
}

Type Scalar::type() const {
  if (data_.type() == boost::typeindex::type_id<Number>()) {
    return Type::Number;
  }
  if (data_.type() == boost::typeindex::type_id<String>()) {
    return Type::String;
  }
  if (data_.type() == boost::typeindex::type_id<Bool>()) {
    return Type::Bool;
  }
  if (data_.type() == boost::typeindex::type_id<Null>()) {
    return Type::Null;
  }
  throw ModelError("Invalid scalar type");
}

Bool Scalar::getBool() const { return boost::get<Bool>(data_); }
Number Scalar::getNumber() const { return boost::get<Number>(data_); }
const String& Scalar::getString() const { return boost::get<String>(data_); }

bool Scalar::operator==(const Value& o) const {
  auto other = dynamic_cast<const Scalar*>(&o);
  if (other == nullptr) return false;
  return data_ == other->data_;
}

Array::Array(std::vector<PValue>&& childs) {
  Index i = 0;
  for (auto& c : childs) {
    childs_.emplace(i++, std::move(c));
  }
}

void Array::append(PValue v) {
  if (childs_.empty()) {
    childs_.emplace(0, std::move(v));
  } else {
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

const Value& Array::operator[](Index i) const {
  auto c = getChild(i);
  if (!c) throw ModelError("Index not found");
  return *c;
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

Type Array::type() const { return Type::Array; }

bool Array::operator==(const Value& o) const {
  auto other = dynamic_cast<const Array*>(&o);
  if (other == nullptr) return false;

  auto l = childs_.begin();
  auto r = other->childs_.begin();

  while (l != childs_.end() && r != other->childs_.end()) {
    if (l->first == r->first) {
      if (*l->second != *r->second) {
        return false;
      }
      l++;
      r++;
    } else if (*l->second == Scalar(Null())) {
      l++;
    } else if (*r->second == Scalar(Null())) {
      r++;
    } else {
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
