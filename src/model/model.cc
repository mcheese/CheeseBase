// Licensed under the Apache License 2.0 (see LICENSE file).

#include "model.h"
namespace cheesebase {
namespace model {

size_t valueExtraWords(uint8_t t) {
  if (t & 0b10000000) { return (t & 0b00111111) / 8 + 1; }
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
  if (m_childs.empty()) {
    m_childs = std::move(a);
  } else {
    for (auto& e : a) m_childs.insert(std::move(e));
  }
}

void Object::append(std::pair<Key, PValue> p) { m_childs.insert(std::move(p)); }

void Object::append(Key k, PValue v) {
  m_childs.emplace(std::move(k), std::move(v));
}

void Object::reserve(size_t s) { m_childs.reserve(s); }

std::ostream& Object::print(std::ostream& os) const {
  auto beg = std::begin(m_childs);
  auto end = std::end(m_childs);
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
  auto beg = std::begin(m_childs);
  auto end = std::end(m_childs);
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
  return m_childs.cbegin();
}

Map<Key, PValue>::const_iterator Object::end() const {
  return m_childs.cend();
}

std::ostream& operator<<(std::ostream& os, Null) { return os << "null"; }

std::ostream& operator<<(std::ostream& os, Bool b) {
  return os << (b ? "true" : "false");
}

std::ostream& Scalar::print(std::ostream& os) const { return os << m_data; }

std::ostream& Scalar::prettyPrint(std::ostream& os, size_t depth) const {
  if (m_data.type() == boost::typeindex::type_id<Bool>())
    return os << (boost::get<Bool>(m_data) ? "true" : "false");
  auto q = (m_data.type() == boost::typeindex::type_id<String>() ? "\"" : "");
  return os << q << m_data << q;
}

ValueType Scalar::type() const {
  if (m_data.type() == boost::typeindex::type_id<Number>()) {
    return ValueType::number;
  }
  if (m_data.type() == boost::typeindex::type_id<String>()) {
    auto len = boost::get<String>(m_data).size();
    if (len > k_short_string_limit) {
      return ValueType::string;
    } else {
      return ValueType(gsl::narrow_cast<uint8_t>(0b10000000 + len));
    }
  }
  if (m_data.type() == boost::typeindex::type_id<Bool>()) {
    return (boost::get<Bool>(m_data) ? ValueType::boolean_true
                                     : ValueType::boolean_false);
  }
  if (m_data.type() == boost::typeindex::type_id<Null>()) {
    return ValueType::null;
  }
  throw ModelError("Invalid scalar type");
}

std::vector<uint64_t> Scalar::extraWords() const {
  std::vector<uint64_t> ret;

  if (m_data.type() == boost::typeindex::type_id<Number>()) {
    ret.push_back(*((uint64_t*)&boost::get<Number>(m_data)));
  } else if (m_data.type() == boost::typeindex::type_id<String>()) {
    auto& str = boost::get<String>(m_data);
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
        } else {
          word >>= 8;
        }
      }
      if (i > 0) {
        word >>= 8 * (7 - i);
        ret.push_back(word);
      }
    }
  }

  return ret;
}

void Array::append(PValue v) { m_childs.push_back(std::move(v)); }

std::ostream& Array::print(std::ostream& os) const {
  os << "[";
  auto beg = std::begin(m_childs);
  auto end = std::end(m_childs);
  for (auto it = beg; it != end; ++it) {
    if (it != beg) os << ",";
    (*it)->print(os);
  }
  return os << ']';
}

std::ostream& Array::prettyPrint(std::ostream& os, size_t depth) const {
  auto indent = std::string(depth, ' ');
  os << "[";
  auto beg = std::begin(m_childs);
  auto end = std::end(m_childs);
  for (auto it = beg; it != end; ++it) {
    if (it != beg) os << ",";
    os << '\n' << indent << "  ";
    (*it)->prettyPrint(os, depth + 2);
  }
  return os << '\n' << indent << ']';
}

ValueType Array::type() const { return ValueType::list; }

std::vector<uint64_t> Array::extraWords() const {
  return std::vector<uint64_t>({ 0 });
}

} // namespace model
} // namespace cheesebase
