// Licensed under the Apache License 2.0 (see LICENSE file).

#include "model.h"
namespace cheesebase {
namespace model {

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

std::ostream& Object::prettyPrint(std::ostream & os, size_t depth) const {
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

std::ostream& operator<<(std::ostream& os, Null) {
  return os << "null";
}

std::ostream& operator<<(std::ostream& os, Bool b) {
  return os << (b ? "true" : "false");
}

std::ostream & Scalar::print(std::ostream & os) const {
  return os << m_data;
}

std::ostream & Scalar::prettyPrint(std::ostream & os, size_t depth) const {
  if (m_data.type() == boost::typeindex::type_id<Bool>())
    return os << (boost::get<Bool>(m_data) ? "true" : "false");
  auto q = (m_data.type() == boost::typeindex::type_id<String>() ? "\"" : "");
  return os << q << m_data << q;
}

void Array::append(PValue v) {
  m_childs.push_back(std::move(v));
}

std::ostream& Array::print(std::ostream & os) const {
  os << "[";
  auto beg = std::begin(m_childs);
  auto end = std::end(m_childs);
  for (auto it = beg; it != end; ++it) {
    if (it != beg) os << ",";
    (*it)->print(os);
  }
  return os << ']';
}

std::ostream& Array::prettyPrint(std::ostream & os, size_t depth) const {
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

} // namespace model
} // namespace cheesebase
