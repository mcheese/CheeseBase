// Licensed under the Apache License 2.0 (see LICENSE file).
#pragma once

#include "../common.h"
#include "../exceptions.h"
#include "../model/model.h"

namespace cheesebase {
namespace disk {

constexpr size_t kShortStringMaxLen = 24;

enum ValueType : uint8_t {
  object = 'O',
  array = 'A',
  number = 'N',
  string = 'S',
  boolean_true = 'T',
  boolean_false = 'F',
  null = '0',
  missing = 'M'
};

inline ValueType valueType(const model::Value& val) {
  struct Visitor {
    ValueType operator()(const model::STuple&) const {
      return ValueType::object;
    }
    ValueType operator()(const model::SCollection&) const {
      return ValueType::array;
    }
    ValueType operator()(const model::Number&) const {
      return ValueType::number;
    }
    ValueType operator()(const model::String& s) const {
      if (s.size() > kShortStringMaxLen) {
        return ValueType::string;
      } else {
        return ValueType(gsl::narrow_cast<uint8_t>(0b10000000 + s.size()));
      }
    }
    ValueType operator()(const model::Bool& b) const {
      return b ? ValueType::boolean_true : ValueType::boolean_false;
    }
    ValueType operator()(const model::Null&) const {
      return ValueType::null;
    }
    ValueType operator()(const model::Missing&) const {
      return ValueType::missing;
    }
  };

  return boost::apply_visitor(Visitor{}, val);
}

inline size_t nrExtraWords(uint8_t t) {
  if (t & 0b10000000) {
    size_t l = (t & 0b00111111);
    return (l == 0 ? 0 : (l - 1) / 8 + 1);
  }
  switch (t) {
  case ValueType::object:
  case ValueType::array:
  case ValueType::number:
  case ValueType::string:
    return 1;
  case ValueType::boolean_true:
  case ValueType::boolean_false:
  case ValueType::null:
    return 0;
  default:
    // Missing should be excluded earlier
    throw ConsistencyError("Unknown value type");
  }
}

inline size_t nrExtraWords(const model::Value& val) {
  return nrExtraWords(valueType(val));
}

inline std::vector<uint64_t> extraWords(const model::Value& val) {
  std::vector<uint64_t> ret;

  if (val.get().type() == typeid(model::Number)) {
    double n = boost::get<model::Number>(val);
    ret.push_back(*(reinterpret_cast<uint64_t*>(&n)));
  } else if (val.get().type() == typeid(model::String)) {
    auto& str = boost::get<model::String>(val);
    if (str.size() > kShortStringMaxLen) {
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

} // namespace disk
} // namespace cheesebase
