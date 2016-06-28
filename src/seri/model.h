// Licensed under the Apache License 2.0 (see LICENSE file).
#pragma once

#include "../exceptions.h"
#include "../model.h"

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
  null = '0'
};

inline ValueType valueType(const model::Value& val) {
  auto t = val.type();
  switch (t) {
  case model::Type::Object:
    return ValueType::object;
  case model::Type::Array:
    return ValueType::array;
  case model::Type::Number:
    return ValueType::number;
  case model::Type::Null:
    return ValueType::null;
  case model::Type::Bool:
    return (dynamic_cast<const model::Scalar&>(val).getBool()
                ? ValueType::boolean_true
                : ValueType::boolean_false);
  case model::Type::String:
    auto len = dynamic_cast<const model::Scalar&>(val).getString().size();
    if (len > kShortStringMaxLen) {
      return ValueType::string;
    } else {
      return ValueType(gsl::narrow_cast<uint8_t>(0b10000000 + len));
    }
  }

  throw ConsistencyError("Unknown type");
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
    throw ConsistencyError("Unknown value type");
  }
}

inline size_t nrExtraWords(const model::Value& val) {
  return nrExtraWords(valueType(val));
}

inline std::vector<uint64_t> extraWords(const model::Scalar& val) {
  std::vector<uint64_t> ret;

  if (val.type() == model::Type::Number) {
    double n = val.getNumber();
    ret.push_back(*(reinterpret_cast<uint64_t*>(&n)));
  } else if (val.type() == model::Type::String) {
    auto& str = val.getString();
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
