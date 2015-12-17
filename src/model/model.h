// Licensed under the Apache License 2.0 (see LICENSE file).

// Class representation of the object model used in the database. Closely
// resembles JSON.
//
//   Object = [(Key, Value)]
//   Key = String
//   Value = Scalar | Object | Array
//   Array = [Value]
//   Scalar = String | Double | Bool | Null
//

#pragma once

#include <boost/container/flat_map.hpp>
#include <boost/container/stable_vector.hpp>
#include <boost/variant.hpp>
#include <memory>
#include <string>
#include "common/common.h"

namespace cheesebase {

DEF_EXCEPTION(ModelError);

namespace model {

////////////////////////////////////////////////////////////////////////////////
// on disk type info
constexpr size_t k_short_string_limit = 24;

enum ValueType : uint8_t {
  object = 'O',
  list = 'A',
  number = 'N',
  string = 'S',
  boolean_true = 'T',
  boolean_false = 'F',
  null = '0'
};

size_t valueExtraWords(uint8_t t);

////////////////////////////////////////////////////////////////////////////////
// scalar
using String = std::string;
using Number = double;
static_assert(sizeof(Number) == 8, "\'double\' should be 64 bit");
using Bool = bool;
class Null {};

using Key = String;

////////////////////////////////////////////////////////////////////////////////
// collections
template <typename K, typename V>
using Map = boost::container::flat_map<K, V>;

template <typename T>
using List = boost::container::stable_vector<T>;

////////////////////////////////////////////////////////////////////////////////
// values
class Value {
public:
  virtual ~Value() = default;
  virtual std::ostream& print(std::ostream& os) const = 0;
  virtual std::ostream& prettyPrint(std::ostream& os, size_t depth) const = 0;
  virtual ValueType type() const = 0;
  virtual std::vector<uint64_t> extraWords() const = 0;
};

using PValue = std::unique_ptr<Value>;

class Object : public Value {
public:
  Object() = default;
  Object(Map<Key, PValue> childs) : m_childs(std::move(childs)) {}
  ~Object() override = default;
  MOVE_ONLY(Object);

  void append(Map<Key, PValue> a);
  void append(std::pair<Key, PValue> p);
  void append(Key, PValue);

  void reserve(size_t s);

  std::ostream& print(std::ostream& os) const override;
  std::ostream& prettyPrint(std::ostream& os, size_t depth = 0) const override;
  ValueType type() const override;
  std::vector<uint64_t> extraWords() const override;

  Map<Key, PValue>::const_iterator begin() const;
  Map<Key, PValue>::const_iterator end() const;

private:
  Map<Key, PValue> m_childs;
};

class Array : public Value {
public:
  Array() = default;
  Array(List<PValue> childs) : m_childs(std::move(childs)) {}
  ~Array() override = default;
  MOVE_ONLY(Array);

  void append(PValue v);

  std::ostream& print(std::ostream& os) const override;
  std::ostream& prettyPrint(std::ostream& os, size_t depth = 0) const override;
  ValueType type() const override;
  std::vector<uint64_t> extraWords() const override;

private:
  List<PValue> m_childs;
};

class Scalar : public Value {
public:
  template <typename T>
  Scalar(T a) : m_data{ a } {};

  ~Scalar() override = default;

  std::ostream& print(std::ostream& os) const override;
  std::ostream& prettyPrint(std::ostream& os, size_t depth) const override;
  ValueType type() const override;
  std::vector<uint64_t> extraWords() const override;

private:
  boost::variant<String, Number, Bool, Null> m_data;
};

} // namespace model
} // namespace cheesebase
