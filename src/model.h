// Licensed under the Apache License 2.0 (see LICENSE file).

// Class representation of the object model used in the database.
// Basically JSON.
//
//   Object = [(Key, Value)]
//   Key = String
//   Value = Scalar | Object | Array
//   Array = [Value]
//   Scalar = String | Double | Bool | Null
//

#pragma once

#include "common.h"
#include "macros.h"

#include <map>
#include <boost/variant.hpp>
#include <boost/optional.hpp>
#include <memory>
#include <string>

namespace cheesebase {

namespace model {

enum class Type { Object, Array, Number, String, Bool, Null };

using String = std::string;
using Number = double;
static_assert(sizeof(Number) == 8, "\'double\' should be 64 bit");
using Bool = bool;
struct Null {
  bool operator==(const Null&) const { return true; }
  bool operator!=(const Null&) const { return false; }
};

using Key = String;
using Index = uint64_t;

template <typename K, typename V>
using Map = std::map<K, V>;

////////////////////////////////////////////////////////////////////////////////
// values
class Value {
public:
  virtual ~Value() = default;
  virtual std::ostream& print(std::ostream& os) const = 0;
  virtual std::ostream& prettyPrint(std::ostream& os,
                                    size_t depth = 0) const = 0;
  std::ostream& operator<<(std::ostream& os) { return print(os); };
  std::string toString() const;
  virtual Type type() const = 0;
  virtual bool operator==(const Value& o) const = 0;
  bool operator!=(const Value& o) const { return !(*this == o); }
  virtual const Value& operator[](Key) const;
  virtual const Value& operator[](Index) const;
};

using PValue = std::unique_ptr<Value>;

class Object : public Value {
public:
  Object() = default;
  Object(Map<Key, PValue> childs) : childs_(std::move(childs)) {}
  ~Object() override = default;
  MOVE_ONLY(Object);

  void append(Map<Key, PValue> a);
  void append(std::pair<Key, PValue> p);
  void append(Key, PValue);

  boost::optional<const Value&> getChild(Key) const;
  const Value& operator[](Key) const override;

  std::ostream& print(std::ostream& os) const override;
  std::ostream& prettyPrint(std::ostream& os, size_t depth = 0) const override;
  Type type() const override;

  Map<Key, PValue>::const_iterator begin() const;
  Map<Key, PValue>::const_iterator end() const;

  bool operator==(const Value& o) const override;

private:
  Map<Key, PValue> childs_;
};

class Array : public Value {
public:
  Array() = default;
  Array(Map<Index, PValue> childs) : childs_(std::move(childs)) {}
  Array(std::vector<PValue>&& childs);
  ~Array() override = default;
  MOVE_ONLY(Array);

  void append(PValue v);
  void append(std::pair<Index, PValue> p);
  void append(Index, PValue);

  boost::optional<const Value&> getChild(Index) const;
  const Value& operator[](Index) const override;

  std::ostream& print(std::ostream& os) const override;
  std::ostream& prettyPrint(std::ostream& os, size_t depth = 0) const override;
  Type type() const override;

  Map<Index, PValue>::const_iterator begin() const;
  Map<Index, PValue>::const_iterator end() const;

  bool operator==(const Value& o) const override;

private:
  Map<Index, PValue> childs_;
};

class Scalar : public Value {
public:
  using value_type = boost::variant<String, Number, Bool, Null>;
  template <typename T>
  Scalar(T a)
      : data_{ a } {};

  Scalar(const char* s) : data_{ std::string(s) } {}

  ~Scalar() override = default;

  std::ostream& print(std::ostream& os) const override;
  std::ostream& prettyPrint(std::ostream& os, size_t depth = 0) const override;
  Type type() const override;
  bool operator==(const Value& o) const override;
  const value_type& data() const { return data_; }

  Bool getBool() const;
  const String& getString() const;
  Number getNumber() const;

private:
  value_type data_;
};

} // namespace model
} // namespace cheesebase
