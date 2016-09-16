#pragma once
#include <string>
#include <map>
#include <vector>
#include <boost/spirit/home/x3/support/ast/variant.hpp>
#include <boost/operators.hpp>

namespace x3 = boost::spirit::x3;

namespace cheesebase {
namespace model {

// Scalar
struct Missing : boost::totally_ordered<Missing> {
  bool operator==(Missing) const { return true; }
  bool operator!=(Missing) const { return false; }
  bool operator<(Missing) const { return false; }
};

struct Null : boost::totally_ordered<Null> {
  bool operator==(Null) const { return true; }
  bool operator!=(Null) const { return false; }
  bool operator<(Null) const { return false; }
};

using Number =  double;
using Bool = bool;
using String = std::string;

// Complex
struct Value;

using Tuple_base = std::map<String, Value>;
using Tuple_member = std::pair<String, Value>;

struct Tuple : Tuple_base, boost::totally_ordered<Tuple> {
  using Tuple_base::Tuple_base;
  using Tuple_base::operator=;

  bool operator==(const Tuple& r) const;
  bool operator<(const Tuple& r) const;
};

using Collection_base = std::vector<Value>;
struct Collection : Collection_base, boost::totally_ordered<Collection> {
  using Collection_base::Collection_base;
  using Collection_base::operator=;

  bool has_order_{ false };

  bool operator==(const Collection& r) const;
  bool operator<(const Collection& r) const;
};

template<typename T>
struct Shared : boost::totally_ordered<Shared<T>>  {
  Shared() : ptr_{ std::make_shared<T>() } {}
  Shared(T&& t) : ptr_{ std::make_shared<T>(std::move(t)) } {}

  std::shared_ptr<const T> ptr_;

  const T* operator->() const { return ptr_.get(); }
  const T& operator*() const { return *ptr_; }
  bool operator==(const Shared<T>& r) const { return **this == *r; }
  bool operator<(const Shared<T>& r) const { return **this < *r; }
};

using STuple = Shared<Tuple>;
using SCollection = Shared<Collection>;

// Root
struct Value : public x3::variant<Missing, Null, Number, Bool, String,
                                  STuple, SCollection>,
               boost::totally_ordered<Value> {
  using base_type::base_type;
  using base_type::operator=;

  Value() = default;
  Value(Missing) : base_type{ Missing() } {}
  Value(Null) : base_type{ Null() } {}
  Value(Number&& x) : base_type{ std::move(x) } {}
  Value(Bool&& x) : base_type{ std::move(x) } {}
  Value(String&& x) : base_type{ std::move(x) } {}
  Value(const char* x) : base_type{ String(x) } {}
  Value(Tuple&& x) : base_type{ std::move(x) } {}
  Value(Collection&& x) : base_type{ std::move(x) } {}

  bool operator==(const Value& r) const;
  bool operator<(const Value& r) const;
};

} // namespace model
} // namespace cheesebase
