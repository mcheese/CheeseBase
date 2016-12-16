#pragma once

#include "../seri/array.h"
#include "../seri/object.h"

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

///////////////////////////////////////////////////////////////////////////////
// Tuple (with lazy fetch)

using Tuple_base = std::map<String, Value>;
using Tuple_member = std::pair<String, Value>;

struct Tuple_lazy {
  Tuple_lazy(std::unique_ptr<disk::ObjectR>&& p) : ref_{ std::move(p) } {}

  Value& at(const String& k);
  const Value& at(const String& k) const;

  mutable std::unique_ptr<disk::ObjectR> ref_;
  mutable Tuple_base cache_;
};

struct Tuple : boost::totally_ordered<Tuple> {
  Tuple() = default;
  template <class... Args>
  Tuple(Args&&... args) : impl_{ std::forward<Args>(args)... } {}

  Tuple(Tuple_base&& x) : impl_{ std::move(x) } {}
  Tuple(const Tuple_base& x) : impl_{ x } {}

  void fetch() const { getBase(); }

  operator Tuple_base() const { return getBase(); }

  auto begin() { return getBase().begin(); }
  auto end() { return getBase().end(); }
  auto begin() const { return getBase().cbegin(); }
  auto end() const { return getBase().cend(); }
  auto size() const { return getBase().size(); }
  auto find(const String& k) const { return getBase().find(k); }
  auto empty() const { return getBase().empty(); }
  auto count(const String& k) const { return getBase().count(k); }
  Value& operator[](const String& k);
  Value& at(const String& k);
  const Value& at(const String& k) const;

  template <class... Args>
  auto insert(Args&&... args) {
    return getBase().insert(std::forward<Args>(args)...);
  }

  template <class... Args>
  auto emplace(Args&&... args) {
    return getBase().emplace(std::forward<Args>(args)...);
  }

  bool operator==(const Tuple& r) const;
  bool operator<(const Tuple& r) const;

private:
  Tuple_base& getBase() const;
  mutable x3::variant<Tuple_base, Tuple_lazy> impl_;
};

///////////////////////////////////////////////////////////////////////////////
// Collection (with lazy fetch)

using Collection_base = std::vector<Value>;

struct Collection_lazy {
  Collection_lazy(std::unique_ptr<disk::ArrayR>&& p) : ref_{ std::move(p) } {}

  Value& at(size_t k);
  const Value& at(size_t k) const;

  mutable std::unique_ptr<disk::ArrayR> ref_;
  mutable std::map<size_t, Value> cache_;
};

struct Collection : boost::totally_ordered<Collection> {
  Collection() = default;
  template <class... Args>
  Collection(Args&&... args) : impl_{ std::forward<Args>(args)... } {}

  Collection(Collection_base&& x) : impl_{ std::move(x) } {}
  Collection(const Collection_base& x) : impl_{ x } {}

  bool has_order_{ false };

  void fetch() const { getBase(); }

  operator Collection_base() const { return getBase(); }

  auto begin() { return getBase().begin(); }
  auto end() { return getBase().end(); }
  auto begin() const { return getBase().cbegin(); }
  auto end() const { return getBase().cend(); }
  auto size() const { return getBase().size(); }
  auto empty() const { return getBase().empty(); }
  auto reserve(size_t s) const { return getBase().reserve(s); }
  Value& operator[](size_t k);
  Value& at(size_t k);
  const Value& at(size_t k) const;

  template <class... Args>
  auto resize(Args&&... args) {
    return getBase().resize(std::forward<Args>(args)...);
  }

  template <class... Args>
  auto emplace_back(Args&&... args) {
    return getBase().emplace_back(std::forward<Args>(args)...);
  }

  template <class... Args>
  auto push_back(Args&&... args) {
    return getBase().push_back(std::forward<Args>(args)...);
  }

  bool operator==(const Collection& r) const;
  bool operator<(const Collection& r) const;

private:
  Collection_base& getBase() const;
  mutable x3::variant<Collection_base, Collection_lazy> impl_;
};

///////////////////////////////////////////////////////////////////////////////

template<typename T>
struct Shared : boost::totally_ordered<Shared<T>>  {
  Shared() : ptr_{ std::make_shared<T>() } {}
  Shared(T&& t) : ptr_{ std::make_shared<T>(std::move(t)) } {}

  std::shared_ptr<const T> ptr_;

  const T* operator->() const { return ptr_.get(); }
  const T& operator*() const { return *ptr_; }
  bool operator==(const Shared<T>& r) const { return **this == *r; }
  bool operator<(const Shared<T>& r) const { return **this < *r; }
  Shared<T>& operator=(const T& r) {
    ptr_ = std::make_shared<T>(r);
    return *this;
  }
  Shared<T>& operator=(T&& r) {
    ptr_ = std::make_shared<T>(std::move(r));
    return *this;
  }
};

void fetchAll(const Value& val);

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
  Value(Tuple_base&& x) : base_type{ Tuple(std::move(x)) } {}
  Value(const Tuple_base& x) : base_type{ Tuple(x) } {}
  Value(Collection_base&& x) : base_type{ Collection(std::move(x)) } {}
  Value(const Collection_base& x) : base_type{ Collection(x) } {}
  Value(Collection&& x) : base_type{ std::move(x) } {}

  bool operator==(const Value& r) const;
  bool operator<(const Value& r) const;

  Value& fetch() {
    fetchAll(*this);
    return *this;
  }
};



} // namespace model
} // namespace cheesebase
