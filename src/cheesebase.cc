// Licensed under the Apache License 2.0 (see LICENSE file).

#include "cheesebase.h"

#include "core.h"
#include "disk_object.h"
#include "disk_array.h"
#include "parser.h"
#include "exceptions.h"
#include <sstream>

namespace cheesebase {

////////////////////////////////////////////////////////////////////////////////
// free functions

namespace {

// Traverses elements of location read only, the last element is opened writable
// and returned.
// May throw NotFoundError.
std::unique_ptr<disk::ValueW> openWritable(Transaction& ta,
                                           Location::const_iterator loc,
                                           Location::const_iterator loc_end) {
  if (loc == loc_end) {
    return std::make_unique<disk::ObjectW>(ta, kRoot);

  } else {
    std::unique_ptr<disk::ValueR> container =
        std::make_unique<disk::ObjectR>(ta.db(), kRoot);

    // loc is random-access-iterator, so end - 1 is OK
    for (; loc != loc_end; loc++) {
      if (loc->which() == 0) {
        auto obj = dynamic_cast<disk::ObjectR*>(container.get());
        if (obj == nullptr) throw NotFoundError();
        if (loc == loc_end - 1)
          return obj->getChildCollectionW(ta, boost::get<std::string>(*loc));
        container = obj->getChildCollectionR(boost::get<std::string>(*loc));
      } else {
        auto obj = dynamic_cast<disk::ArrayR*>(container.get());
        if (obj == nullptr) throw NotFoundError();
        if (loc == loc_end - 1)
          return obj->getChildCollectionW(ta, boost::get<uint64_t>(*loc));
        container = obj->getChildCollectionR(boost::get<uint64_t>(*loc));
      }
    }
  }

  // this actually should never be reached
  throw NotFoundError();
}

// Traverses elements of location read only and returns the last.
// May throw NotFoundError.
std::unique_ptr<disk::ValueR> openReadonly(Database& db,
                                           Location::const_iterator loc,
                                           Location::const_iterator loc_end) {
  std::unique_ptr<disk::ValueR> container =
      std::make_unique<disk::ObjectR>(db, kRoot);

  for (; loc != loc_end; loc++) {
    if (loc->which() == 0) {
      auto obj = dynamic_cast<disk::ObjectR*>(container.get());
      if (obj == nullptr) throw NotFoundError();
      container = obj->getChildCollectionR(boost::get<std::string>(*loc));
    } else {
      auto arr = dynamic_cast<disk::ArrayR*>(container.get());
      if (arr == nullptr) throw NotFoundError();
      container = arr->getChildCollectionR(boost::get<uint64_t>(*loc));
    }
  }
  if (!container) throw NotFoundError();
  return container;
}

void insertValue(Database& db, Location::const_iterator loc,
                 Location::const_iterator loc_end, const std::string& key,
                 const model::Value& val, disk::Overwrite ow) {
  auto ta = db.startTransaction();
  auto coll = openWritable(ta, loc, loc_end);

  auto obj = dynamic_cast<disk::ObjectW*>(coll.get());
  if (obj == nullptr) throw NotFoundError();

  if (obj->insert(key, val, ow))
    ta.commit(obj->getWrites());
  else
    throw CRUDError();
}

void insertValue(Database& db, Location::const_iterator loc,
                 Location::const_iterator loc_end, uint64_t index,
                 const model::Value& val, disk::Overwrite ow) {
  auto ta = db.startTransaction();
  auto coll = openWritable(ta, loc, loc_end);

  auto obj = dynamic_cast<disk::ArrayW*>(coll.get());
  if (obj == nullptr) throw NotFoundError();

  if (obj->insert(Key(index), val, ow))
    ta.commit(obj->getWrites());
  else
    throw CRUDError();
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////
// Query

Query::Query(CheeseBase* cb, Location loc, std::string key) : cb_{ cb } {
  loc.push_back(std::move(key));
  location_ = std::move(loc);
}

Query::Query(CheeseBase* cb, Location loc, uint64_t index) : cb_{ cb } {
  loc.push_back(index);
  location_ = std::move(loc);
}

Query Query::operator[](std::string key) {
  return Query(cb_, location_, std::move(key));
}

Query Query::operator[](uint64_t index) { return Query(cb_, location_, index); }

void Query::insert(const std::string& key, const model::Value& val) {
  cb_->insert(key, val, location_);
}

void Query::insert(uint64_t index, const model::Value& val) {
  cb_->insert(index, val, location_);
}

void Query::insert(const model::Value& val) {
  Expects(location_.size() > 0);

  if (location_.back().which() == 0) {
    insertValue(*cb_->db_, location_.cbegin(), location_.cend() - 1,
                boost::get<std::string>(location_.back()), val,
                disk::Overwrite::Insert);
  } else {
    insertValue(*cb_->db_, location_.begin(), location_.end() - 1,
                boost::get<uint64_t>(location_.back()), val,
                disk::Overwrite::Insert);
  }
}

void Query::update(const std::string& key, const model::Value& val) {
  cb_->update(key, val, location_);
}

void Query::update(uint64_t index, const model::Value& val) {
  cb_->update(index, val, location_);
}

void Query::update(const model::Value& val) {
  Expects(location_.size() > 0);

  if (location_.back().which() == 0) {
    insertValue(*cb_->db_, location_.cbegin(), location_.cend() - 1,
                boost::get<std::string>(location_.back()), val,
                disk::Overwrite::Update);
  } else {
    insertValue(*cb_->db_, location_.begin(), location_.end() - 1,
                boost::get<uint64_t>(location_.back()), val,
                disk::Overwrite::Update);
  }
}

void Query::upsert(const std::string& key, const model::Value& val) {
  cb_->upsert(key, val, location_);
}

void Query::upsert(uint64_t index, const model::Value& val) {
  cb_->upsert(index, val, location_);
}

void Query::upsert(const model::Value& val) {
  Expects(location_.size() > 0);

  if (location_.back().which() == 0) {
    insertValue(*cb_->db_, location_.cbegin(), location_.cend() - 1,
                boost::get<std::string>(location_.back()), val,
                disk::Overwrite::Upsert);
  } else {
    insertValue(*cb_->db_, location_.begin(), location_.end() - 1,
                boost::get<uint64_t>(location_.back()), val,
                disk::Overwrite::Upsert);
  }
}

uint64_t Query::append(const model::Value& val) {
  return cb_->append(val, location_);
}

std::unique_ptr<model::Value> Query::get() const { return cb_->get(location_); }

void Query::remove() { cb_->remove(location_); }

////////////////////////////////////////////////////////////////////////////////
// CheeseBase

CheeseBase::CheeseBase(const std::string& db_name)
    : db_(std::make_unique<Database>(db_name)) {}

CheeseBase::~CheeseBase() {}

Query CheeseBase::operator[](std::string key) {
  return Query(this, {}, std::move(key));
}

Query CheeseBase::operator[](uint64_t index) { return Query(this, {}, index); }

void CheeseBase::insert(const std::string& key, const model::Value& val,
                        const Location& location) {
  insertValue(*db_, location.begin(), location.end(), key, val,
              disk::Overwrite::Insert);
}

void CheeseBase::insert(uint64_t index, const model::Value& val,
                        const Location& location) {
  insertValue(*db_, location.begin(), location.end(), index, val,
              disk::Overwrite::Insert);
}

void CheeseBase::update(const std::string& key, const model::Value& val,
                        const Location& location) {
  insertValue(*db_, location.begin(), location.end(), key, val,
              disk::Overwrite::Update);
}

void CheeseBase::update(uint64_t index, const model::Value& val,
                        const Location& location) {
  insertValue(*db_, location.begin(), location.end(), index, val,
              disk::Overwrite::Update);
}

void CheeseBase::upsert(const std::string& key, const model::Value& val,
                        const Location& location) {
  insertValue(*db_, location.begin(), location.end(), key, val,
              disk::Overwrite::Upsert);
}

void CheeseBase::upsert(uint64_t index, const model::Value& val,
                        const Location& location) {
  insertValue(*db_, location.begin(), location.end(), index, val,
              disk::Overwrite::Upsert);
}

uint64_t CheeseBase::append(const model::Value& val, const Location& loc) {
  if (loc.empty()) throw NotFoundError();
  auto ta = db_->startTransaction();

  auto coll = openWritable(ta, loc.begin(), loc.end());
  auto arr = dynamic_cast<disk::ArrayW*>(coll.get());
  if (arr == nullptr) throw NotFoundError();

  auto ret = arr->append(val);

  ta.commit(coll->getWrites());

  return ret.value;
}

std::unique_ptr<model::Value> CheeseBase::get(const Location& location) const {
  std::unique_ptr<model::Value> ret;

  if (location.empty()) {
    ret = disk::ObjectR(*db_, kRoot).getValue();

  } else {
    auto coll = openReadonly(*db_, location.begin(), location.end() - 1);

    if (location.back().which() == 0) {
      auto obj = dynamic_cast<disk::ObjectR*>(coll.get());
      if (obj == nullptr) throw NotFoundError();
      ret = obj->getChildValue(boost::get<std::string>(location.back()));
    } else {
      auto arr = dynamic_cast<disk::ArrayR*>(coll.get());
      if (arr == nullptr) throw NotFoundError();
      ret = arr->getChildValue(boost::get<uint64_t>(location.back()));
    }
  }

  if (!ret) throw NotFoundError();
  return ret;
}

void CheeseBase::remove(const Location& location) {
  if (location.empty()) throw CRUDError();
  auto ta = db_->startTransaction();
  auto parent = openWritable(ta, location.begin(), location.end() - 1);

  bool success;
  if (location.back().which() == 0) {
    auto obj = dynamic_cast<disk::ObjectW*>(parent.get());
    if (obj == nullptr) throw NotFoundError();
    success = obj->remove(boost::get<std::string>(location.back()));
  } else {
    auto arr = dynamic_cast<disk::ArrayW*>(parent.get());
    if (arr == nullptr) throw NotFoundError();
    success = arr->remove(Key(boost::get<uint64_t>(location.back())));
  }

  if (success)
    ta.commit(parent->getWrites());
  else
    throw NotFoundError();
}

} // namespace cheesebase
