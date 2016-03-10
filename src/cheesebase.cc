// Licensed under the Apache License 2.0 (see LICENSE file).

#include "cheesebase.h"

#include "core.h"
#include "disk_object.h"
#include "parser.h"
#include <sstream>

using namespace cheesebase;

CheeseBase::CheeseBase(const std::string& db_name)
    : db_(std::make_unique<Database>(db_name)) {}

CheeseBase::~CheeseBase() {}

bool CheeseBase::insert(const std::string& location, const std::string& json) {
  try {
    auto value = parseJson(json.begin(), json.end());
    auto ta = db_->startTransaction();
    disk::ObjectW object{ ta, k_root };
    auto success = object.insert(location, *value, disk::Overwrite::Insert);
    if (success) ta.commit(object.getWrites());
    return success;
  } catch (std::exception& e) {
    std::cerr << e.what() << std::endl;
    return false;
  }
}

bool CheeseBase::update(const std::string& location, const std::string& json) {
  try {
    auto value = parseJson(json.begin(), json.end());
    auto ta = db_->startTransaction();
    disk::ObjectW object{ ta, k_root };
    auto success = object.insert(location, *value, disk::Overwrite::Update);
    if (success) ta.commit(object.getWrites());
    return success;
  } catch (std::exception& e) {
    std::cerr << e.what() << std::endl;
    return false;
  }
}

bool CheeseBase::upsert(const std::string& location, const std::string& json) {
  try {
    auto value = parseJson(json.begin(), json.end());
    auto ta = db_->startTransaction();
    disk::ObjectW object{ ta, k_root };
    auto success = object.insert(location, *value, disk::Overwrite::Upsert);
    if (success) ta.commit(object.getWrites());
    return success;
  } catch (std::exception& e) {
    std::cerr << e.what() << std::endl;
    return false;
  }
}

std::string CheeseBase::get(const std::string& location) {
  try {
    std::ostringstream ss;
    auto object = disk::ObjectR(*db_, k_root);
    auto v = (location.length() == 0 ? object.getValue()
                                     : object.getChildValue(location));
    if (v) v->prettyPrint(dynamic_cast<std::ostream&>(ss), 0);
    return ss.str();
  } catch (std::exception& e) {
    std::cerr << e.what() << std::endl;
    return {};
  }
}

bool CheeseBase::remove(const std::string& location) {
  try {
    auto ta = db_->startTransaction();
    disk::ObjectW object{ ta, k_root };
    auto success = object.remove(location);
    if (success) ta.commit(object.getWrites());
    return success;
  } catch (std::exception& e) {
    std::cerr << e.what() << std::endl;
    return false;
  }
}
