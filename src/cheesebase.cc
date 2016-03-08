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
    disk::BtreeWritable tree{ ta, k_root };
    auto success = tree.insert(location, *value, disk::Overwrite::Insert);
    if (success) ta.commit(tree.getWrites());
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
    disk::BtreeWritable tree{ ta, k_root };
    auto success = tree.insert(location, *value, disk::Overwrite::Update);
    if (success) ta.commit(tree.getWrites());
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
    disk::BtreeWritable tree{ ta, k_root };
    auto success = tree.insert(location, *value, disk::Overwrite::Upsert);
    if (success) ta.commit(tree.getWrites());
    return success;
  } catch (std::exception& e) {
    std::cerr << e.what() << std::endl;
    return false;
  }
}

std::string CheeseBase::get(const std::string& location) {
  try {
    std::ostringstream ss;
    auto tree = disk::BtreeReadOnly(*db_, k_root);
    auto v = (location.length() == 0
                  ? std::make_unique<model::Object>(tree.getObject())
                  : tree.getValue(location));
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
    disk::BtreeWritable tree{ ta, k_root };
    auto success = tree.remove(location);
    if (success) ta.commit(tree.getWrites());
    return success;
  } catch (std::exception& e) {
    std::cerr << e.what() << std::endl;
    return false;
  }
}
