// Licensed under the Apache License 2.0 (see LICENSE file).

#include "cheesebase.h"

#include "core.h"
#include "model/btree.h"
#include "model/parser.h"
#include <sstream>

using namespace cheesebase;

CheeseBase::CheeseBase(const std::string& db_name)
    : m_db(std::make_unique<Database>(db_name)) {}

CheeseBase::~CheeseBase() {}

bool CheeseBase::insert(const std::string& location, const std::string& json) {
  try {
    auto value = parseJson(json.begin(), json.end());
    auto ta = m_db->startTransaction();
    btree::BtreeWritable tree{ ta, k_root };
    auto success = tree.insert(location, *value, btree::Overwrite::Insert);
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
    auto ta = m_db->startTransaction();
    btree::BtreeWritable tree{ ta, k_root };
    auto success = tree.insert(location, *value, btree::Overwrite::Update);
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
    auto ta = m_db->startTransaction();
    btree::BtreeWritable tree{ ta, k_root };
    auto success = tree.insert(location, *value, btree::Overwrite::Upsert);
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
    auto tree = btree::BtreeReadOnly(*m_db, k_root);
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
    auto ta = m_db->startTransaction();
    btree::BtreeWritable tree{ ta, k_root };
    auto success = tree.remove(location);
    if (success) ta.commit(tree.getWrites());
    return success;
  } catch (std::exception& e) {
    std::cerr << e.what() << std::endl;
    return false;
  }
}
