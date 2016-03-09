// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "disk_value.h"
#include "model.h"

namespace cheesebase {
namespace disk {

class StringW : public ValueW {
public:
  StringW(Transaction& ta, model::String str);
  StringW(Transaction& ta, Addr addr);
  Writes getWrites() const override;
  void destroy() override;

private:
  model::String str_;
  std::vector<Block> blocks_;
  uint64_t hdr_;
};

class StringR : public ValueR {
public:
  StringR(Database& db, Addr addr);
  model::PValue getValue() override;
};

} // namespace disk
} // namespace cheesebase
