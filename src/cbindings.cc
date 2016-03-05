// Licensed under the Apache License 2.0 (see LICENSE file).

#define _CRT_NONSTDC_NO_DEPRECATE

#include "cheesebase.h"
#include <string>
#include <cstring>

extern "C" {

  CheeseBase* cheesebase_open(const char* file) {
    try {
      return new CheeseBase(file);
    } catch (std::exception) {
      return nullptr;
    }
  }

  void cheesebase_close(CheeseBase* self) { delete self; }

  const char* cheesebase_get(CheeseBase* self, const char* location) {
    auto ret = self->get(location);
    return strdup(ret.c_str());
  }

  int cheesebase_insert(CheeseBase* self, const char* location,
                        const char* json) {
    return self->insert(location, json);
  }

  int cheesebase_update(CheeseBase* self, const char* location,
                        const char* json) {
    return self->update(location, json);
  }

  int cheesebase_upsert(CheeseBase* self, const char* location,
                        const char* json) {
    return self->upsert(location, json);
  }

  int cheesebase_remove(CheeseBase* self, const char* location) {
    return self->remove(location);
  }
}
