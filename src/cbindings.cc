// Licensed under the Apache License 2.0 (see LICENSE file).

// External interface. Not yet implemented.

#include "cheesebase.h"
#include <string>
#include <cstring>

extern "C" {

using namespace cheesebase;

CheeseBase* cheesebase_open(const char* file) {
  try {
    return new CheeseBase(file);
  } catch (std::exception) { return nullptr; }
}

void cheesebase_close(CheeseBase* self) { delete self; }
}
