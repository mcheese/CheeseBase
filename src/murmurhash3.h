//-----------------------------------------------------------------------------
// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.

#pragma once

#include <stdint.h>
#include <string>

namespace cheesebase {

uint32_t hashString(std::string str);

} // namespace cheesebase
