// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include <exception>
#include <string>

namespace cheesebase {

struct CheeseBaseError : public std::exception {
  CheeseBaseError() : text_{ "CheeseBaseError" } {}
  CheeseBaseError(std::string text) : text_{ std::move(text) } {}
  const char* what() const noexcept { return text_.c_str(); }
  std::string text_;
};

#define DEF_EXCEPTION(NAME)                                                    \
  struct NAME : public CheeseBaseError {                                       \
    NAME() : CheeseBaseError(#NAME) {}                                         \
    NAME(std::string text) : CheeseBaseError(text) {}                          \
  };

DEF_EXCEPTION(ConsistencyError);
DEF_EXCEPTION(NotFoundError);
DEF_EXCEPTION(CRUDError);
DEF_EXCEPTION(UnknownKeyError);
DEF_EXCEPTION(IndexOutOfRangeError);
DEF_EXCEPTION(DatabaseError);
DEF_EXCEPTION(FileError);
DEF_EXCEPTION(AllocError);
DEF_EXCEPTION(BlockLockError);
DEF_EXCEPTION(ModelError);
DEF_EXCEPTION(KeyCacheError);
DEF_EXCEPTION(ParserError);

#undef DEF_EXCEPTION

} // namespace cheesebase
