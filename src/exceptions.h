// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include <exception>
#include <string>

namespace cheesebase {

class CheeseBaseError : public std::exception {
public:
  CheeseBaseError() : text_{ "CheeseBaseError" } {}
  explicit CheeseBaseError(std::string text) : text_{ std::move(text) } {}
  virtual const char* what() const noexcept override { return text_.c_str(); }

private:
  std::string text_;
};

#define DEF_EXCEPTION(NAME)                                                    \
  class NAME : public CheeseBaseError {                                        \
    using CheeseBaseError::CheeseBaseError;                                    \
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
DEF_EXCEPTION(QueryError);

#undef DEF_EXCEPTION

} // namespace cheesebase
