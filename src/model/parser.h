// Licensed under the Apache License 2.0 (see LICENSE file).

#include "common/common.h"
#include "model.h"
#include <cstdlib>

namespace cheesebase {

DEF_EXCEPTION(ParserError);

template <class InputIt>
model::Object parseJson(InputIt begin, InputIt end) {
  return JsonParser<InputIt>::parse(begin, end);
}

template <class InputIt>
class JsonParser {
public:
  static model::Object parse(InputIt begin, InputIt end) {
    return JsonParser(begin, end).parseDoc();
  };

private:
  JsonParser(InputIt begin, InputIt end) : it(begin), end(end) {}
  InputIt it;
  InputIt end;

  model::Object parseDoc() {
    skipWs();
    auto doc = parseObject();
    skipWs();
    if (it != end) throw ParserError("more data than expected");
    return doc;
  }

  using C = const char;

  void skipWs() noexcept {
    while (it != end && isspace(*it)) ++it;
  }

  void expect(C c) {
    if (it == end || *it != c)
      throw ParserError(std::string("expected '") + c + "'");
    ++it;
  }

  bool check(C c) const { return (it != end && *it == c); }

  model::String parseString() {
    expect('"');
    model::String str;
    for (; it != end && *it != '\"'; ++it) {
      auto c = *it;
      if (!isprint(c)) throw ParserError("expected printable character");
      if (c == '\\') {
        if (++it == end)
          throw ParserError("unexpected end while reading string");
        c = *it;
        if (c == '\"' || c == '\\')
          ;
        else if (c == 'n')
          c = '\n';
        else if (c == 'b')
          c = '\b';
        else if (c == 't')
          c = '\t';
        else if (c == 'r')
          c = '\r';
        else
          throw ParserError("unexpected character after '\\'");
      }
      str.push_back(c);
    }
    expect('"');
    return str;
  }

  model::PValue parseNumber() {
    std::string tmp;
    while (it != end && ((*it >= '0' && *it <= '9') || *it == '.' ||
                         *it == '-' || *it == '+' || *it == 'e' || *it == 'E'))
      tmp.push_back(*(it++));
    char* last;
    auto num = strtod(tmp.c_str(), &last);
    if (errno == ERANGE) {
      errno = 0;
      throw ParserError("number out of range");
    }
    if (last - tmp.c_str() != tmp.size()) {
      throw ParserError("invalid number");
    }
    return model::PValue(new model::Scalar(num));
  }

  model::PValue parseBool() {
    if (check('t')) {
      ++it;
      expect('r');
      expect('u');
      expect('e');
      return model::PValue(new model::Scalar(true));
    } else if (*it == 'f') {
      ++it;
      expect('a');
      expect('l');
      expect('s');
      expect('e');
      return model::PValue(new model::Scalar(false));
    }
    throw ParserError("expected bool");
  }

  model::PValue parseNull() {
    expect('n');
    expect('u');
    expect('l');
    expect('l');
    return model::PValue(new model::Scalar(model::Null()));
  }

  model::Object parseObject() {
    model::Object obj;

    expect('{');
    skipWs();
    if (!check('}')) {
      auto key = parseString();
      skipWs();
      expect(':');
      skipWs();
      auto val = parseValue();
      obj.append(std::move(key), std::move(val));
      skipWs();
      while (check(',')) {
        ++it;
        skipWs();
        auto key = parseString();
        skipWs();
        expect(':');
        skipWs();
        auto val = parseValue();
        obj.append(std::move(key), std::move(val));
        skipWs();
      }
    }
    expect('}');

    return obj;
  }

  model::Array parseArray() {
    model::Array arr;
    expect('[');
    skipWs();
    if (!check(']')) {
      arr.append(parseValue());
      skipWs();
      while (check(',')) {
        ++it;
        skipWs();
        arr.append(parseValue());
        skipWs();
      }
    }
    expect(']');
    return arr;
  }

  model::PValue parseValue() {
    if (it == end) throw ParserError("expected value");

    switch (*it) {
    case '{':
      return model::PValue(new model::Object(parseObject()));
    case '"':
      return model::PValue(new model::Scalar(parseString()));
    case '[':
      return model::PValue(new model::Array(parseArray()));
    case 'n':
      return parseNull();
    case 't':
    case 'f':
      return parseBool();
    default:
      if ((*it >= '0' && *it <= '9') || *it == '-') return parseNumber();
    }
    throw ParserError("expected value");
  }
};

} // namespace cheesebase
