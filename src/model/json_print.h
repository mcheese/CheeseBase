#pragma once

#include "model.h"
#include <ostream>

namespace cheesebase {
namespace model {

class JsonPrinter {
public:
  JsonPrinter(std::ostream& os) : os_{ os } {}
  ~JsonPrinter() { os_ << '\n'; }

  void operator()(const model::Value& q, size_t indent = 0) {
    boost::apply_visitor(([indent, this](auto& ctx) { (*this)(ctx, indent); }),
                         q);
  }

  void operator()(const model::Missing&, size_t = 0) { os_ << "missing"; }
  void operator()(const model::Null&, size_t = 0) { os_ << "null"; }
  void operator()(const model::Number& q, size_t = 0) { os_ << q; }
  void operator()(const model::Bool& q, size_t = 0) { os_ << (q ? "true" : "false"); }
  void operator()(const model::String& q, size_t = 0) { os_ << '"' + q + '"'; }
  void operator()(const model::Shared<model::Tuple>& q, size_t indent = 0) {
    if (!q->empty()) {
      indent++;

      os_ << "{\n";

      auto it = std::begin(*q);

      os_ << std::string(2 * indent, ' ');
      (*this)(it->first, indent);
      os_ << ": ";
      (*this)(it->second, indent);

      for (++it; it != std::end(*q); ++it) {
        os_ << ",\n" << std::string(2 * indent, ' ');
        (*this)(it->first, indent);
        os_ << ": ";
        (*this)(it->second, indent);
      }

      os_ << '\n' << std::string(2 * (indent - 1), ' ') << '}';
    } else {
      os_ << "{}";
    }
  }
  void operator()(const model::Shared<model::Collection>& q, size_t indent = 0) {
    if (!q->empty()) {
      indent++;

      os_ << "[\n";

      auto it = std::begin(*q);

      os_ << std::string(2 * indent, ' ');
      (*this)(*it, indent);

      for (++it; it != std::end(*q); ++it) {
        os_ << ",\n" << std::string(2 * indent, ' ');
        (*this)(*it, indent);
      }

      os_ << '\n' << std::string(2 * (indent - 1), ' ') << ']';
    } else {
      os_ << "[]";
    }
  }

private:
  std::ostream& os_;
};

}
}
