// Licensed under the Apache License 2.0 (see LICENSE file).

#include "string.h"

#include "../core.h"

namespace cheesebase {
namespace disk {

namespace {

CB_PACKED(struct DskStringHdr {
  static constexpr uint64_t kMagic{ (static_cast<uint64_t>('S') << 48) +
                                    (static_cast<uint64_t>('T') << 56) };
  DskStringHdr() = default;
  DskStringHdr(size_t s) {
    data_ = static_cast<uint64_t>(s);
    Expects(data_ < lowerBitmask(48));
    data_ += kMagic;
  }

  size_t size() const {
    check();
    return data_ & lowerBitmask(48);
  }

  void check() const {
    if ((data_ & upperBitmask(16)) != kMagic) {
      throw ConsistencyError();
    }
  }

  uint64_t data_;
});

using StrNext = DskNext<'S'>;

const size_t kFirstDataSize =
    k_page_size - (sizeof(StrNext) + sizeof(DskStringHdr));
const size_t kOtherDataSize = k_page_size - sizeof(StrNext);

} // anonymous namespace

StringW::StringW(Transaction& ta, Addr addr) : ValueW(ta, addr) {}

StringW::StringW(Transaction& ta, model::String str)
    : ValueW(ta), str_{ std::move(str) } {

  auto size = str_.size();
  auto to_write = std::min(size, kFirstDataSize);

  blocks_.emplace_back(
      ta_.alloc(to_write + sizeof(StrNext) + sizeof(DskStringHdr)));
  addr_ = blocks_.back().addr;
  size -= to_write;

  while (size > 0) {
    to_write = std::min(size, kOtherDataSize);
    blocks_.emplace_back(ta_.alloc(to_write + sizeof(StrNext)));
    size -= to_write;
  }
}

Writes StringW::getWrites() const {
  Expects(blocks_.size() >= 1);

  Writes ret;
  ret.reserve(blocks_.size() * 2 + 1);

  // Header with string size
  ret.push_back(
      { Addr(addr_.value + sizeof(StrNext)), DskStringHdr(str_.size()).data_ });
  // Reference to next block if multi block string
  ret.push_back(
      { addr_,
        StrNext(blocks_.size() >= 2 ? blocks_[1].addr : Addr(0)).data() });

  auto span = gsl::as_bytes(Span<const char>(str_));
  auto block_it = blocks_.begin();

  // Write first block
  auto to_write = std::min(static_cast<size_t>(span.size()), kFirstDataSize);
  Expects(to_write < block_it->size);
  ret.push_back(
      { Addr(block_it->addr.value + sizeof(StrNext) + sizeof(DskStringHdr)),
        span.subspan(0, to_write) });
  span = span.subspan(to_write);

  // Write all the other blocks
  while (span.size() > 0) {
    block_it++;
    Expects(block_it != blocks_.end());

    ret.push_back({ block_it->addr, StrNext(std::next(block_it) != blocks_.end()
                                                ? std::next(block_it)->addr
                                                : Addr(0))
                                        .data() });
    to_write = std::min(static_cast<size_t>(span.size()), kOtherDataSize);
    Expects(to_write < block_it->size);
    ret.push_back({ Addr(block_it->addr.value + sizeof(StrNext)),
                    span.subspan(0, to_write) });
    span = span.subspan(to_write);
  }
  Ensures(span.size() == 0);

  return ret;
}

void StringW::destroy() {
  if (!blocks_.empty()) {
    // String was just created and is not yet on disk.

    for (const auto& b : blocks_) {
      ta_.free(b.addr, b.size);
    }
    blocks_.clear();
    addr_ = Addr(0);
    return;
  }
  Expects(!addr_.isNull());

  auto ref = ta_.loadBlock<ssizeof<StrNext>() + ssizeof<DskStringHdr>()>(addr_);
  auto next = getFromSpan<StrNext>(*ref).next();
  auto size =
      getFromSpan<DskStringHdr>(ref->subspan(ssizeof<StrNext>())).size();
  ref.free();

  auto size_here = std::min(size, kFirstDataSize);
  ta_.free(addr_, size_here + sizeof(DskStringHdr) + sizeof(StrNext));
  size -= size_here;

  while (!next.isNull()) {
    if (size <= 0) throw ConsistencyError();

    auto block = ta_.loadBlock<ssizeof<StrNext>()>(next);
    auto new_next = getFromSpan<StrNext>(*block).next();

    size_here = std::min(size, kOtherDataSize);
    ta_.free(next, size_here + sizeof(StrNext));

    size -= size_here;
    next = new_next;
  }
  if (size != 0) throw ConsistencyError();
}

////////////////////////////////////////////////////////////////////////////////
// StringR

StringR::StringR(Database& db, Addr addr) : ValueR(db, addr) {}

model::Value StringR::getValue() {
  auto page = db_.loadPage(addr_.pageNr());
  auto span = page->subspan(addr_.pageOffset());

  auto next = getFromSpan<StrNext>(span).next();
  span = span.subspan(ssizeof<StrNext>());
  auto size = getFromSpan<DskStringHdr>(span).size();
  span = span.subspan(ssizeof<DskStringHdr>());

  std::string str;
  str.reserve(size);

  auto size_here = std::min(size, kFirstDataSize);
  auto char_span = gsl::as_span<const char>(span.subspan(0, size_here));
  str.append(char_span.begin(), char_span.end());
  page.free();
  size -= size_here;

  while (!next.isNull()) {
    if (size <= 0) throw ConsistencyError();

    page = db_.loadPage(next.pageNr());
    span = page->subspan(next.pageOffset());

    auto new_next = getFromSpan<StrNext>(span).next();
    span = span.subspan(ssizeof<StrNext>());

    size_here = std::min(size, kOtherDataSize);
    char_span = gsl::as_span<const char>(span.subspan(0, size_here));
    str.append(char_span.begin(), char_span.end());
    size -= size_here;

    next = new_next;
  }
  if (size != 0) throw ConsistencyError();

  return std::move(str); // constructs Value
}

} // namespace disk
} // namespace cheeseabse
