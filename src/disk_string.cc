// Licensed under the Apache License 2.0 (see LICENSE file).

#include "disk_string.h"

#include "core.h"
#include "structs.h"

namespace cheesebase {
namespace disk {

const size_t kBlockMaxSize = k_page_size - sizeof(DskBlockHdr);

namespace {

static const uint64_t kMagic{ (static_cast<uint64_t>('S') << 48) +
                              (static_cast<uint64_t>('T') << 56) };
static const uint64_t kBitmap{ static_cast<uint64_t>(0xFFFF) << 48 };

CB_PACKED(struct DskStringHdr {
  DskStringHdr() = default;
  DskStringHdr(size_t s) {
    data_ = static_cast<uint64_t>(s);
    Expects((data_ & kBitmap) == 0);
    data_ += kMagic;
  }

  size_t size() const {
    check();
    return data_ & ~kMagic;
  }

  void check() const {
    if ((data_ & kBitmap) != kMagic) {
      throw ConsistencyError("Expected string magic bytes");
    }
  }

  uint64_t data_;
});

} // anonymous namespace

StringW::StringW(Transaction& ta, Addr addr) : ValueW(ta, addr) {
  auto hdr = gsl::as_span<DskStringHdr>(
      ta_.load(addr.pageNr())
          ->subspan(addr.pageOffset() + ssizeof<DskBlockHdr>()))[0];
  hdr.check();
  hdr_ = hdr.data_;
}

StringW::StringW(Transaction& ta, model::String str)
    : ValueW(ta), str_{ std::move(str) } {
  hdr_ = DskStringHdr(str_.size()).data_;
  if (str_.size() + sizeof(DskStringHdr) < kBlockMaxSize) {
    blocks_.emplace_back(ta_.alloc(str_.size() + sizeof(DskStringHdr)));
    addr_ = blocks_.back().addr;
    Expects(blocks_.back().size >=
            str_.size() + sizeof(DskStringHdr) + sizeof(DskBlockHdr))
  } else {
    blocks_.emplace_back(ta_.alloc(kBlockMaxSize));
    addr_ = blocks_.back().addr;
    int size =
        static_cast<int>(str_.size()) - kBlockMaxSize + sizeof(DskStringHdr);
    while (size > 0) {
      blocks_.emplace_back(ta_.allocExtension(
          blocks_.back().addr,
          std::min(kBlockMaxSize, static_cast<size_t>(size))));
      size -= gsl::narrow_cast<int>(blocks_.back().size);
    }
  }
}

Writes StringW::getWrites() const {
  Writes ret;
  ret.push_back({ Addr(addr_.value + sizeof(DskBlockHdr)),
                  gsl::as_bytes(Span<const uint64_t>(&hdr_, ssizeof(hdr_))) });
  auto span = gsl::as_bytes(Span<const char>(str_));
  auto hdr_size = sizeof(DskBlockHdr) + sizeof(DskStringHdr);
  for (auto& blk : blocks_) {
    auto sz = std::min(blk.size - hdr_size, static_cast<size_t>(span.size()));
    ret.push_back({ Addr(blk.addr.value + hdr_size), { span.subspan(0, sz) } });
    span = span.subspan(sz);
    hdr_size = sizeof(DskBlockHdr);
  }
  return ret;
}

void StringW::destroy() { ta_.free(addr_); }

////////////////////////////////////////////////////////////////////////////////
// StringR

StringR::StringR(Database& db, Addr addr) : ValueR(db, addr) {}

model::PValue StringR::getValue() {
  auto addr = addr_;
  auto page = db_.loadPage(addr.pageNr());
  DskBlockHdr hdr =
      gsl::as_span<DskBlockHdr>(page->subspan(addr.pageOffset()))[0];
  auto span = page->subspan(addr.pageOffset(), toBlockSize(hdr.type()));
  auto next = hdr.next();

  auto size = gsl::as_span<DskStringHdr>(
                  span.subspan(sizeof(DskBlockHdr), sizeof(DskStringHdr)))[0]
                  .size();
  std::string str;
  str.reserve(size);

  span = span.subspan(sizeof(DskBlockHdr) + sizeof(DskStringHdr));
  while (size > 0) {
    auto sz = std::min(static_cast<size_t>(span.size()), size);
    str.append(gsl::as_span<const char>(span).data(), sz);
    size -= sz;
    if (!next.isNull()) {
      if (addr.pageNr() != next.pageNr()) {
        page = db_.loadPage(next.pageNr());
      }
      addr = next;

      hdr = gsl::as_span<DskBlockHdr>(page->subspan(addr.pageOffset()))[0];
      span = page->subspan(addr.pageOffset() + sizeof(DskBlockHdr),
                           toBlockSize(hdr.type()) - sizeof(DskBlockHdr));
      next = hdr.next();
    } else {
      if (size != 0)
        throw ConsistencyError("String longer than allocated space");
    }
  }

  return std::make_unique<model::Scalar>(std::move(str));
}

} // namespace disk
} // namespace cheeseabse
