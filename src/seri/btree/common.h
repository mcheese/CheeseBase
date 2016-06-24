// Licensed under the Apache License 2.0 (see LICENSE file).
#pragma once

#include "btree.h"
#include "../../common.h"
#include "../../cache.h"
#include "../../allocator.h"

namespace cheesebase {
namespace disk {
namespace btree {

constexpr std::ptrdiff_t kBlockSize = 256;
constexpr std::ptrdiff_t kNodeSize = kBlockSize - sizeof(DskBlockHdr);

CB_PACKED(struct DskKey {
  DskKey() = default;
  explicit DskKey(Key key)
    : big_{ static_cast<uint32_t>(key.value) }
    , small_{ static_cast<uint16_t>(key.value >> 32) } {
    Expects(key.value < (static_cast<uint64_t>(1) << 48));
  }

  Key key() const noexcept {
    return Key(static_cast<uint64_t>(big_) +
      (static_cast<uint64_t>(small_) << 32));
  }

private:
  uint32_t big_;
  uint16_t small_;
});
static_assert(sizeof(DskKey) == 6, "Invalid disk key size");


// Dummy type used as argument
enum class AllocateNew {};

class AbsInternalW;

class NodeW {
public:
  virtual ~NodeW() = default;
  NodeW(Addr addr) : addr_(addr) {}

  Addr addr() const { return addr_; }
  virtual Writes getWrites() const = 0;

  // inserts value, returns true on success
  virtual bool insert(Key key, const model::Value&, Overwrite,
                      AbsInternalW* parent) = 0;

  // inserts value at maximum existing key + 1 and returns this key
  virtual Key append(const model::Value& val, AbsInternalW* parent) = 0;

  // deallocate node and all its children
  virtual void destroy() = 0;

  // delete value, returns true if found and removed
  virtual bool remove(Key key, AbsInternalW* parent) = 0;

protected:
  Addr addr_;
};

bool isNodeLeaf(const ReadRef<kBlockSize>& block);
std::unique_ptr<NodeW> openNodeW(Transaction& ta, Addr addr);
std::unique_ptr<NodeW> openRootW(Transaction& ta, Addr addr,
                                 BtreeWritable& parent);

template <class C, typename K>
void tryTransfer(C& from, C& to, const K& key) {
  auto lookup = from.find(key);
  if (lookup != from.end()) {
    to.insert(std::move(*lookup));
    from.erase(lookup);
  }
}

} // namespace btree
} // namespace disk
} // namespace cheesebase
