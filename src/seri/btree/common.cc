// Licensed under the Apache License 2.0 (see LICENSE file).
#include "common.h"

#include "leaf.h"
#include "internal.h"
#include "../../core.h"

namespace cheesebase {
namespace disk {
namespace btree {

bool isNodeLeaf(const ReadRef<kBlockSize>& block) {
  // first byte of Addr is always 0
  // next-ptr of leafs put a flag in the first byte, marking the node as leaf
  return gsl::as_span<const DskLeafHdr>(block->subspan(
      ssizeof<DskBlockHdr>(), sizeof(DskLeafHdr)))[0].hasMagic();
}

std::unique_ptr<NodeW> openNodeW(Transaction& ta, Addr addr) {
  auto block = ta.loadBlock<kBlockSize>(addr);

  if (isNodeLeaf(block))
    return std::make_unique<LeafW>(ta, addr);
  else
    return std::make_unique<InternalW>(ta, addr);
}

std::unique_ptr<NodeW> openRootW(Transaction& ta, Addr addr,
                                 BtreeWritable& tree) {
  auto block = ta.loadBlock<kBlockSize>(addr);

  if (isNodeLeaf(block))
    return std::make_unique<RootLeafW>(ta, addr, tree);
  else
    return std::make_unique<RootInternalW>(ta, addr, tree);
}

} // namespace btree
} // namespace disk
} // namespace cheesebase
