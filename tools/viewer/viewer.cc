#include "structs.h"
#include "common.h"
#include <fstream>
#include <iostream>
#include <map>
#include <set>

using namespace cheesebase;

DskBlockHdr get_hdr(Addr addr, std::ifstream& fs) {
  fs.seekg(addr.value, std::ios_base::beg);
  DskBlockHdr hdr;
  fs.read((char*)&hdr, sizeof(hdr));
  return hdr;
}
bool check_free(std::set<Addr>& free, Addr first, BlockType type, int level,
                std::ifstream& fs) {

  auto next = first;
  while (!next.isNull()) {
    if (next.value % (k_page_size / level) != 0) {
      std::cout << "Corrupted: invalid block in free list of PageAlloc!\n";
      return false;
    }
    auto hdr = get_hdr(next, fs);
    if (hdr.type() != type) {
      std::cout << "Corrupted: invalid block in free list of level " << level
                << " allocator.\n";
      return false;
    }
    free.insert(next);
    next = hdr.next();
  }
  return true;
}

int main(int argc, char* argv[]) {
  if (argc < 2) return 1;

  std::ifstream fs;
  fs.open(argv[1], std::fstream::in | std::fstream::binary);

  DskDatabaseHdr db_hdr;
  fs.read((char*)&db_hdr, sizeof(db_hdr));

  fs.seekg(0, std::ios_base::end);
  auto end = fs.tellg();
  if (db_hdr.end_of_file.value > static_cast<uint64_t>(end)) {
    std::cout << "Corrupted: file size < end of file marker!\n";
    return 1;
  }

  std::set<Addr> free;
  if (!check_free(free, db_hdr.free_alloc_pg, BlockType::pg, 1, fs)) return 1;
  if (!check_free(free, db_hdr.free_alloc_t1, BlockType::t1, 2, fs)) return 1;
  if (!check_free(free, db_hdr.free_alloc_t2, BlockType::t2, 4, fs)) return 1;
  if (!check_free(free, db_hdr.free_alloc_t3, BlockType::t3, 8, fs)) return 1;
  if (!check_free(free, db_hdr.free_alloc_t4, BlockType::t4, 16, fs)) return 1;

  size_t n = 1;

  for (n = 1; n < db_hdr.end_of_file.pageNr().value; ++n) {
    Addr offset = Addr(0);
    do {
      auto addr = Addr(n * k_page_size + offset.value);
      auto hdr = get_hdr(addr, fs);
      int len;
      switch (hdr.type()) {
      case BlockType::pg:
        offset.value += k_page_size;
        len = 64;
        break;
      case BlockType::t1:
        offset.value += k_page_size / 2;
        len = 32;
        break;
      case BlockType::t2:
        offset.value += k_page_size / 4;
        len = 16;
        break;
      case BlockType::t3:
        offset.value += k_page_size / 8;
        len = 8;
        break;
      case BlockType::t4:
        offset.value += k_page_size / 16;
        len = 4;
        break;
      default:
        offset.value = k_page_size + 1;
        len = 64;
      }
      char c =
          (offset.value > k_page_size ? '!' : (free.count(addr) > 0 ? ' ' : ':'));
      std::cout << "[" << std::string(len - 2, c).c_str() << "]";
    } while (offset.value < k_page_size);
    std::cout << "\n";
  }

  return 0;
}
