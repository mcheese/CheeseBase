#include "model/parser.h"
#include <chrono>
#include <iostream>

int main() {
  auto t1 = std::chrono::high_resolution_clock::now();
  std::istream_iterator<char> it(std::cin);
  std::istream_iterator<char> end;

  auto t2 = std::chrono::high_resolution_clock::now();
  auto doc = cheesebase::parseJson(it, end);
  auto t3 = std::chrono::high_resolution_clock::now();

  std::cout << "\n\n";
  //doc.prettyPrint(std::cout);

  std::cout << "  Reading: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1)
                   .count()
            << "ms\n"
            << "  Parsing: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2)
                   .count()
            << "ms\n";

  return 0;
}
