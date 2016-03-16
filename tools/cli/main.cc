// Licensed under the Apache License 2.0 (see LICENSE file).

#include <cheesebase.h>
#include <parser.h>
#include <iostream>
#include <algorithm>
#include <cwctype>
#include <cctype>

template <class It>
uint64_t parseIndex(It& it, It& end) {
  std::string str;
  while (it != end && std::isdigit(*it)) { str.push_back(*(it++)); }
  uint64_t index = std::strtoull(str.c_str(), nullptr, 10);
  return index;
}

template <class It>
std::string parseKey(It& it, It& end) {
  std::string str;
  while (it != end && std::isalnum(*it)) { str.push_back(*(it++)); }
  return str;
}

template <class It>
cheesebase::Query parseLocation(cheesebase::CheeseBase& cb, It& it, It& end) {
  if (it == end) throw std::runtime_error("invalid path");

  if (it != end && std::iswspace(*it)) it++;
  if (it == end) throw std::runtime_error("invalid path");
  auto loc = cb[parseKey(it, end)];

  for (;;) {
    if (it == end || std::iswspace(*it)) {
      // end of path
      return loc;

    } else if (*it == '.') {
      // key next
      it++;
      loc = loc[parseKey(it, end)];

    } else if (*it == '[') {
      // arry index next
      it++;
      loc = loc [parseIndex(it, end)];
      if (it == end || *it != ']') throw std::runtime_error("invalid path");
      it++;

    } else {
      // no valid path anymore
      throw std::runtime_error("invalid path");
    }
  }
}

void inputLoop(cheesebase::CheeseBase& cb) {
  std::string command;
  std::string where;
  while (std::cin) {
    std::cout << "\n> " << std::flush;
    std::cin >> command;
    std::transform(command.begin(), command.end(), command.begin(), ::tolower);

    if (command == ":q" || command == "q" || command == "quit" ||
        command == "exit") {
      return;
    }

    std::istreambuf_iterator<char> it{ std::cin };
    std::istreambuf_iterator<char> end{};

    try {
      if (command == "insert") {
        parseLocation(cb, it, end).insert(*cheesebase::parseJson(&it, end));
      } else if (command == "update") {
        parseLocation(cb, it, end).update(*cheesebase::parseJson(&it, end));
      } else if (command == "upsert") {
        parseLocation(cb, it, end).upsert(*cheesebase::parseJson(&it, end));
      } else if (command == "getall") {
        cb.get({})->prettyPrint(std::cout);
      } else if (command == "get") {
        parseLocation(cb, it, end).get()->prettyPrint(std::cout);
      } else if (command == "remove") {
        parseLocation(cb, it, end).remove();
      } else if (command == "append") {
        parseLocation(cb, it, end).append(*cheesebase::parseJson(&it, end));
      } else {
        std::cerr << "Unknown command";
      }

    } catch (std::exception& e) {
      std::cerr << "Error: " << e.what() << "\n";
    }

    while (it != end && *it != ';' && *it != '\n') it++;
  }
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout << "Usage: " << (argc > 0 ? argv[0] : "cheesebase-cli")
              << " <db-file>\n";
    return 1;
  }
  try {
    cheesebase::CheeseBase cb{ argv[1] };
    inputLoop(cb);
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << "\n";
    return 1;
  }
  return 0;
}

