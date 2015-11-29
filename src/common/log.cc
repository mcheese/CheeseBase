// Licensed under the Apache License 2.0 (see LICENSE file).

#include "log.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <ctime>

namespace cheesebase {

using namespace std;

static const auto log_file{"cheesebase.log"};

bool Log::bad{false};
Log::LogLevel Log::notify_level{Log::LogLevel::info};
Log::LogLevel Log::write_level{Log::LogLevel::warn};
Log::LogLevel Log::log_level{std::max(notify_level, write_level)};

std::ofstream Log::open_log_file() {
  if (!bad) {
    ofstream fs = std::ofstream(log_file, std::ios_base::app);

    if (!fs.good()) {
      cerr << "Failed to open \"" << log_file << "\", not logging to file."
           << endl;
      bad = true;
    }
    return fs;
  }
  return ofstream();
}

std::ofstream Log::file_stream = Log::open_log_file();

Log::Log(LogLevel level) : message_level(level) { time(&now); }

Log::~Log() {
  static const char* const levels[] = {
      "ERROR", "WARN ", "INFO ",
  };

  if (message_level <= notify_level) {
    cerr << levels[message_level] << " | " << stream.str() << endl;
  }

  if (message_level <= write_level && !bad) {
    static const size_t time_length = sizeof("YYYY-MM-DD HH:MM:SS ");
    char time_buf[time_length];
    struct tm timeinfo;
    localtime_s(&timeinfo, &now);
    strftime(time_buf, time_length, "%Y-%m-%d %H:%M:%S", &timeinfo);

    if (!file_stream.good()) file_stream = open_log_file();

    file_stream << time_buf << " | " << levels[message_level] << " | "
                << stream.str() << endl;
  }
}

std::ostringstream& Log::get() { return stream; }

void Log::set_notify_level(LogLevel level) {
  notify_level = level;
  log_level = std::max(notify_level, write_level);
}

void Log::set_write_level(LogLevel level) {
  write_level = level;
  log_level = std::max(notify_level, write_level);
}

Log::LogLevel Log::get_log_level() { return log_level; }

} // namespace cheesebase
