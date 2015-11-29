// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include <sstream>
#include <fstream>

#define LOG_ERROR                                                              \
  \
if(::cheesebase::Log::LogLevel::error > ::cheesebase::Log::get_log_level());   \
  \
else::cheesebase::Log(::cheesebase::Log::LogLevel::error)                      \
      .get()

#define LOG_WARN                                                               \
  \
if(::cheesebase::Log::LogLevel::warn > ::cheesebase::Log::get_log_level());    \
  \
else::cheesebase::Log(::cheesebase::Log::LogLevel::warn)                       \
      .get()

#define LOG_INFO                                                               \
  \
if(::cheesebase::Log::LogLevel::info > ::cheesebase::Log::get_log_level());    \
  \
else::cheesebase::Log(::cheesebase::Log::LogLevel::info)                       \
      .get()

namespace cheesebase {

class Log {
public:
  enum LogLevel { error, warn, info };

  Log(LogLevel level);
  ~Log();

  std::ostringstream& get();

  static void set_notify_level(LogLevel level);
  static void set_write_level(LogLevel level);
  static LogLevel get_log_level();

private:
  std::ostringstream stream;
  LogLevel message_level;
  time_t now;
  static bool bad;
  static LogLevel notify_level;
  static LogLevel write_level;
  static LogLevel log_level;
  static std::ofstream file_stream;
  static std::ofstream open_log_file();
};

} // namespace cheesebase
