// Licensed under the Apache License 2.0 (see LICENSE file).

#pragma once

#include "common/sync.h"
#include "fileio.h"
#include "inc_prio_queue.h"

#include <boost/variant.hpp>
#include <condition_variable>
#include <future>

namespace cheesebase {

struct CachePage;

class DiskWorker {
public:
  DEF_EXCEPTION(invalid_request);

  DiskWorker(const std::string& filename, OpenMode mode);
  ~DiskWorker();

  void write(gsl::not_null<CachePage*> page);
  void write(const std::vector<gsl::not_null<CachePage*>>& pages);
  void read(gsl::not_null<CachePage*> page);

private:
  enum class ReqType { none, write, read };
  struct Req {
    CachePage* page;
    ReqType type;
    int* done;
  };

  // core loop processing disk requests
  void loop();

  FileIO m_fileio;
  bool m_run = true;
  std::future<void> m_async;
  Mutex m_queue_mtx;
  Cond m_queue_notify;
  Cond m_task_notify;
  IncPrioQueue<Addr, Req> m_queue;
};

} // namespace cheesebase
