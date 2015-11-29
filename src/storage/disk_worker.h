// Licensed under the Apache License 2.0 (see LICENSE file).

#include "fileio.h"
#include "inc_prio_queue.h"
#include "common/sync.h"

#include <boost/variant.hpp>
#include <condition_variable>
#include <future>

namespace cheesebase {

class DiskWorker {
public:
  DEF_EXCEPTION(invalid_request);

  DiskWorker(const std::string& filename, OpenMode mode);
  ~DiskWorker();

  void write(gsl::span<const byte> buffer, Addr disk_addr);
  void read(gsl::span<byte> buffer, Addr disk_addr);

private:
  template <typename View>
  struct DiskReq {
    Addr offset;
    View buffer;
    Cond* cb;
  };
  using DiskWriteReq = DiskReq<gsl::span<const byte>>;
  using DiskReadReq = DiskReq<gsl::span<byte>>;
  using DiskReqVar = boost::variant<DiskWriteReq, DiskReadReq>;

  // core loop processing disk requests
  void loop();

  FileIO m_fileio;
  bool m_run = true;
  std::future<void> m_async;
  Mutex m_queue_mtx;
  Cond m_queue_notify;
  IncPrioQueue<Addr, std::unique_ptr<DiskReqVar>> m_queue;
};

} // namespace cheesebase
