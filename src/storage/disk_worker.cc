// Licensed under the Apache License 2.0 (see LICENSE file).

#include "disk_worker.h"

namespace cheesebase {

DiskWorker::DiskWorker(const std::string& filename, OpenMode mode)
    : m_fileio(filename, mode, true)
    , m_async(std::async(std::launch::async, [this]() { loop(); })) {}

DiskWorker::~DiskWorker() {
  m_run = false;
  m_queue_notify.notify_all();
}

void DiskWorker::loop() {
  ExLock<Mutex> lck{m_queue_mtx};

  while (m_run) {
    AsyncReq last{};
    Cond* cond{nullptr};

    while (m_queue.size() > 0) {
      auto req = m_queue.dequeue();
      lck.unlock();

      if (req->which() == 0) {
        auto& wreq = boost::get<DiskReq<gsl::span<const byte>>>(*req);
        last = m_fileio.write_async(wreq.offset, wreq.buffer);
        if (cond) cond->notify_all();
        cond = wreq.cb;
      } else {
        auto& rreq = boost::get<DiskReq<gsl::span<byte>>>(*req);
        last = m_fileio.read_async(rreq.offset, rreq.buffer);
        if (cond) cond->notify_all();
        cond = rreq.cb;
      }

      lck.lock();
    }

    last.wait();
    if (cond) cond->notify_all();

    m_queue_notify.wait(lck);
  }
}

void DiskWorker::write(gsl::span<const byte> buffer, Addr disk_addr) {
  ExLock<Mutex> lck{m_queue_mtx};
  Cond cb;
  m_queue.enqueue(disk_addr, std::make_unique<DiskReqVar>(
                                 DiskWriteReq({disk_addr, buffer, &cb})));
  m_queue_notify.notify_all();
  cb.wait(lck);
}

void DiskWorker::read(gsl::span<byte> buffer, Addr disk_addr) {
  if (disk_addr >= m_fileio.size()) return;

  ExLock<Mutex> lck{m_queue_mtx};
  Cond cb;
  m_queue.enqueue(disk_addr, std::make_unique<DiskReqVar>(
                                 DiskReadReq({disk_addr, buffer, &cb})));
  m_queue_notify.notify_all();
  cb.wait(lck);
}

} // namespace cheesebase
