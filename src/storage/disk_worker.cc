// Licensed under the Apache License 2.0 (see LICENSE file).

#include "disk_worker.h"
#include "cache.h"

namespace cheesebase {

DiskWorker::DiskWorker(const std::string& filename, OpenMode mode)
    : m_fileio(filename, mode, true)
    , m_async(std::async(std::launch::async, [this]() { loop(); })) {}

DiskWorker::~DiskWorker() {
  m_run = false;
  m_queue_notify.notify_all();
}

void DiskWorker::loop() {
  ExLock<Mutex> lck{ m_queue_mtx };

  while (m_run) {
    AsyncReq last{};
    Req last_req{};
    last_req.type = ReqType::none;

    while (m_queue.size() > 0) {
      auto req = m_queue.dequeue();
      lck.unlock();

      if (req.type == ReqType::write) {
        last =
            m_fileio.write_async(page_addr(req.page->page_nr), req.page->data);
      } else if (req.type == ReqType::read) {
        last =
            m_fileio.read_async(page_addr(req.page->page_nr), req.page->data);
      }

      lck.lock();

      if (last_req.type != ReqType::none) {
        if (last_req.type == ReqType::write) last_req.page->changed = false;
        if (last_req.done) *last_req.done = 1;
        m_task_notify.notify_all();
      }

      last_req = req;
    }

    last.wait();
    if (last_req.type != ReqType::none) {
      if (last_req.type == ReqType::write) last_req.page->changed = false;
      if (last_req.done) *last_req.done = 1;
      m_task_notify.notify_all();
    }

    m_queue_notify.wait(lck);
  }
}

void DiskWorker::write(gsl::not_null<CachePage*> page) {
  ExLock<Mutex> lck{ m_queue_mtx };
  int done = 0;
  m_queue.enqueue(page_addr(page->page_nr), { page, ReqType::write, &done });
  m_queue_notify.notify_all();
  while (!done) m_task_notify.wait(lck);
}

void DiskWorker::write(const std::vector<gsl::not_null<CachePage*>>& pages) {
  ExLock<Mutex> lck{ m_queue_mtx };
  std::vector<int> done(pages.size(), 0);
  size_t i = 0;
  for (auto p : pages) {
    Expects(i < done.size());
    m_queue.enqueue(page_addr(p->page_nr),
                    { p, ReqType::write, &(done.data()[i++]) });
  }
  m_queue_notify.notify_all();
  for (auto& d : done) {
    while (!d) m_task_notify.wait(lck);
  }
}

void DiskWorker::read(gsl::not_null<CachePage*> page) {
  if (page_addr(page->page_nr) >= m_fileio.size()) return;

  ExLock<Mutex> lck{ m_queue_mtx };
  int done = 0;
  m_queue.enqueue(page_addr(page->page_nr), { page, ReqType::read, &done });
  m_queue_notify.notify_all();
  while (!done) m_task_notify.wait(lck);
}

} // namespace cheesebase
