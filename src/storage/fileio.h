// Licensed under the Apache License 2.0 (see LICENSE file).

// Queue read/write requests to the file that get executed by a single thread
// in optimal order.

#pragma once

#include "storage.h"

#ifdef _WIN32
#define NOMINMAX // min() and max() macros leak otherwise...
#include <Windows.h>
#undef NOMINMAX
using AsyncStruct = OVERLAPPED;
using Handle = HANDLE;
#else
// TODO: POSIX impl
#endif

namespace cheesebase {

class FileIO {
public:
  struct file_error : public std::exception { const char* what() const noexcept { return "file_error"; } };
  struct bad_argument : public std::exception { const char* what() const noexcept { return "bad_argument"; } };

  class AsyncReq {
  public:
    AsyncReq() = default;
    AsyncReq(std::unique_ptr<AsyncStruct>&& op, Handle handle,
             const uint64_t expected);
    ~AsyncReq();

    // Non-copyable, movable.
    AsyncReq(const AsyncReq&) = delete;
    AsyncReq& operator=(const AsyncReq&) = delete;
    AsyncReq(AsyncReq&& r) = default;
    AsyncReq& operator=(AsyncReq&& r) = default;

    void wait();

  private:
    Handle m_handle;
    uint64_t m_expected;
    std::unique_ptr<AsyncStruct> m_async_struct;
  };

  explicit FileIO(const std::string& filename, Storage::OpenMode mode);
  ~FileIO();
  
  // Read part of file starting at offset into buffer. The size of the buffer
  // array view is read.
  void read(const uint64_t offset, gsl::array_view<byte> buffer) const;
  
  // Write content of buffer into the file starting at offset.
  void write(const uint64_t offset, const gsl::array_view<byte> buffer);

  // Resize the file to size
  void resize(const uint64_t size);

  // Get file size
  uint64_t size() const;

  // Queue read request. Use returned object to wait() for completion.
  AsyncReq read_async(const uint64_t offset,
                      gsl::array_view<byte> buffer) const;

  // Queue write request. Use returned object to wait() for completion.
  AsyncReq write_async(const uint64_t offset,
                       const gsl::array_view<byte> buffer);

private:
  uint64_t m_size;
  Handle m_file_handle;
};

} // namespace cheesebase
