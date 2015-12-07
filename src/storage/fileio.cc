// Licensed under the Apache License 2.0 (see LICENSE file).

#include "fileio.h"
#include <sstream>

namespace cheesebase {

AsyncReq::AsyncReq(std::unique_ptr<AsyncStruct> op, Handle handle,
                   size_t expected)
    : m_async_struct(move(op)), m_handle(handle), m_expected(expected) {}

AsyncReq::~AsyncReq() {
  if (m_async_struct) wait();
}

#ifdef _WIN32
///////////////////////////////////////////////////////////////////////////////
// Windows implementation

namespace {

void fillOverlapped(OVERLAPPED* o, const uint64_t offset) {
  memset(o, 0, sizeof(*o));
  o->Offset = static_cast<uint32_t>(offset);
  o->OffsetHigh = static_cast<uint32_t>(offset >> 32);
  o->hEvent = ::CreateEvent(NULL, TRUE, FALSE, NULL);
}

void writeFile(HANDLE handle, gsl::span<const Byte> buffer, OVERLAPPED* o) {
  if (!::WriteFile(handle, buffer.data(),
                   gsl::narrow<DWORD>(buffer.size_bytes()), NULL, o)) {
    auto err = ::GetLastError();
    if (err != ERROR_IO_PENDING) {
      auto offs = static_cast<uint64_t>(o->Offset) +
                  (static_cast<uint64_t>(o->OffsetHigh) << 32);
      std::stringstream ss;
      ss << "WriteFile() failed for size " << buffer.size_bytes()
         << " at offset " << offs << " with 0x" << std::hex << err;
      throw FileError(ss.str());
    }
  }
}

void readFile(HANDLE handle, gsl::span<Byte> buffer, OVERLAPPED* o) {
  if (!::ReadFile(handle, buffer.data(),
                  gsl::narrow<DWORD>(buffer.size_bytes()), NULL, o)) {
    auto err = ::GetLastError();
    if (err != ERROR_IO_PENDING) {
      auto offs = static_cast<uint64_t>(o->Offset) +
                  (static_cast<uint64_t>(o->OffsetHigh) << 32);
      std::stringstream ss;
      ss << "ReadFile() failed for size " << buffer.size_bytes()
         << " at offset " << offs << " with 0x" << std::hex << err;
      throw FileError(ss.str());
    }
  }
}

void waitOverlapped(HANDLE handle, OVERLAPPED* o, const uint64_t expected) {
  DWORD bytes;
  if (!::GetOverlappedResult(handle, o, &bytes, TRUE)) {
    auto err = ::GetLastError();
    auto offs = static_cast<uint64_t>(o->Offset) +
                (static_cast<uint64_t>(o->OffsetHigh) << 32);
    std::stringstream ss;
    ss << "GetOverlappedResult() failed for size " << expected << " at offset "
       << offs << " with 0x" << std::hex << err;
    throw FileError(ss.str());
  }
  if (bytes != expected) {
    std::stringstream ss;
    ss << "FileIO request transferred " << bytes
       << " bytes while trying to transfer " << expected << " bytes";
    throw FileError(ss.str());
  }
}
uint64_t getSize(HANDLE handle) {
  LARGE_INTEGER size;
  if (!::GetFileSizeEx(handle, &size)) {
    auto err = ::GetLastError();
    std::stringstream ss;
    ss << "GetFileSizeEx() failed for with 0x" << std::hex << err;
    throw FileError(ss.str());
  }
  return size.QuadPart;
}

} // anonymous namespace

FileIO::FileIO(const std::string& filename, OpenMode mode, bool direct) {
  DWORD open_arg;
  switch (mode) {
  case OpenMode::create_new:
    open_arg = CREATE_NEW;
    break;
  case OpenMode::create_always:
    open_arg = CREATE_ALWAYS;
    break;
  case OpenMode::open_existing:
    open_arg = OPEN_EXISTING;
    break;
  case OpenMode::open_always:
    open_arg = OPEN_ALWAYS;
    break;
  default:
    throw BadArgument();
  }

  m_file_handle = ::CreateFileA(
      filename.c_str(),                   // filename
      GENERIC_WRITE | GENERIC_READ,       // desired access
      FILE_SHARE_WRITE | FILE_SHARE_READ, // share mode
      NULL,                               // security
      open_arg,                           // create or open
      FILE_FLAG_OVERLAPPED                // attributes
          | (direct ? FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH : NULL),
      NULL); // template

  if (m_file_handle == INVALID_HANDLE_VALUE) {
    auto err = ::GetLastError();
    std::stringstream ss;
    ss << "CreateFile() failed for " << filename << " with 0x" << std::hex
       << err;
    throw FileError(ss.str());
  }
}

FileIO::~FileIO() { ::CloseHandle(m_file_handle); }

void AsyncReq::wait() {
  if (m_async_struct) {
    waitOverlapped(m_handle, m_async_struct.get(), m_expected);
    ::CloseHandle(m_async_struct->hEvent);
    m_async_struct.reset();
  }
}

void FileIO::read(uint64_t offset, gsl::span<Byte> buffer) const {
  OVERLAPPED o;
  fillOverlapped(&o, offset);
  readFile(m_file_handle, buffer, &o);
  waitOverlapped(m_file_handle, &o, buffer.size_bytes());
}

void FileIO::write(uint64_t offset, gsl::span<const Byte> buffer) {
  OVERLAPPED o;
  fillOverlapped(&o, offset);
  writeFile(m_file_handle, buffer, &o);
  waitOverlapped(m_file_handle, &o, buffer.size_bytes());
}

void FileIO::resize(uint64_t size) {
  LARGE_INTEGER li;
  li.QuadPart = size;
  if (!::SetFilePointerEx(m_file_handle, li, NULL, FILE_BEGIN)) {
    auto err = ::GetLastError();
    std::stringstream ss;
    ss << "SetFilePointer() failed for size " << size << " with 0x" << std::hex
       << err;
    throw FileError(ss.str());
  }
  if (!::SetEndOfFile(m_file_handle)) {
    auto err = ::GetLastError();
    std::stringstream ss;
    ss << "SetEndOfFile() failed for position " << size << " with 0x"
       << std::hex << err;
    throw FileError(ss.str());
  }
}

AsyncReq FileIO::readAsync(uint64_t offset, gsl::span<Byte> buffer) const {
  auto o = std::make_unique<OVERLAPPED>();
  fillOverlapped(o.get(), offset);
  readFile(m_file_handle, buffer, o.get());
  return AsyncReq{ std::move(o), m_file_handle,
                   gsl::narrow_cast<size_t>(buffer.size_bytes()) };
}

AsyncReq FileIO::writeAsync(uint64_t offset, gsl::span<const Byte> buffer) {
  auto o = std::make_unique<OVERLAPPED>();
  fillOverlapped(o.get(), offset);
  writeFile(m_file_handle, buffer, o.get());
  return AsyncReq{ std::move(o), m_file_handle,
                   gsl::narrow_cast<size_t>(buffer.size_bytes()) };
}

uint64_t FileIO::size() const { return getSize(m_file_handle); }

#else
///////////////////////////////////////////////////////////////////////////////
// POSIX implementation

// TODO: POSIX impl

#endif
///////////////////////////////////////////////////////////////////////////////

} // namespace cheesebase
