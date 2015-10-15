// Licensed under the Apache License 2.0 (see LICENSE file).

#include "fileio.h"
#include "common/log.h"

namespace cheesebase {

FileIO::AsyncReq::AsyncReq(std::unique_ptr<AsyncStruct>&& op,
                           Handle handle, const uint64_t expected)
  : m_async_struct(move(op))
  , m_handle(handle)
  , m_expected(expected)
{}

FileIO::AsyncReq::~AsyncReq()
{
  if (m_async_struct) wait();
}

#ifdef _WIN32
///////////////////////////////////////////////////////////////////////////////
// Windows implementation

namespace {

void fill_overlapped(OVERLAPPED* o, const uint64_t offset)
{
  memset(o, 0, sizeof(*o));
  o->Offset = static_cast<uint32_t>(offset);
  o->OffsetHigh = static_cast<uint32_t>(offset >> 32);
  o->hEvent = ::CreateEvent(NULL, TRUE, FALSE, NULL);
}

void write_file(HANDLE handle, const gsl::array_view<byte> buffer,
                OVERLAPPED* o)
{
  if (!::WriteFile(handle, buffer.data(), gsl::narrow<DWORD>(buffer.bytes()),
                   NULL, o)) {
    auto err = ::GetLastError();
    if (err != ERROR_IO_PENDING) {
      auto offs = static_cast<uint64_t>(o->Offset)
                  + (static_cast<uint64_t>(o->OffsetHigh) << 32);
      LOG_ERROR
        << "WriteFile() failed for size " << buffer.bytes()
        << " at offset " << offs << " with 0x" << std::hex << err;
      throw FileIO::file_error{};
    }
  }
}

void read_file(HANDLE handle, gsl::array_view<byte> buffer, OVERLAPPED* o)
{
  if (!::ReadFile(handle, buffer.data(), gsl::narrow<DWORD>(buffer.bytes()),
                  NULL, o)) {
    auto err = ::GetLastError();
    if (err != ERROR_IO_PENDING) {
      auto offs = static_cast<uint64_t>(o->Offset)
                  + (static_cast<uint64_t>(o->OffsetHigh) << 32);
      LOG_ERROR
        << "ReadFile() failed for size " << buffer.bytes()
        << " at offset " << offs << " with 0x" << std::hex << err;
      throw FileIO::file_error{};
    }
  }
}

void wait_overlapped(HANDLE handle, OVERLAPPED* o, const uint64_t expected)
{
   DWORD bytes;
   if (!::GetOverlappedResult(handle, o, &bytes, TRUE)) {
    auto err = ::GetLastError();
    auto offs = static_cast<uint64_t>(o->Offset)
                + (static_cast<uint64_t>(o->OffsetHigh) << 32);
    LOG_ERROR
      << "GetOverlappedResult() failed for size " << expected
      << " at offset " << offs << " with 0x" << std::hex << err;
    throw FileIO::file_error{};
  }
  if (bytes != expected) {
    LOG_ERROR
      << "FileIO request transferred " << bytes
      << " bytes while trying to transfer " << expected << " bytes";
    throw FileIO::file_error{};
  }
}
uint64_t get_size(HANDLE handle)
{
  LARGE_INTEGER size;
  if (!::GetFileSizeEx(handle, &size)) {
    auto err = ::GetLastError();
    LOG_ERROR << "GetFileSizeEx() failed for with 0x" << std::hex << err;
    throw FileIO::file_error{};
  }
  return size.QuadPart;
}

} // anonymous namespace

FileIO::FileIO(const std::string& filename, Storage::OpenMode mode)
{
  DWORD open_arg;
  switch (mode) {
  case Storage::OpenMode::create_new:    open_arg = CREATE_NEW;    break;
  case Storage::OpenMode::create_always: open_arg = CREATE_ALWAYS; break;
  case Storage::OpenMode::open_existing: open_arg = OPEN_EXISTING; break;
  case Storage::OpenMode::open_always:   open_arg = OPEN_ALWAYS;   break;
  default: throw bad_argument{};
  }

  m_file_handle =
    ::CreateFileA(filename.c_str(),                   // filename
                  GENERIC_WRITE | GENERIC_READ,       // desired access
                  FILE_SHARE_WRITE | FILE_SHARE_READ, // share mode
                  NULL,                               // security
                  open_arg,                           // create or open
                  FILE_FLAG_OVERLAPPED,               // attributes
                  NULL);                              // template

  if (m_file_handle == INVALID_HANDLE_VALUE) {
    auto err = ::GetLastError();
    LOG_ERROR << "CreateFile() failed for " << filename
              << " with 0x" << std::hex << err;
    throw file_error{};
  }
}

FileIO::~FileIO()
{
  ::CloseHandle(m_file_handle);
}

void FileIO::AsyncReq::wait()
{
  if (m_async_struct) {
    wait_overlapped(m_handle, m_async_struct.get(), m_expected);
    ::CloseHandle(m_async_struct->hEvent);
    m_async_struct.reset();
  }
}

void FileIO::read(const uint64_t offset, gsl::array_view<byte> buffer) const
{
  OVERLAPPED o;
  fill_overlapped(&o, offset);
  read_file(m_file_handle, buffer, &o);
  wait_overlapped(m_file_handle, &o, buffer.bytes());
}

void FileIO::write(const uint64_t offset, const gsl::array_view<byte> buffer)
{
  OVERLAPPED o;
  fill_overlapped(&o, offset);
  write_file(m_file_handle, buffer, &o);
  wait_overlapped(m_file_handle, &o, buffer.bytes());
}

void FileIO::resize(const uint64_t size)
{
  LARGE_INTEGER li;
  li.QuadPart = size;
  if (!::SetFilePointerEx(m_file_handle, li, NULL, FILE_BEGIN)) {
    auto err = ::GetLastError();
    LOG_ERROR
      << "SetFilePointer() failed for size " << size
      << " with 0x" << std::hex << err;
    throw file_error{};
  }
  if (!::SetEndOfFile(m_file_handle)) {
    auto err = ::GetLastError();
    LOG_ERROR
      << "SetEndOfFile() failed for position " << size
      << " with 0x" << std::hex << err;
    throw file_error{};
  }
}

FileIO::AsyncReq FileIO::read_async(const uint64_t offset,
                                    gsl::array_view<byte> buffer) const
{
  auto o = std::make_unique<OVERLAPPED>();
  fill_overlapped(o.get(), offset);
  read_file(m_file_handle, buffer, o.get());
  return AsyncReq{ std::move(o), m_file_handle, buffer.bytes() };
}

FileIO::AsyncReq FileIO::write_async(const uint64_t offset,
                                     const gsl::array_view<byte> buffer)
{
  auto o = std::make_unique<OVERLAPPED>();
  fill_overlapped(o.get(), offset);
  write_file(m_file_handle, buffer, o.get());
  return AsyncReq{ std::move(o), m_file_handle, buffer.bytes() };
}

uint64_t FileIO::size() const
{
  return get_size(m_file_handle);
}

#else
///////////////////////////////////////////////////////////////////////////////
// POSIX implementation

// TODO: POSIX impl

#endif
///////////////////////////////////////////////////////////////////////////////

} // namespace cheesebase
