/// @file   ProfilerPoolManager.cpp
/// @brief  ProfilerPoolManager implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include "Profiler.h"
#include "ProfilerXrdMon.h"
#include "XrdXrootdMonData.hh"

#include <string.h>

using namespace dmlite;

// --------------- ProfilerIOHandler

ProfilerIOHandler::ProfilerIOHandler(IOHandler* decorates,
    const std::string& pfn, int flags, StackInstance *si) throw(DmException)
{
  Log(Logger::DEBUG, profilerlogmask, profilerlogname, " path:" << pfn);

  this->stack_ = si;

  this->decorated_   = decorates;
  this->decoratedId_ = strdup( decorates->getImplId().c_str() );

  xfrstats_.read = 0;
  xfrstats_.readv = 0;
  xfrstats_.write = 0;

  size_t file_size = 0;
  try {
    file_size = this->fstat().st_size;
  } catch (DmException& e) {
    Err(profilerlogname, "Could not determine filesize for " << pfn << ": " << e.code() << ", " << e.what());
  }

  //test send fileMonitoring msg
  sendUserIdentOrNOP();
  // we actually never use this, but active LFN in the stream
  //sendFileOpen(pfn);
  reportXrdFileOpen(pfn, file_size);
}

ProfilerIOHandler::~ProfilerIOHandler()
{
  if (!file_closed_) {
    reportXrdFileClose(this->xfrstats_, true);
  }

  delete this->decorated_;
  free(this->decoratedId_);
}

size_t ProfilerIOHandler::read(char* buffer, size_t count) throw (DmException)
{
  Log(Logger::DEBUG, profilerlogmask, profilerlogname, " count:" << count);

  PROFILE_ASSIGN(size_t, read, buffer, count);
  xfrstats_.read += ret;
  return ret;
}

void ProfilerIOHandler::close(void) throw (DmException)
{
  Log(Logger::DEBUG, profilerlogmask, profilerlogname, "");

  PROFILE(close);

  reportXrdFileClose(this->xfrstats_);
  file_closed_ = true;
}
struct ::stat ProfilerIOHandler::fstat(void) throw (DmException)
{
  Log(Logger::DEBUG, profilerlogmask, profilerlogname, "");

  PROFILE_RETURN(struct ::stat, fstat);
}
size_t ProfilerIOHandler::write(const char* buffer, size_t count) throw (DmException)
{
  Log(Logger::DEBUG, profilerlogmask, profilerlogname, " count:" << count);

  PROFILE_ASSIGN(size_t, write, buffer, count);
  xfrstats_.write += ret;
  return ret;
}
size_t ProfilerIOHandler::readv(const struct iovec* vector, size_t count) throw (DmException)
{
  Log(Logger::DEBUG, profilerlogmask, profilerlogname, " count:" << count);

  PROFILE_ASSIGN(size_t, readv, vector, count);
  xfrstats_.readv += ret;
  return ret;
}
size_t ProfilerIOHandler::writev(const struct iovec* vector, size_t count) throw (DmException)
{
  Log(Logger::DEBUG, profilerlogmask, profilerlogname, " count:" << count);

  PROFILE_ASSIGN(size_t, writev, vector, count);
  xfrstats_.write += ret;
  return ret;
}
size_t ProfilerIOHandler::pread(void* buffer, size_t count, off_t offset) throw (DmException)
{
  Log(Logger::DEBUG, profilerlogmask, profilerlogname, " count:" << count);

  PROFILE_ASSIGN(size_t, pread, buffer, count, offset);
  xfrstats_.read += ret;
  return ret;
}
size_t ProfilerIOHandler::pwrite(const void* buffer, size_t count, off_t offset) throw (DmException)
{
  Log(Logger::DEBUG, profilerlogmask, profilerlogname, " count:" << count);

  PROFILE_ASSIGN(size_t, pwrite, buffer, count, offset);
  xfrstats_.write += ret;
  return ret;
}
void ProfilerIOHandler::seek(off_t offset, Whence whence) throw (DmException)
{
  Log(Logger::DEBUG, profilerlogmask, profilerlogname, " offs:" << offset);

  PROFILE(seek, offset, whence);
}
off_t ProfilerIOHandler::tell(void) throw (DmException)
{
  PROFILE_RETURN(off_t, tell);
}
void ProfilerIOHandler::flush(void) throw (DmException)
{
  PROFILE(flush);
}
bool ProfilerIOHandler::eof(void) throw (DmException)
{
  PROFILE_RETURN(bool, eof);
}


// --------------- ProfilerIODriver

ProfilerIODriver::ProfilerIODriver(IODriver* decorates) throw(DmException)
{
  Log(Logger::DEBUG, profilerlogmask, profilerlogname, " Ctor");

  this->decorated_   = decorates;
  this->decoratedId_ = strdup( decorates->getImplId().c_str() );
}

ProfilerIODriver::~ProfilerIODriver() {

  delete this->decorated_;
  free(this->decoratedId_);

  reportXrdFileDiscAndFlushOrNOP();

  Log(Logger::INFO, profilerlogmask, profilerlogname, "");
}


void ProfilerIODriver::setStackInstance(StackInstance* si) throw (DmException)
{
  BaseInterface::setStackInstance(this->decorated_, si);
  this->stack_ = si;
}



void ProfilerIODriver::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  BaseInterface::setSecurityContext(this->decorated_, ctx);
}

void ProfilerIODriver::doneWriting(const Location& loc) throw (DmException)
{
  Log(Logger::INFO, profilerlogmask, profilerlogname, " loc:" << loc.toString());

  PROFILE(doneWriting, loc);
}
