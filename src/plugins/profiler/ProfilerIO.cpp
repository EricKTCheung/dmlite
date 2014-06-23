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
  this->stack_ = si;

  this->decorated_   = decorates;
  this->decoratedId_ = new char [decorates->getImplId().size() + 1];
  strcpy(this->decoratedId_, decorates->getImplId().c_str());

  xfrstats_.read = 0;
  xfrstats_.readv = 0;
  xfrstats_.write = 0;

  size_t file_size = 0;
  try {
    file_size = this->fstat().st_size;
  } catch (DmException& e) {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "Could not determine filesize for %s: %d, %s",
        pfn.c_str(), e.code(), e.what());
  }

  //test send fileMonitoring msg
  sendUserIdentOrNOP();
  // we actually never use this, but active LFN in the stream
  //sendFileOpen(pfn);
  reportXrdFileOpen(pfn, file_size);

  syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
      __func__);
}

ProfilerIOHandler::~ProfilerIOHandler()
{
  if (!file_closed_) {
    reportXrdFileClose(this->xfrstats_, true);
  }

  delete this->decorated_;
  delete this->decoratedId_;

  syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
      __func__);
}

size_t ProfilerIOHandler::read(char* buffer, size_t count) throw (DmException)
{
  PROFILE_ASSIGN(size_t, read, buffer, count);
  xfrstats_.read += ret;
  return ret;
}

void ProfilerIOHandler::close(void) throw (DmException)
{
  PROFILE(close);

  reportXrdFileClose(this->xfrstats_);
  file_closed_ = true;
}
struct ::stat ProfilerIOHandler::fstat(void) throw (DmException)
{
  PROFILE_RETURN(struct ::stat, fstat);
}
size_t ProfilerIOHandler::write(const char* buffer, size_t count) throw (DmException)
{
  PROFILE_ASSIGN(size_t, write, buffer, count);
  xfrstats_.write += ret;
  return ret;
}
size_t ProfilerIOHandler::readv(const struct iovec* vector, size_t count) throw (DmException)
{
  PROFILE_ASSIGN(size_t, readv, vector, count);
  xfrstats_.readv += ret;
  return ret;
}
size_t ProfilerIOHandler::writev(const struct iovec* vector, size_t count) throw (DmException)
{
  PROFILE_ASSIGN(size_t, writev, vector, count);
  xfrstats_.write += ret;
  return ret;
}
size_t ProfilerIOHandler::pread(void* buffer, size_t count, off_t offset) throw (DmException)
{
  PROFILE_ASSIGN(size_t, pread, buffer, count, offset);
  xfrstats_.read += ret;
  return ret;
}
size_t ProfilerIOHandler::pwrite(const void* buffer, size_t count, off_t offset) throw (DmException)
{
  PROFILE_ASSIGN(size_t, pwrite, buffer, count, offset);
  xfrstats_.write += ret;
  return ret;
}
void ProfilerIOHandler::seek(off_t offset, Whence whence) throw (DmException)
{
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
  this->decorated_   = decorates;
  this->decoratedId_ = strdup( decorates->getImplId().c_str() );

  syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
      __func__);
}

ProfilerIODriver::~ProfilerIODriver() {

  delete this->decorated_;
  free(this->decoratedId_);

  reportXrdFileDiscAndFlushOrNOP();

  syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
      __func__);
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
