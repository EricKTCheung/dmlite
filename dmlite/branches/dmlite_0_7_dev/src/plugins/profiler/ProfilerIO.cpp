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

  //test send fileMonitoring msg
  sendUserIdentOrNOP();

  if (!this->stack_->contains("fileid")) {
    this->stack_->set("fileid", XrdMonitor::getDictId());
  }
  boost::any fileid_any = this->stack_->get("fileid");
  kXR_unt32 fileid = Extensible::anyToUnsigned(fileid_any);

  const SecurityContext *secCtx = this->stack_->getSecurityContext();
  kXR_unt32 dictid = XrdMonitor::getDictIdFromDn(secCtx->user.name);

  XrdMonitor::sendFileOpen(fileid, pfn);
  XrdMonitor::reportXrdFileOpen(dictid, fileid, pfn, 1001);
}

ProfilerIOHandler::~ProfilerIOHandler()
{
  if (!file_closed_) {
    boost::any fileid_any = this->stack_->get("fileid");
    kXR_unt32 fileid = Extensible::anyToUnsigned(fileid_any);

    XrdMonitor::reportXrdFileClose(fileid, this->xfrstats_, true);
  }

  if (this->stack_->contains("fileid")) {
    this->stack_->erase("fileid");
  }

  delete this->decorated_;
  delete this->decoratedId_;
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

  boost::any fileid_any = this->stack_->get("fileid");
  kXR_unt32 fileid = Extensible::anyToUnsigned(fileid_any);

  XrdMonitor::reportXrdFileClose(fileid, this->xfrstats_);
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
}

ProfilerIODriver::~ProfilerIODriver() {

  delete this->decorated_;
  free(this->decoratedId_);
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
