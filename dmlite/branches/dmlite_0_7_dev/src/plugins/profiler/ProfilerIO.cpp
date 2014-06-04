/// @file   ProfilerPoolManager.cpp
/// @brief  ProfilerPoolManager implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include "Profiler.h"
#include "XrdXrootdMonData.hh"

#include <string.h>

using namespace dmlite;

// --------------- ProfilerIOHandler

ProfilerIOHandler::ProfilerIOHandler(IOHandler* decorates,
    const std::string& pfn, int flags, StackInstance *si) throw(DmException): stack_(si)
{
  this->decorated_   = decorates;
  this->decoratedId_ = new char [decorates->getImplId().size() + 1];
  strcpy(this->decoratedId_, decorates->getImplId().c_str());

  //test send fileMonitoring msg
  if (!this->stack_->contains("dictid")) {
    this->stack_->set("dictid", XrdMonitor::getDictId());
  }
  boost::any dictid_any = this->stack_->get("dictid");
  kXR_unt32 dictid = Extensible::anyToUnsigned(dictid_any);

  sendUserIdentOrNOP();

  XrdMonitor::reportXrdFileOpen(dictid, 0, pfn, 1001);
}

ProfilerIOHandler::~ProfilerIOHandler()
{
  delete this->decorated_;
  delete this->decoratedId_;
}

size_t ProfilerIOHandler::read(char* buffer, size_t count) throw (DmException)
{
  PROFILE_RETURN(size_t, read, buffer, count);
}

void ProfilerIOHandler::close(void) throw (DmException)
{
  PROFILE(close);
}
struct ::stat ProfilerIOHandler::fstat(void) throw (DmException)
{
  PROFILE_RETURN(struct ::stat, fstat);
}
size_t ProfilerIOHandler::write(const char* buffer, size_t count) throw (DmException)
{
  PROFILE_RETURN(size_t, write, buffer, count);
}
size_t ProfilerIOHandler::readv(const struct iovec* vector, size_t count) throw (DmException)
{
  PROFILE_RETURN(size_t, readv, vector, count);
}
size_t ProfilerIOHandler::writev(const struct iovec* vector, size_t count) throw (DmException)
{
  PROFILE_RETURN(size_t, writev, vector, count);
}
size_t ProfilerIOHandler::pread(void* buffer, size_t count, off_t offset) throw (DmException)
{
  PROFILE_RETURN(size_t, pread, buffer, count, offset);
}
size_t ProfilerIOHandler::pwrite(const void* buffer, size_t count, off_t offset) throw (DmException)
{
  PROFILE_RETURN(size_t, pwrite, buffer, count, offset);
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

void ProfilerIOHandler::sendUserIdentOrNOP()
{
  if (this->stack_->contains("sent_userident")) {
    return;
  }

  if (!this->stack_->contains("dictid")) {
    this->stack_->set("dictid", XrdMonitor::getDictId());
  }
  kXR_char dictid = Extensible::anyToUnsigned(this->stack_->get("dictid"));

  //XrdMonitor::sendShortUserIdent(dictid);

  SecurityContext *secCtx = this->stack_->getSecurityContext();

  XrdMonitor::sendUserIdent(dictid,
      // protocol
      secCtx->user.name, // user DN
      secCtx->credentials.remoteAddress, // user hostname
      // org
      // role
      secCtx->groups[0].name
      // info
  );

  this->stack_->set("sent_userident", true);
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
