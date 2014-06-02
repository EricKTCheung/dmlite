/// @file   ProfilerPoolManager.cpp
/// @brief  ProfilerPoolManager implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include "Profiler.h"
#include "XrdXrootdMonData.hh"

#include <string.h>

using namespace dmlite;

// --------------- ProfilerIOHandler

ProfilerIOHandler::ProfilerIOHandler(IOHandler* decorates) throw(DmException)
{
  this->decorated_   = decorates;
  this->decoratedId_ = new char [decorates->getImplId().size() + 1];
  strcpy(this->decoratedId_, decorates->getImplId().c_str());
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
}



void ProfilerIODriver::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  BaseInterface::setSecurityContext(this->decorated_, ctx);
}
