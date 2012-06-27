/// @file   core/builtin/IO.cpp
/// @brief  Built-in IO factory (std::fstream)
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstring>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/utils/dm_security.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "Adapter.h"
#include "IO.h"

using namespace dmlite;



StdIOFactory::StdIOFactory(): passwd_("default"), useIp_(true)
{
  // Nothing
}



StdIOFactory::~StdIOFactory()
{
  // Nothing
}



void StdIOFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  if (key == "TokenPassword") {
    this->passwd_ = value;
  }
  else if (key == "TokenId") {
    if (strcasecmp(value.c_str(), "ip") == 0)
      this->useIp_ = true;
    else
      this->useIp_ = false;
  }
  throw DmException(DM_UNKNOWN_OPTION, key + " not known");
}



IODriver* StdIOFactory::createIODriver(PluginManager* pm) throw (DmException)
{
  return new StdIODriver(this->passwd_, this->useIp_);
}



StdIODriver::StdIODriver(std::string passwd, bool useIp):
  si_(0), secCtx_(0), passwd_(passwd), useIp_(useIp)
{
  // Nothing
}



StdIODriver::~StdIODriver()
{
  // Nothing
}



void StdIODriver::setStackInstance(StackInstance* si) throw (DmException)
{
  this->si_ = si;
}



void StdIODriver::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  this->secCtx_ = ctx;
}



IOHandler* StdIODriver::createIOHandler(const std::string& pfn, int openmode,
                                        const std::map<std::string, std::string>& extras) throw (DmException)
{
  // Validate request
  std::map<std::string, std::string>::const_iterator i = extras.find("token");
  
  if (i == extras.end())
    throw DmException(DM_FORBIDDEN, "Missing token");
  
  std::string userId;
  if (this->useIp_)
    userId = this->secCtx_->getCredentials().remote_addr;
  else
    userId = this->secCtx_->getCredentials().client_name;
  
  if (!dmlite::validateToken(i->second,
                             userId,
                             pfn, this->passwd_,
                             openmode & std::ios_base::out))
    throw DmException(DM_FORBIDDEN, "Token does not validate (%s)", userId.c_str());
  
  // Create
  return new StdIOHandler(pfn, openmode);
}



struct stat StdIODriver::pStat(const std::string& pfn) throw (DmException)
{
  struct stat st;
  
  if (stat(pfn.c_str(), &st) != 0)
    ThrowExceptionFromSerrno(errno);
  
  return st;
}




StdIOHandler::StdIOHandler(const std::string& path, int openmode) throw (DmException):
  eof_(false), pos_(0)
{
  this->fd_ = ::open(path.c_str(), openmode, 0644);
  if (this->fd_ == -1)
    throw DmException(DM_NO_SUCH_FILE, "Could not open %s (%d)", path.c_str(), errno);
}



StdIOHandler::~StdIOHandler()
{
  if (this->fd_ != -1)
    ::close(this->fd_);
}



void StdIOHandler::close() throw (DmException)
{
  ::close(this->fd_);
  this->fd_ = -1;
}



size_t StdIOHandler::read(char* buffer, size_t count) throw (DmException)
{
  size_t nbytes = ::read(this->fd_, buffer, count);
  
  eof_ = (nbytes < count);
  
  return nbytes;
}



size_t StdIOHandler::write(const char* buffer, size_t count) throw (DmException)
{
  return ::write(this->fd_, buffer, count);
}



void StdIOHandler::seek(long offset, int whence) throw (DmException)
{
  if ((pos_ = ::lseek(this->fd_, offset, whence)) == ((off_t) - 1))
    throw DmException(DM_INTERNAL_ERROR, "Could not seek (%d)", errno);
}



long StdIOHandler::tell(void) throw (DmException)
{
  return pos_;
}



void StdIOHandler::flush(void) throw (DmException)
{
  // Nothing
}



bool StdIOHandler::eof(void) throw (DmException)
{
  return eof_;
}



struct stat StdIOHandler::pstat() throw (DmException)
{
  struct stat buf;
  if (fstat(this->fd_, &buf) != 0)
    throw DmException(DM_INTERNAL_ERROR, "Could not stat (%d)", errno);
  return buf;
}
