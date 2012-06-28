/// @file   core/builtin/IO.cpp
/// @brief  Built-in IO factory (std::fstream)
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstring>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/utils/dm_security.h>
#include <errno.h>
#include <fstream>
#include <iosfwd>
#include <sys/stat.h>
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
  si_(0), secCtx_(0), passwd_(passwd), useIp_(useIp_)
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



IOHandler* StdIODriver::createIOHandler(const std::string& pfn, std::iostream::openmode openmode,
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
                             openmode == std::ios_base::out))
    throw DmException(DM_FORBIDDEN, "Token does not validate");
  
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




StdIOHandler::StdIOHandler(const std::string& path, std::iostream::openmode openmode) throw (DmException):
  stream_(path.c_str(), openmode), path_(path)
{
  // Check we actually opened
  if (this->stream_.fail()) {
    throw DmException(DM_NO_SUCH_FILE, "Could not open " + path);
  }
}



StdIOHandler::~StdIOHandler()
{
  this->close();
}



void StdIOHandler::close() throw (DmException)
{
  this->stream_.close();
}



size_t StdIOHandler::read(char* buffer, size_t count) throw (DmException)
{
  return this->stream_.read(buffer, count).gcount();
}



size_t StdIOHandler::write(const char* buffer, size_t count) throw (DmException)
{
  this->stream_.write(buffer, count);
  return count;
}



void StdIOHandler::seek(long offset, std::ios_base::seekdir whence) throw (DmException)
{
  this->stream_.seekg(offset, whence);
  this->stream_.seekp(offset, whence);
}



long StdIOHandler::tell(void) throw (DmException)
{
  return this->stream_.tellg();
}



void StdIOHandler::flush(void) throw (DmException)
{
  this->stream_.flush();
}



bool StdIOHandler::eof(void) throw (DmException)
{
  return this->stream_.eof();
}



struct stat StdIOHandler::pstat() throw (DmException)
{
  struct stat buf;

  if (stat(this->path_.c_str(), &buf) != 0) {
    switch (errno) {
      case ENOENT:
        throw DmException(DM_NO_SUCH_FILE, this->path_ + " does not exist");
        break;
      default:
        throw DmException(DM_INTERNAL_ERROR,
                          "Could not open %s (errno %d)", this->path_.c_str(), errno);
    }
  }

  return buf; 
}
