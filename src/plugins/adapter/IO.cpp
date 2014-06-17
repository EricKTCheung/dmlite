/// @file   IO.cpp
/// @brief  Filesystem IO.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <dmlite/cpp/dmlite.h>
#include <dpm_api.h>
#include <Cthread_api.h>
#include <errno.h>
#include <serrno.h>
#include <sys/uio.h>
#include "Adapter.h"
#include "DpmAdapter.h"
#include "FunctionWrapper.h"
#include "IO.h"

using namespace dmlite;


StdIOFactory::StdIOFactory(): passwd_("default"), useIp_(true)
{
  Cthread_init();
  
  setenv("CSEC_MECH", "ID", 1);
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
  else if (key == "DpmHost" || key == "Host") {
    setenv("DPM_HOST", value.c_str(), 1);
    setenv("DPNS_HOST", value.c_str(), 1);
  }
  else
    throw DmException(DMLITE_CFGERR(DMLITE_UNKNOWN_KEY),
                      key + " not known");
}



IODriver* StdIOFactory::createIODriver(PluginManager* pm) throw (DmException)
{
  return new StdIODriver(this->passwd_, this->useIp_);
}



StdIODriver::StdIODriver(std::string passwd, bool useIp):
  secCtx_(0), passwd_(passwd), useIp_(useIp)
{
  // Nothing
}



StdIODriver::~StdIODriver()
{
  // Nothing
}



std::string StdIODriver::getImplId() const throw ()
{
  return "StdIODriver";
}



void StdIODriver::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  this->secCtx_ = ctx;
  
}



IOHandler* StdIODriver::createIOHandler(const std::string& pfn,
                                        int flags,
                                        const Extensible& extras,
                                        mode_t mode) throw (DmException)
{
  if ( !(flags & IODriver::kInsecure) ) {
      if (!extras.hasField("token"))
        throw DmException(EACCES, "Missing token");

      std::string userId;
      if (this->useIp_)
        userId = this->secCtx_->credentials.remoteAddress;
      else
        userId = this->secCtx_->credentials.clientName;

      if (dmlite::validateToken(extras.getString("token"),
            userId,
            pfn, this->passwd_,
            flags != O_RDONLY) != kTokenOK)

        throw DmException(EACCES, "Token does not validate (using %s)",
            this->useIp_?"IP":"DN");

  }
  
  // Create
  return new StdIOHandler(pfn, flags, mode);
}



void StdIODriver::doneWriting(const Location& loc) throw (DmException)
{
  struct dpm_filestatus     *statuses;
  int                        nReplies;
  std::string                sfn;
  
  if (loc.empty())
    throw DmException(EINVAL, "Empty location");

  // Need the sfn
  sfn = loc[0].url.query.getString("sfn");
  if (sfn.empty())
    throw DmException(EINVAL, "sfn not specified");

  // Need the dpm token
  std::string token = loc[0].url.query.getString("dpmtoken");
  if (token.empty())
    throw DmException(EINVAL, "dpmtoken not specified"); 

  
  // Workaround... putdone in this context should be invoked by root
  // to make it work also in the cases when the client is unknown (nobody)
  
  FunctionWrapper<int> reset(dpm_client_resetAuthorizationId);
  reset();
  
  const char* sfnPtr[] = { sfn.c_str() };
  FunctionWrapper<int, char*, int, char**, int*, dpm_filestatus**>
    (dpm_putdone, (char*)token.c_str(), 1, (char**)&sfnPtr, &nReplies, &statuses)(3);

  dpm_free_filest(nReplies, statuses);
}




void StdIODriver::setStackInstance(StackInstance* si) throw (DmException)
{
  this->si_ = si;
}




StdIOHandler::StdIOHandler(const std::string& path,
                           int flags, mode_t mode) throw (DmException):
  eof_(false)
{ 
  this->fd_ = ::open(path.c_str(), flags, mode);
  if (this->fd_ == -1)
    throw DmException(errno, "Could not open %s", path.c_str());
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



struct stat StdIOHandler::fstat() throw (DmException)
{
  struct stat st;
  ::fstat(this->fd_, &st);
  return st;
}



size_t StdIOHandler::read(char* buffer, size_t count) throw (DmException)
{
  ssize_t nbytes = ::read(this->fd_, buffer, count);
  if (nbytes < 0) {
    char errbuffer[128];
    strerror_r(errno, errbuffer, sizeof(errbuffer));
    throw DmException(errno, "%s", errbuffer);
  }
  
  eof_ = (static_cast<size_t>(nbytes) < count);
  
  return static_cast<size_t>(nbytes);
}



size_t StdIOHandler::write(const char* buffer, size_t count) throw (DmException)
{
  ssize_t nbytes = ::write(this->fd_, buffer, count);
  if (nbytes < 0) {
    char errbuffer[128];
    strerror_r(errno, errbuffer, sizeof(errbuffer));
    throw DmException(errno, "%s", errbuffer);
  }

  return static_cast<size_t>(nbytes);
}



size_t StdIOHandler::readv(const struct iovec* vector, size_t count) throw (DmException)
{
  ssize_t nbytes = ::readv(this->fd_, vector, count);
  if (nbytes < 0) {
    char errbuffer[128];
    strerror_r(errno, errbuffer, sizeof(errbuffer));
    throw DmException(errno, "%s", errbuffer);
  }

  return static_cast<size_t>(nbytes);
}



size_t StdIOHandler::writev(const struct iovec* vector, size_t count) throw (DmException)
{
  ssize_t nbytes = ::writev(this->fd_, vector, count);
  if (nbytes < 0) {
    char errbuffer[128];
    strerror_r(errno, errbuffer, sizeof(errbuffer));
    throw DmException(errno, "%s", errbuffer);
  }

  return static_cast<size_t>(nbytes);
}



size_t StdIOHandler::pread(void* buffer, size_t count, off_t offset) throw (DmException)
{
  ssize_t nbytes = ::pread(this->fd_, buffer, count, offset);
  if (nbytes < 0) {
    char errbuffer[128];
    strerror_r(errno, errbuffer, sizeof(errbuffer));
    throw DmException(errno, "%s", errbuffer);
  }

  return static_cast<size_t>(nbytes);
}



size_t StdIOHandler::pwrite(const void* buffer, size_t count, off_t offset) throw (DmException)
{
  ssize_t nbytes = ::pwrite(this->fd_, buffer, count, offset);
  if (nbytes < 0) {
    char errbuffer[128];
    strerror_r(errno, errbuffer, sizeof(errbuffer));
    throw DmException(errno, "%s", errbuffer);
  }

  return static_cast<size_t>(nbytes);
}



void StdIOHandler::seek(off_t offset, Whence whence) throw (DmException)
{
  if (::lseek64(this->fd_, offset, whence) == ((off_t) - 1))
    throw DmException(errno, "Could not seek");
}



off_t StdIOHandler::tell(void) throw (DmException)
{
  return ::lseek64(this->fd_, 0, SEEK_CUR);
}



void StdIOHandler::flush(void) throw (DmException)
{
  // Nothing
}



bool StdIOHandler::eof(void) throw (DmException)
{
  return eof_;
}
