/// @file   DomeAdapterIO.cpp
/// @brief  Filesystem IO with dome
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/pooldriver.h>
#include <errno.h>
#include <sys/uio.h>
#include <iostream>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "utils/logger.h"
#include "utils/DavixPool.h"
#include "DomeAdapterIO.h"
#include "DomeAdapter.h"

using namespace dmlite;

DomeIOFactory::DomeIOFactory(): passwd_("default"), useIp_(true)
{
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " Ctor");
  setenv("CSEC_MECH", "ID", 1);
}

DomeIOFactory::~DomeIOFactory()
{
  // Nothing
}

void DomeIOFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  std::cout << "configuring DomeIOFactory: " << key << ": " << value << std::endl;
  bool gotit = true;
  LogCfgParm(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, key, value);

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
  else gotit = false;
  
  if (gotit)
    LogCfgParm(Logger::Lvl1, Logger::unregistered, "BuiltInAuthnFactory", key, value);
   
}

IODriver* DomeIOFactory::createIODriver(PluginManager* pm) throw (DmException)
{
  return new DomeIODriver(this->passwd_, this->useIp_);
}

DomeIODriver::DomeIODriver(std::string passwd, bool useIp):
  secCtx_(0), passwd_(passwd), useIp_(useIp)
{
  // Nothing
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " Ctor");
}

DomeIODriver::~DomeIODriver()
{
  // Nothing
}

std::string DomeIODriver::getImplId() const throw ()
{
  return "DomeIODriver";
}

void DomeIODriver::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  this->secCtx_ = ctx;
}

void DomeIODriver::setStackInstance(StackInstance* si) throw (DmException)
{
  this->si_ = si;
}

IOHandler* DomeIODriver::createIOHandler(const std::string& pfn,
                                        int flags,
                                        const Extensible& extras,
                                        mode_t mode) throw (DmException)
{
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " pfn:" << pfn);

  if ( !(flags & IODriver::kInsecure) ) {
      if (!extras.hasField("token"))
        throw DmException(EACCES, "Missing token on pfn: %s", pfn.c_str());

      std::string userId;
      if (this->useIp_)
        userId = this->secCtx_->credentials.remoteAddress;
      else
        userId = this->secCtx_->credentials.clientName;

      if (dmlite::validateToken(extras.getString("token"),
            userId,
            pfn, this->passwd_,
            flags != O_RDONLY) != kTokenOK)

        throw DmException(EACCES, "Token does not validate (using %s) on pfn %s",
            this->useIp_?"IP":"DN", pfn.c_str());

  }

  // Create
  return new DomeIOHandler(pfn, flags, mode);
}

void DomeIODriver::doneWriting(const Location& loc) throw (DmException)
{
  Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, " loc:" << loc.toString());

  struct dpm_filestatus     *statuses;
  int                        nReplies;
  std::string                sfn;

  if (loc.empty())
    throw DmException(EINVAL, "Empty location");

  // Need the sfn
  sfn = loc[0].url.query.getString("sfn");
  if (sfn.empty())
    throw DmException(EINVAL, "sfn not specified loc: %s", loc.toString().c_str());

  // Need the dpm token
  // std::string token = loc[0].url.query.getString("dpmtoken");
  // if (token.empty())
  //   throw DmException(EINVAL, "dpmtoken not specified loc: %s", loc.toString().c_str());

  // send a put done to the local dome instance
  std::cout << "I am in doneWriting, about to send putdone to dome" << std::endl;
  Davix::Uri uri("https://localhost/domedisk/" + sfn);
  Davix::DavixError* err = NULL;
  DavixStuff *ds = DavixCtxPoolHolder::getDavixCtxPool().acquire();

  Davix::PostRequest req(*ds->ctx, uri, &err);
  req.addHeaderField("remoteclientdn", this->secCtx_->credentials.clientName);
  req.addHeaderField("remoteclientaddr", this->secCtx_->credentials.remoteAddress);

  boost::property_tree::ptree jresp;
  jresp.put("pfn", loc[0].url.path);

  std::ostringstream os;
  boost::property_tree::write_json(os, jresp);
  req.setRequestBody(os.str());

  // Workaround... putdone in this context should be invoked by root
  // to make it work also in the cases when the client is unknown (nobody)

  // FunctionWrapper<int> reset(dpm_client_resetAuthorizationId);
  // reset();

  // const char* sfnPtr[] = { sfn.c_str() };
  // FunctionWrapper<int, char*, int, char**, int*, dpm_filestatus**>
  //   (dpm_putdone, (char*)token.c_str(), 1, (char**)&sfnPtr, &nReplies, &statuses)(3);

  // Log(Logger::Lvl2, adapterlogmask, adapterlogname, " loc:" << loc.toString() <<
  //    " status[0]:" << (nReplies > 0 ? statuses->status : -1) <<
  //    " errstring: '" << (statuses->errstring != NULL ? statuses->errstring : "") << "'"
  //   );

  // dpm_free_filest(nReplies, statuses);
}




DomeIOHandler::DomeIOHandler(const std::string& path,
                           int flags, mode_t mode) throw (DmException):
  eof_(false)
{
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " path:" << path);

  this->fd_ = ::open(path.c_str(), flags, mode);
  if (this->fd_ == -1) {
    char errbuffer[128];
    strerror_r(errno, errbuffer, sizeof(errbuffer));
    throw DmException(errno, "Could not open %s err: %s", path.c_str(), errbuffer);
  }
}



DomeIOHandler::~DomeIOHandler()
{
  if (this->fd_ != -1)
    ::close(this->fd_);
}



void DomeIOHandler::close() throw (DmException)
{
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " fd:" << this->fd_);
  ::close(this->fd_);
  this->fd_ = -1;
}


int DomeIOHandler::fileno() throw (DmException)
{
    Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " fd:" << this->fd_);
    return this->fd_;
}


struct stat DomeIOHandler::fstat() throw (DmException)
{

  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " fd:" << this->fd_);

  struct stat st;
  ::fstat(this->fd_, &st);
  return st;
}



size_t DomeIOHandler::read(char* buffer, size_t count) throw (DmException)
{
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " fd:" << this->fd_ << " count:" << count);

  ssize_t nbytes = ::read(this->fd_, buffer, count);
  if (nbytes < 0) {
    char errbuffer[128];
    strerror_r(errno, errbuffer, sizeof(errbuffer));
    throw DmException(errno, "%s on fd %s ", errbuffer, this->fd_);
  }

  eof_ = (static_cast<size_t>(nbytes) < count);

  return static_cast<size_t>(nbytes);
}



size_t DomeIOHandler::write(const char* buffer, size_t count) throw (DmException)
{
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " fd:" << this->fd_ << " count:" << count);

  ssize_t nbytes = ::write(this->fd_, buffer, count);
  if (nbytes < 0) {
    char errbuffer[128];
    strerror_r(errno, errbuffer, sizeof(errbuffer));
    throw DmException(errno, "%s on fd %s ", errbuffer, this->fd_);
  }

  return static_cast<size_t>(nbytes);
}



size_t DomeIOHandler::readv(const struct iovec* vector, size_t count) throw (DmException)
{
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " fd:" << this->fd_ << " count:" << count);

  ssize_t nbytes = ::readv(this->fd_, vector, count);
  if (nbytes < 0) {
    char errbuffer[128];
    strerror_r(errno, errbuffer, sizeof(errbuffer));
    throw DmException(errno, "%s on fd %s ", errbuffer, this->fd_);
  }

  return static_cast<size_t>(nbytes);
}



size_t DomeIOHandler::writev(const struct iovec* vector, size_t count) throw (DmException)
{
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " fd:" << this->fd_ << " count:" << count);

  ssize_t nbytes = ::writev(this->fd_, vector, count);
  if (nbytes < 0) {
    char errbuffer[128];
    strerror_r(errno, errbuffer, sizeof(errbuffer));
    throw DmException(errno, "%s on fd %s ", errbuffer, this->fd_);
  }

  return static_cast<size_t>(nbytes);
}



size_t DomeIOHandler::pread(void* buffer, size_t count, off_t offset) throw (DmException)
{
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " fd:" << this->fd_ << " count:" << count);

  ssize_t nbytes = ::pread(this->fd_, buffer, count, offset);
  if (nbytes < 0) {
    char errbuffer[128];
    strerror_r(errno, errbuffer, sizeof(errbuffer));
    throw DmException(errno, "%s on fd %s ", errbuffer, this->fd_);
  }

  return static_cast<size_t>(nbytes);
}



size_t DomeIOHandler::pwrite(const void* buffer, size_t count, off_t offset) throw (DmException)
{
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " fd:" << this->fd_ << " count:" << count);

  ssize_t nbytes = ::pwrite(this->fd_, buffer, count, offset);
  if (nbytes < 0) {
    char errbuffer[128];
    strerror_r(errno, errbuffer, sizeof(errbuffer));
    throw DmException(errno, "%s on fd %s ", errbuffer, this->fd_);
  }

  return static_cast<size_t>(nbytes);
}



void DomeIOHandler::seek(off_t offset, Whence whence) throw (DmException)
{
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " fd:" << this->fd_ << " offs:" << offset);
  if (::lseek64(this->fd_, offset, whence) == ((off_t) - 1))
    throw DmException(errno, "Could not seek on fd %s ", this->fd_);
}



off_t DomeIOHandler::tell(void) throw (DmException)
{

  return ::lseek64(this->fd_, 0, SEEK_CUR);
}



void DomeIOHandler::flush(void) throw (DmException)
{
  // Nothing
}



bool DomeIOHandler::eof(void) throw (DmException)
{
  return eof_;
}
