/// @file   RFIO.cpp
/// @brief  RFIO IO.
/// @author CERN DPM <dpm-devel@cern.ch>
#include <dmlite/cpp/dmlite.h>
#include <dpm_api.h>
#include <Cthread_api.h>
#include <errno.h>
#include <rfio_api.h>
#include <serrno.h>
#include "Adapter.h"
#include "DpmAdapter.h"
#include "FunctionWrapper.h"
#include "RFIO.h"

using namespace dmlite;


Logger::bitmask dmlite::adapterRFIOlogmask = 0;
Logger::component dmlite::adapterRFIOlogname = "AdapterRFIO";


// Prototype of rfio method (in rfio_api.h header but only available for rfio kernel)
extern "C" {
int rfio_parse(char *, char **, char **);
}




StdRFIOFactory::StdRFIOFactory(): passwd_("default"), useIp_(true)
{
  adapterRFIOlogmask = Logger::get()->getMask(adapterRFIOlogname);

  Cthread_init();

  setenv("CSEC_MECH", "ID", 1);
}



StdRFIOFactory::~StdRFIOFactory()
{
  // Nothing
}



void StdRFIOFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  bool gotit = true;
  LogCfgParm(Logger::Lvl4, adapterRFIOlogmask, adapterRFIOlogname, key, value);

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
    LogCfgParm(Logger::Lvl1, Logger::unregistered, "StdRFIOFactory", key, value);
  
}



IODriver* StdRFIOFactory::createIODriver(PluginManager* pm) throw (DmException)
{
  return new StdRFIODriver(this->passwd_, this->useIp_);
}



StdRFIODriver::StdRFIODriver(std::string passwd, bool useIp):
  si_(0), secCtx_(0), passwd_(passwd), useIp_(useIp)
{
  // Nothing
}



StdRFIODriver::~StdRFIODriver()
{
  // Nothing
}



std::string StdRFIODriver::getImplId() const throw ()
{
  return "StdRFIODriver";
}



void StdRFIODriver::setStackInstance(StackInstance* si) throw (DmException)
{
  this->si_ = si;
}



void StdRFIODriver::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  this->secCtx_ = ctx;
}



IOHandler* StdRFIODriver::createIOHandler(const std::string& pfn,
                                          int flags,
                                          const Extensible& extras,
                                          mode_t mode) throw (DmException)
{
  Log(Logger::Lvl4, adapterRFIOlogmask, adapterRFIOlogname, "pfn: " << pfn);

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
  return new StdRFIOHandler(pfn, flags, mode);
}

StdRFIOHandler::pp::pp(int fd, bool *peof, off_t np):
  fd(fd), peof(peof)
{
  if ((pos = rfio_lseek64(fd, 0, SEEK_CUR)) == -1)
    throw DmException(serrno, "Could not seek on fd ", fd);
  if (rfio_lseek64(fd, np, SEEK_SET) == - 1)
    throw DmException(serrno, "Could not seek on fd ", fd);
  eof = *peof; *peof = false;
}

StdRFIOHandler::pp::~pp()
{
  if (rfio_lseek64(fd, pos, SEEK_SET) == - 1)
    throw DmException(serrno, "Could not seek on fd ", fd);
  *peof = eof;
}

void StdRFIODriver::doneWriting(const Location& loc) throw (DmException)
{
  struct dpm_filestatus     *statuses;
  int                        nReplies;
  std::string                sfn;

  Log(Logger::Lvl4, adapterRFIOlogmask, adapterRFIOlogname, "loc: " << loc.toString());

  if (loc.empty())
    throw DmException(EINVAL, "Empty location");

  // Need the sfn
  sfn = loc[0].url.query.getString("sfn");
  if (sfn.empty())
    throw DmException(EINVAL, "sfn not specified. loc: %s", loc.toString().c_str());

  // Need the dpm token
  std::string token = loc[0].url.query.getString("dpmtoken");
  if (token.empty())
    throw DmException(EINVAL, "dpmtoken not specified. loc: %s", loc.toString().c_str()); 

  // Workaround... putdone in this context should be invoked by root
  // to make it work also in the cases when the client is unknown (nobody)

  FunctionWrapper<int> reset(dpm_client_resetAuthorizationId);
  reset();

  const char* sfnPtr[] = { sfn.c_str() };
  FunctionWrapper<int, char*, int, char**, int*, dpm_filestatus**>
    (dpm_putdone, (char*)token.c_str(), 1, (char**)&sfnPtr, &nReplies, &statuses)(3);

  dpm_free_filest(nReplies, statuses);
  Log(Logger::Lvl3, adapterRFIOlogmask, adapterRFIOlogname, "Exiting. loc: " << loc.toString());
}


StdRFIOHandler::StdRFIOHandler(const std::string& path,
                               int flags, mode_t mode) throw (DmException):
  eof_(false), islocal_(false)
{
  int err;
  char *host, *filename;
  std::string qpath = path;

  Log(Logger::Lvl4, adapterRFIOlogmask, adapterRFIOlogname, "path: " << path);

  if (qpath[0] == '/')
      qpath = "localhost:" + qpath;

  if ((err=pthread_mutex_init(&this->mtx_, 0)))
    throw DmException(err, "Could not create a new mutex");
  if (!rfio_parse((char *)qpath.c_str(),&host,&filename) && !host)
    this->islocal_ = true;
  this->fd_ = rfio_open64((char *)qpath.c_str(), flags, mode);
  if (this->fd_ == -1)
    throw DmException(rfio_serrno(), "Could not open %s", qpath.c_str());
}



StdRFIOHandler::~StdRFIOHandler()
{
  Log(Logger::Lvl4, adapterRFIOlogmask, adapterRFIOlogname, "");
  if (this->fd_ != -1)
    rfio_close(this->fd_);
  pthread_mutex_destroy(&this->mtx_);
  Log(Logger::Lvl3, adapterRFIOlogmask, adapterRFIOlogname, "Exiting.");
}


void StdRFIOHandler::close() throw (DmException)
{
  rfio_close(this->fd_);
  this->fd_ = -1;
}

int StdRFIOHandler::fileno(void) throw (DmException)
{
  Log(Logger::Lvl4, adapterRFIOlogmask, adapterRFIOlogname, " fd:" << this->fd_);
  if (islocal_) {
    return this->fd_;
  }
  throw DmException(EIO, "file not open or is accessed with rfio but not local, so file descriptor is unavailable");
}

size_t StdRFIOHandler::read(char* buffer, size_t count) throw (DmException)
{
  Log(Logger::Lvl4, adapterRFIOlogmask, adapterRFIOlogname, "count:" << count);

  lk l(islocal_ ? 0 : &this->mtx_);
  size_t nbytes = rfio_read(this->fd_, buffer, count);

  eof_  = (nbytes < count);

  Log(Logger::Lvl3, adapterRFIOlogmask, adapterRFIOlogname, "Exiting. count:" << count << " res:" << nbytes);
  return nbytes;
}

size_t StdRFIOHandler::pread(void* buffer, size_t count, off_t offset) throw (DmException)
{
  Log(Logger::Lvl4, adapterRFIOlogmask, adapterRFIOlogname, "offs:" << offset << "count:" << count);
  if (this->islocal_)
    return static_cast<size_t>(::pread(this->fd_, buffer, count, offset));
  lk l(&this->mtx_);
  pp p(this->fd_, &this->eof_, offset);

  size_t res = rfio_read(this->fd_, buffer, count);
  Log(Logger::Lvl3, adapterRFIOlogmask, adapterRFIOlogname, "Exiting. offs:" << offset << " count:" << count << " res:" << res);
  return res;
}

size_t StdRFIOHandler::write(const char* buffer, size_t count) throw (DmException)
{
  Log(Logger::Lvl4, adapterRFIOlogmask, adapterRFIOlogname, "count:" << count);
  lk l(islocal_ ? 0 : &this->mtx_);
  size_t nbytes = rfio_write(this->fd_, (char *)buffer, count);
  Log(Logger::Lvl3, adapterRFIOlogmask, adapterRFIOlogname, "Exiting. count:" << count << " res:" << nbytes);
  return nbytes;
}

size_t StdRFIOHandler::pwrite(const void* buffer, size_t count, off_t offset) throw (DmException)
{
  Log(Logger::Lvl4, adapterRFIOlogmask, adapterRFIOlogname, "offs:" << offset << "count:" << count);
  if (this->islocal_)
    return static_cast<size_t>(::pwrite(this->fd_, buffer, count, offset));
  lk l(&this->mtx_);
  pp p(this->fd_, &this->eof_, offset);

  size_t res = rfio_write(this->fd_, (char *)buffer, count);
  Log(Logger::Lvl3, adapterRFIOlogmask, adapterRFIOlogname, "Exiting. offs:" << offset << " count:" << count << " res:" << res);
  return res;
}

void StdRFIOHandler::seek(off_t offset, Whence whence) throw (DmException)
{

  Log(Logger::Lvl4, adapterRFIOlogmask, adapterRFIOlogname, "offs:" << offset);

  lk l(islocal_ ? 0 : &this->mtx_);
  if (rfio_lseek64(this->fd_, offset, whence) == -1)
    throw DmException(serrno, "Could not seek fd %s", this->fd_);

  Log(Logger::Lvl3, adapterRFIOlogmask, adapterRFIOlogname, "Exiting. offs:" << offset);
}



off_t StdRFIOHandler::tell(void) throw (DmException)
{
  Log(Logger::Lvl4, adapterRFIOlogmask, adapterRFIOlogname, "");

  lk l(islocal_ ? 0 : &this->mtx_);

  off_t o = rfio_lseek64(this->fd_, 0, SEEK_CUR);

  Log(Logger::Lvl3, adapterRFIOlogmask, adapterRFIOlogname, "Exiting. offs:" << o);
  return o;
}



void StdRFIOHandler::flush(void) throw (DmException)
{
  // Nothing
}



bool StdRFIOHandler::eof(void) throw (DmException)
{
  return eof_;
}
