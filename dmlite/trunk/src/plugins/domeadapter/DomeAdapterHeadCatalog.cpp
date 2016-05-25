/// @file   DomeAdapterIO.cpp
/// @brief  Filesystem IO with dome
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/pooldriver.h>
#include <errno.h>
#include <sys/uio.h>
#include <iostream>
#include <stdio.h>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "utils/logger.h"
#include "utils/DomeUtils.h"
#include "utils/DomeTalker.h"
#include "DomeAdapterHeadCatalog.h"
// #include "DomeAdapter.h"

using namespace dmlite;
using namespace Davix;

DomeAdapterHeadCatalogFactory::DomeAdapterHeadCatalogFactory(): davixPool_(&davixFactory_, 10) {
  domeadapterlogmask = Logger::get()->getMask(domeadapterlogname);
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " Ctor");
}

DomeAdapterHeadCatalogFactory::~DomeAdapterHeadCatalogFactory()
{
  // Nothing
}

void DomeAdapterHeadCatalogFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  bool gotit = true;
  LogCfgParm(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, key, value);

  if (key == "DomeHead") {
    domehead_ = value;
  }
  // if parameter starts with "Davix", pass it on to the factory
  else if( key.find("Davix") != std::string::npos) {
    Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Received davix pool parameter: " << key << "," << value);
    davixFactory_.configure(key, value);
  }
  else gotit = false;

  if (gotit)
    LogCfgParm(Logger::Lvl1, Logger::unregistered, "BuiltInAuthnFactory", key, value);
}

// IODriver* DomeIOFactory::createIODriver(PluginManager* pm) throw (DmException)
// {
//   return new DomeIODriver(passwd_, useIp_, domedisk_, davixPool_);
// }

DomeAdapterHeadCatalog::DomeAdapterHeadCatalog(DomeAdapterHeadCatalogFactory *factory) :
  secCtx_(0), factory_(*factory)
{
  // Nothing
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " Ctor");
}

DomeAdapterHeadCatalog::~DomeAdapterHeadCatalog()
{
  // Nothing
}

std::string DomeAdapterHeadCatalog::getImplId() const throw ()
{
  return "DomeAdapterHeadCatalog";
}

void DomeAdapterHeadCatalog::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  this->secCtx_ = ctx;
}

void DomeAdapterHeadCatalog::setStackInstance(StackInstance* si) throw (DmException)
{
  this->si_ = si;
}

void DomeAdapterHeadCatalog::getChecksum(const std::string& path,
                                         const std::string& csumtype,
                                         std::string& csumvalue,
                                         const bool allowcalc,
                                         const int waitsecs) throw (DmException) {

  Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, " Entering, path '"
                                                            << path << "', csumtype '"
                                                            << csumtype << "'");

  csumvalue = "DomeAdapterHeadCatalog-test";

  // while(1) {
  //   DomeTalker talker(factory_.davixPool_, &secCtx_->credentials, factory_.domehead_,
  //                     "GET", "dome_chksum");
  //
  //   boost::property_tree::ptree params;
  //   params.put("checksum-type", csumtype);
  //   params.put("lfn", path);
  // }
  
}

// IOHandler* DomeIODriver::createIOHandler(const std::string& pfn,
//                                         int flags,
//                                         const Extensible& extras,
//                                         mode_t mode) throw (DmException)
// {
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " pfn:" << pfn);
//
//   if ( !(flags & IODriver::kInsecure) ) {
//       if (!extras.hasField("token"))
//         throw DmException(EACCES, "Missing token on pfn: %s", pfn.c_str());
//
//       std::string userId;
//       if (this->useIp_)
//         userId = this->secCtx_->credentials.remoteAddress;
//       else
//         userId = this->secCtx_->credentials.clientName;
//
//       // this request might either be coming from a regular user client,
//       // or another disk server trying to do tunnelling. In the latter
//       // case, the userId is simply "root"
//       if( dmlite::validateToken(extras.getString("token"),
//          "root", pfn, this->passwd_, flags != O_RDONLY) != kTokenOK &&
//
//           dmlite::validateToken(extras.getString("token"),
//             userId,
//             pfn, this->passwd_,
//             flags != O_RDONLY) != kTokenOK)
//
//         throw DmException(EACCES, "Token does not validate (using %s) on pfn %s",
//             this->useIp_?"IP":"DN", pfn.c_str());
//   }
//
//   // Create - local or tunneled?
//   if(DomeUtils::server_from_rfio_syntax(pfn) == pfn) {
//     return new DomeIOHandler(pfn, flags, mode);
//   }
//
//   // tunneled I/O
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " Creating tunnel handler for " << pfn);
//   std::string server = DomeUtils::server_from_rfio_syntax(pfn);
//   std::string path = DomeUtils::pfn_from_rfio_syntax(pfn);
//
//   // we are a disk server doing tunnelling, use "root" as userId
//   std::string supertoken = dmlite::generateToken("root", path, this->passwd_, 50000, flags != O_RDONLY);
//
//   std::string url = SSTR("http://" << server << "/" << path << "?token=" << Uri::escapeString(supertoken));
//   return new DomeTunnelHandler(davixPool_, url, flags, mode);
// }
//
// void DomeIODriver::doneWriting(const Location& loc) throw (DmException)
// {
//   Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, " loc:" << loc.toString());
//
//   struct dpm_filestatus     *statuses;
//   int                        nReplies;
//   std::string                sfn;
//
//   if (loc.empty())
//     throw DmException(EINVAL, "Empty location");
//
//   // Need the sfn
//   sfn = loc[0].url.query.getString("sfn");
//   if (sfn.empty())
//     throw DmException(EINVAL, "sfn not specified loc: %s", loc.toString().c_str());
//
//   // send a put done to the local dome instance
//   Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, " about to send put done for " << loc[0].url.path << " - " << sfn);
//
//   DomeTalker talker(davixPool_, &secCtx_->credentials, domedisk_,
//                     "POST", "dome_putdone");
//
//   boost::property_tree::ptree params;
//   params.put("pfn", loc[0].url.path);
//   params.put("size", loc[0].size);
//   params.put("lfn", sfn);
//
//   if(!talker.execute(params)) {
//     throw DmException(EINVAL, talker.err());
//   }
//
//   Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, "doneWriting was successful - putdone sent to domedisk");
// }
//
// DomeTunnelHandler::DomeTunnelHandler(DavixCtxPool &pool, const std::string &url, int flags, mode_t mode) throw(DmException)
//   : url_(url), grabber_(pool), ds_(grabber_), dpos_(ds_->ctx) {
//
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " Tunnelling '" << url_ << "', flags: " << flags << ", mode: " << mode);
//
//   DavixError *err = NULL;
//   ds_->parms->addHeader("Content-Range", "bytes 0-/*");
//   fd_ = dpos_.open(ds_->parms, url_, flags, &err);
//   checkErr(&err);
//
//   lastRead_ = 1;
// }
//
// DomeTunnelHandler::~DomeTunnelHandler() {
//
// }
//
// void DomeTunnelHandler::close() throw (DmException) {
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " Closing");
//   DavixError *err = NULL;
//   dpos_.close(fd_, &err);
//   checkErr(&err);
// }
//
// int DomeTunnelHandler::fileno() throw (DmException) {
//   throw DmException(EIO, "File is not local, file descriptor is unavailable");
// }
//
// void DomeTunnelHandler::checkErr(DavixError **err) throw (DmException) {
//   if(err && *err) {
//     throw DmException(EINVAL, SSTR("DavixError (" << (*err)->getStatus() << "): " << (*err)->getErrMsg() ));
//   }
// }
//
// size_t DomeTunnelHandler::read(char* buffer, size_t count) throw (DmException) {
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " DomeTunnelHandler. Read " << count << " bytes");
//
//   DavixError *err = NULL;
//   lastRead_ = dpos_.read(fd_, buffer, count, &err);
//   checkErr(&err);
//   return lastRead_;
// }
//
// size_t DomeTunnelHandler::write(const char* buffer, size_t count) throw (DmException) {
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " DomeTunnelHandler. Write " << count << " bytes");
//
//   DavixError *err = NULL;
//   size_t ret = dpos_.write(fd_, buffer, count, &err);
//   checkErr(&err);
//   return ret;
// }
//
// size_t DomeTunnelHandler::pread(void* buffer, size_t count, off_t offset) throw (DmException) {
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " DomeTunnelHandler. pread " << count << " bytes with offset " << offset);
//
//   DavixError *err = NULL;
//   lastRead_ = dpos_.pread(fd_, buffer, count, offset, &err);
//   checkErr(&err);
//   return lastRead_;
// }
//
// size_t DomeTunnelHandler::pwrite(const void* buffer, size_t count, off_t offset) throw (DmException) {
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " DomeTunnelHandler. pwrite " << count << " bytes with offset " << offset);
//
//   DavixError *err = NULL;
//   size_t ret = dpos_.pwrite(fd_, buffer, count, offset, &err);
//   checkErr(&err);
//   return ret;
// }
//
// void DomeTunnelHandler::seek(off_t offset, Whence whence) throw (DmException) {
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " DomeTunnelHandler. seek at offset " << offset << ", whence " << whence);
//   DavixError *err = NULL;
//   dpos_.lseek(fd_, offset, whence, &err);
//   checkErr(&err);
// }
//
// off_t DomeTunnelHandler::tell(void) throw (DmException) {
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " DomeTunnelHandler. tell");
//   DavixError *err = NULL;
//   off_t ret = dpos_.lseek(fd_, 0, SEEK_CUR, &err);
//   checkErr(&err);
//   return ret;
// }
//
// void DomeTunnelHandler::flush(void) throw (DmException) {
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " DomeTunnelHandler. flush (noop)");
//   // nothing
// }
//
// bool DomeTunnelHandler::eof(void) throw (DmException) {
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " DomeTunnelHandler. eof: " << lastRead_ == 0);
//   return lastRead_ == 0;
// }
//
// void DomeIOHandler::mkdirp(const std::string& path) throw (DmException) {
//   std::vector<std::string> parts = DomeUtils::split(path, "/");
//   std::ostringstream tocreate(parts[0]);
//
//   // rudimentary sanity protection: never try to create the top-level directory
//   for(std::vector<std::string>::iterator it = parts.begin()+1; it+1 != parts.end(); it++) {
//     tocreate << "/" + *it;
//
//     struct stat info;
//     if(::stat(tocreate.str().c_str(), &info) != 0) {
//       Log(Logger::Lvl1, domeadapterlogmask, domeadapterlogname, " Creating directory: " << tocreate.str());
//
//       mode_t prev = umask(0);
//       int ret = ::mkdir(tocreate.str().c_str(), 0770);
//       umask(prev);
//
//       if(ret != 0) {
//         char errbuffer[128];
//         strerror_r(errno, errbuffer, sizeof(errbuffer));
//         throw DmException(errno, "Could not create directory: %s err: %s", tocreate.str().c_str(), errbuffer);
//       }
//     }
//   }
// }
//
// DomeIOHandler::DomeIOHandler(const std::string& path,
//                            int flags, mode_t mode) throw (DmException):
//   eof_(false)
// {
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " path:" << path << ", flags: " << flags << ", mode: " << mode);
//
//   if(flags & O_CREAT) {
//     this->mkdirp(path);
//   }
//
//   this->fd_ = ::open(path.c_str(), flags, mode);
//   if (this->fd_ == -1) {
//     char errbuffer[128];
//     strerror_r(errno, errbuffer, sizeof(errbuffer));
//     throw DmException(errno, "Could not open %s err: %s", path.c_str(), errbuffer);
//   }
// }
//
//
//
// DomeIOHandler::~DomeIOHandler()
// {
//   if (this->fd_ != -1)
//     ::close(this->fd_);
// }
//
//
//
// void DomeIOHandler::close() throw (DmException)
// {
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " fd:" << this->fd_);
//   ::close(this->fd_);
//   this->fd_ = -1;
// }
//
//
// int DomeIOHandler::fileno() throw (DmException)
// {
//     Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " fd:" << this->fd_);
//     return this->fd_;
// }
//
//
// struct stat DomeIOHandler::fstat() throw (DmException)
// {
//
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " fd:" << this->fd_);
//
//   struct stat st;
//   ::fstat(this->fd_, &st);
//   return st;
// }
//
//
//
// size_t DomeIOHandler::read(char* buffer, size_t count) throw (DmException)
// {
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " fd:" << this->fd_ << " count:" << count);
//
//   ssize_t nbytes = ::read(this->fd_, buffer, count);
//   if (nbytes < 0) {
//     char errbuffer[128];
//     strerror_r(errno, errbuffer, sizeof(errbuffer));
//     throw DmException(errno, "%s on fd %s ", errbuffer, this->fd_);
//   }
//
//   eof_ = (static_cast<size_t>(nbytes) < count);
//
//   return static_cast<size_t>(nbytes);
// }
//
//
//
// size_t DomeIOHandler::write(const char* buffer, size_t count) throw (DmException)
// {
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " fd:" << this->fd_ << " count:" << count);
//
//   ssize_t nbytes = ::write(this->fd_, buffer, count);
//   if (nbytes < 0) {
//     char errbuffer[128];
//     strerror_r(errno, errbuffer, sizeof(errbuffer));
//     throw DmException(errno, "%s on fd %s ", errbuffer, this->fd_);
//   }
//
//   return static_cast<size_t>(nbytes);
// }
//
//
//
// size_t DomeIOHandler::readv(const struct iovec* vector, size_t count) throw (DmException)
// {
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " fd:" << this->fd_ << " count:" << count);
//
//   ssize_t nbytes = ::readv(this->fd_, vector, count);
//   if (nbytes < 0) {
//     char errbuffer[128];
//     strerror_r(errno, errbuffer, sizeof(errbuffer));
//     throw DmException(errno, "%s on fd %s ", errbuffer, this->fd_);
//   }
//
//   return static_cast<size_t>(nbytes);
// }
//
//
//
// size_t DomeIOHandler::writev(const struct iovec* vector, size_t count) throw (DmException)
// {
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " fd:" << this->fd_ << " count:" << count);
//
//   ssize_t nbytes = ::writev(this->fd_, vector, count);
//   if (nbytes < 0) {
//     char errbuffer[128];
//     strerror_r(errno, errbuffer, sizeof(errbuffer));
//     throw DmException(errno, "%s on fd %s ", errbuffer, this->fd_);
//   }
//
//   return static_cast<size_t>(nbytes);
// }
//
//
//
// size_t DomeIOHandler::pread(void* buffer, size_t count, off_t offset) throw (DmException)
// {
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " fd:" << this->fd_ << " count:" << count);
//
//   ssize_t nbytes = ::pread(this->fd_, buffer, count, offset);
//   if (nbytes < 0) {
//     char errbuffer[128];
//     strerror_r(errno, errbuffer, sizeof(errbuffer));
//     throw DmException(errno, "%s on fd %s ", errbuffer, this->fd_);
//   }
//
//   return static_cast<size_t>(nbytes);
// }
//
//
//
// size_t DomeIOHandler::pwrite(const void* buffer, size_t count, off_t offset) throw (DmException)
// {
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " fd:" << this->fd_ << " count:" << count);
//
//   ssize_t nbytes = ::pwrite(this->fd_, buffer, count, offset);
//   if (nbytes < 0) {
//     char errbuffer[128];
//     strerror_r(errno, errbuffer, sizeof(errbuffer));
//     throw DmException(errno, "%s on fd %s ", errbuffer, this->fd_);
//   }
//
//   return static_cast<size_t>(nbytes);
// }
//
//
//
// void DomeIOHandler::seek(off_t offset, Whence whence) throw (DmException)
// {
//   Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " fd:" << this->fd_ << " offs:" << offset);
//   if (::lseek64(this->fd_, offset, whence) == ((off_t) - 1))
//     throw DmException(errno, "Could not seek on fd %s ", this->fd_);
// }
//
//
//
// off_t DomeIOHandler::tell(void) throw (DmException)
// {
//
//   return ::lseek64(this->fd_, 0, SEEK_CUR);
// }
//
//
//
// void DomeIOHandler::flush(void) throw (DmException)
// {
//   // Nothing
// }
//
//
//
// bool DomeIOHandler::eof(void) throw (DmException)
// {
//   return eof_;
// }
