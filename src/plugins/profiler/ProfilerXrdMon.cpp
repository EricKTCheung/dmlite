/// @file   ProfilerXrdMon.h
/// @brief  Profiler plugin.
/// @author Martin Hellmich <mhellmic@cern.ch>

#include "ProfilerXrdMon.h"

using namespace dmlite;


ProfilerXrdMon::ProfilerXrdMon(): file_closed_(false), stack_(0x00),
  dictid_(0), fileid_(0), protocol_("null")
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");
}


ProfilerXrdMon::~ProfilerXrdMon()
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");
}


void ProfilerXrdMon::sendUserIdentOrNOP(std::string user_dn)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");
  if (!dictid_) {
    const SecurityContext *secCtx = getSecurityContext();

    kXR_unt32 dictid = getDictId();

    std::string protocol = getProtocol();

    std::string username;
    if (!user_dn.empty() && secCtx->user.name == "nobody") {
      username = user_dn;
    } else {
      username = secCtx->user.name;
    }

    std::string authProtocol;
    if (secCtx->user.name == "nobody") {
      authProtocol = "none";
    } else {
      authProtocol = "gsi";
    }

    XrdMonitor::sendUserIdent(dictid,
        protocol, // protocol
        authProtocol, // authentication protocol
        getShortUserName(username), // unique username
        secCtx->credentials.remoteAddress, // user hostname
        // org
        // role
        secCtx->groups[0].name,
        username // info, here: user DN
    );
  }
  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Exiting.");
}


void ProfilerXrdMon::reportXrdRedirCmd(const std::string &path, const int cmd_id)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path = " << path << ", cmd_id = " << cmd_id);
  kXR_unt32 dictid = getDictId();

  XrdMonitor::reportXrdRedirNsCmd(dictid, path, cmd_id);
  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Exiting.");
}


void ProfilerXrdMon::reportXrdRedirCmd(const Location &loc, const int cmd_id)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "loc, cmd_id " << cmd_id);
  kXR_unt32 dictid = getDictId();

  Url url = loc[0].url;

  XrdMonitor::reportXrdRedirCmd(dictid, url.domain, url.port, url.path, cmd_id);
  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Exiting.");
}


void ProfilerXrdMon::reportXrdFileOpen(const std::string &path, const long long file_size)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path = " << path << ", file_size = " << file_size);
  kXR_unt32 dictid = getDictId();
  kXR_unt32 fileid = getFileId();
  XrdMonitor::reportXrdFileOpen(dictid, fileid, path, file_size);
  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Exiting.");
}


void ProfilerXrdMon::reportXrdFileClose(const XrdXrootdMonStatXFR xfr,
                                        const XrdXrootdMonStatOPS ops,
                                        const XrdXrootdMonStatSSQ ssq,
                                        const int flags)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "xfr, ops, ssq, flags = " << flags);
  kXR_unt32 fileid = getFileId();
  XrdMonitor::reportXrdFileClose(fileid, xfr, ops, ssq, flags);
  rmFileId();
  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Exiting.");
}


void ProfilerXrdMon::reportXrdFileDiscAndFlush()
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");
  kXR_unt32 dictid = getDictId();
  XrdMonitor::reportXrdFileDisc(dictid);
  XrdMonitor::flushXrdFileStream();
  rmDictId();
  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Exiting.");
}


void ProfilerXrdMon::reportXrdFileDiscAndFlushOrNOP()
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");
  if (hasDictId()) {
    kXR_unt32 dictid = getDictId();
    XrdMonitor::reportXrdFileDisc(dictid);
    XrdMonitor::flushXrdFileStream();
    rmDictId();
  }
  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Exiting.");
}


kXR_unt32 ProfilerXrdMon::getDictId()
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");

  if (!dictid_) {
    dictid_ = XrdMonitor::getDictId();
  }

  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Exiting. dictid = " << dictid_);
  return dictid_;
}


bool ProfilerXrdMon::hasDictId()
{
  return dictid_;
}


kXR_unt32 ProfilerXrdMon::getFileId()
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");
  if (!fileid_) {
    fileid_ = XrdMonitor::getDictId();
  }

  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Exiting. fileid = " << fileid_);
  return fileid_;
}


void ProfilerXrdMon::rmDictId()
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");
  dictid_ = 0;
  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Exiting.");
}


void ProfilerXrdMon::rmFileId()
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");
  if (fileid_) {
    fileid_ = 0;
  }
  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Exiting.");
}


std::string ProfilerXrdMon::getShortUserName(const std::string &username)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "username = " << username);
  if (username[0] != '/')
    return username;

  std::string short_uname;
  size_t pos1, pos2;

  pos1 = username.find("CN");
  if (pos1 == username.npos)
    return username;

  pos2 = username.find("/CN", pos1+1);
  short_uname.assign(username, pos1, pos2-pos1);

  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Exiting. short_uname = " << short_uname);
  return short_uname;
}


void ProfilerXrdMon::fillSsqStats()
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");
  if (XrdMonitor::file_flags_ & XrdXrootdMonFileHdr::hasSSQ) {
    XrdXrootdMonDouble xval;
    xval.dreal = ssq_.read;
    ssqstats_.read.dlong = htonll(xval.dlong);
    xval.dreal = ssq_.readv;
    ssqstats_.readv.dlong = htonll(xval.dlong);
    xval.dreal = ssq_.rsegs;
    ssqstats_.rsegs.dlong = htonll(xval.dlong);
    xval.dreal = ssq_.write;
    ssqstats_.write.dlong = htonll(xval.dlong);
  }
  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Exiting.");
}


const SecurityContext* ProfilerXrdMon::getSecurityContext()
{
  if (stack_) {
    return this->stack_->getSecurityContext();
  } else {
    return &secCtx_;
  }
}


std::string ProfilerXrdMon::getProtocol()
{
  if (stack_) {
    if (stack_->contains("protocol")) {
      boost::any proto_any = this->stack_->get("protocol");
      std::string protocol = Extensible::anyToString(proto_any);
      return protocol;
    } else {
      return "null";
    }
  } else {
    return protocol_;
  }
}
