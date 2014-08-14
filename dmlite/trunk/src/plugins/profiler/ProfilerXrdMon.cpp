/// @file   ProfilerXrdMon.h
/// @brief  Profiler plugin.
/// @author Martin Hellmich <mhellmic@cern.ch>

#include "ProfilerXrdMon.h"

using namespace dmlite;


ProfilerXrdMon::ProfilerXrdMon(): file_closed_(false)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");
}


ProfilerXrdMon::~ProfilerXrdMon()
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");
  // we have to trust that the IDs are deleted with the stack
  // in a useful manner :)
}


void ProfilerXrdMon::sendUserIdentOrNOP()
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");
  if (!this->stack_->contains("dictid")) {
    const SecurityContext *secCtx = this->stack_->getSecurityContext();

    kXR_unt32 dictid = getDictId();

    std::string protocol = "null";
    //protocol = secCtx->credentials.mech;
    if (this->stack_->contains("protocol")) {
      boost::any proto_any = this->stack_->get("protocol");
      protocol = Extensible::anyToString(proto_any);
    }
    if (secCtx->user.name != "nobody") {
      protocol = "gsi";
    }

    //char dictid_str[21];
    //snprintf(dictid_str, 21, "%d", ntohl(dictid));
    //std::string unique_userid = std::string(dictid_str);

    XrdMonitor::sendUserIdent(dictid,
        protocol, // protocol
        getShortUserName(secCtx->user.name), // unique username
        secCtx->credentials.remoteAddress, // user hostname
        // org
        // role
        secCtx->groups[0].name,
        secCtx->user.name // info, here: user DN
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
  //const SecurityContext *secCtx = this->stack_->getSecurityContext();
  //kXR_unt32 dictid = XrdMonitor::getDictIdFromDn(secCtx->user.name);

  if (!this->stack_->contains("dictid")) {
    this->stack_->set("dictid", XrdMonitor::getDictId());
  }
  boost::any dictid_any = this->stack_->get("dictid");
  kXR_unt32 dictid = Extensible::anyToUnsigned(dictid_any);

  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Exiting. dictid = " << dictid);
  return dictid;
}


bool ProfilerXrdMon::hasDictId()
{
  return this->stack_->contains("dictid");
}


kXR_unt32 ProfilerXrdMon::getFileId()
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");
  if (!this->stack_->contains("fileid")) {
    this->stack_->set("fileid", XrdMonitor::getDictId());
  }
  boost::any fileid_any = this->stack_->get("fileid");
  kXR_unt32 fileid = Extensible::anyToUnsigned(fileid_any);

  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Exiting. fileid = " << fileid);
  return fileid;
}


void ProfilerXrdMon::rmDictId()
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");
  if (this->stack_->contains("dictid")) {
    this->stack_->erase("dictid");
  }
  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Exiting.");
}


void ProfilerXrdMon::rmFileId()
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");
  if (this->stack_->contains("fileid")) {
    this->stack_->erase("fileid");
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
