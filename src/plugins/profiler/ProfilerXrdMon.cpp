/// @file   ProfilerXrdMon.h
/// @brief  Profiler plugin.
/// @author Martin Hellmich <mhellmic@cern.ch>

#include "ProfilerXrdMon.h"

using namespace dmlite;


ProfilerXrdMon::ProfilerXrdMon(): file_closed_(false)
{

}


ProfilerXrdMon::~ProfilerXrdMon()
{
  rmDictId();
  rmFileId();
}


void ProfilerXrdMon::sendUserIdentOrNOP()
{
  if (!this->stack_->contains("dictid")) {
    const SecurityContext *secCtx = this->stack_->getSecurityContext();

    kXR_unt32 dictid = getDictId();

    std::string protocol = "null";
    if (this->stack_->contains("protocol")) {
      boost::any proto_any = this->stack_->get("protocol");
      protocol = Extensible::anyToString(proto_any);
    }

    char dictid_str[21];
    snprintf(dictid_str, 21, "%d", ntohl(dictid));
    std::string unique_userid = secCtx->user.name + dictid_str;

    XrdMonitor::sendUserIdent(dictid,
        protocol, // protocol
        unique_userid, // unique username
        secCtx->credentials.remoteAddress, // user hostname
        // org
        // role
        secCtx->groups[0].name,
        secCtx->user.name // info, here: user DN
    );
  }
}


void ProfilerXrdMon::reportXrdRedirCmd(const std::string &path, const int cmd_id)
{
  kXR_unt32 dictid = getDictId();

  XrdMonitor::reportXrdRedirNsCmd(dictid, path, cmd_id);
}


void ProfilerXrdMon::reportXrdRedirCmd(const Location &loc, const int cmd_id)
{
  kXR_unt32 dictid = getDictId();

  Url url = loc[0].url;

  XrdMonitor::reportXrdRedirCmd(dictid, url.domain, url.port, url.path, cmd_id);
}


void ProfilerXrdMon::reportXrdFileOpen(const std::string &path, const long long file_size)
{
  kXR_unt32 dictid = getDictId();
  kXR_unt32 fileid = getFileId();
  XrdMonitor::reportXrdFileOpen(dictid, fileid, path, file_size);
}


void ProfilerXrdMon::reportXrdFileClose(const XrdXrootdMonStatXFR xfr, const bool forced)
{
  kXR_unt32 fileid = getFileId();
  XrdMonitor::reportXrdFileClose(fileid, xfr, forced);
  rmFileId();
}


void ProfilerXrdMon::reportXrdFileDisc()
{
  kXR_unt32 dictid = getDictId();
  XrdMonitor::reportXrdFileDisc(dictid);
  //XrdMonitor::flushXrdFileStream();
  rmDictId();
}


kXR_unt32 ProfilerXrdMon::getDictId()
{
  //const SecurityContext *secCtx = this->stack_->getSecurityContext();
  //kXR_unt32 dictid = XrdMonitor::getDictIdFromDn(secCtx->user.name);

  if (!this->stack_->contains("dictid")) {
    this->stack_->set("dictid", XrdMonitor::getDictId());
  }
  boost::any dictid_any = this->stack_->get("dictid");
  kXR_unt32 dictid = Extensible::anyToUnsigned(dictid_any);

  return dictid;
}


kXR_unt32 ProfilerXrdMon::getFileId()
{
  if (!this->stack_->contains("fileid")) {
    this->stack_->set("fileid", XrdMonitor::getDictId());
  }
  boost::any fileid_any = this->stack_->get("fileid");
  kXR_unt32 fileid = Extensible::anyToUnsigned(fileid_any);

  return fileid;
}


void ProfilerXrdMon::rmDictId()
{
  if (this->stack_->contains("dictid")) {
    this->stack_->erase("dictid");
  }
}


void ProfilerXrdMon::rmFileId()
{
  if (this->stack_->contains("fileid")) {
    this->stack_->erase("fileid");
  }
}
