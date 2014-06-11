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
  if (this->stack_->contains("sent_userident")) {
    this->stack_->erase("sent_userident");
  }
}


void ProfilerXrdMon::sendUserIdentOrNOP()
{
  if (this->stack_->contains("sent_userident")) {
    return;
  }

  const SecurityContext *secCtx = this->stack_->getSecurityContext();
  kXR_unt32 dictid = XrdMonitor::getDictIdFromDn(secCtx->user.name);

  XrdMonitor::sendUserIdent(dictid,
      // protocol
      secCtx->user.name, // user DN
      secCtx->credentials.remoteAddress, // user hostname
      // org
      // role
      secCtx->groups[0].name
      // info
  );

  this->stack_->set("sent_userident", true);
}


void ProfilerXrdMon::reportXrdRedirCmd(const std::string &path, const int cmd_id)
{
  const SecurityContext *secCtx = this->stack_->getSecurityContext();
  kXR_unt32 dictid = XrdMonitor::getDictIdFromDn(secCtx->user.name);

  XrdMonitor::reportXrdRedirNsCmd(dictid, path, cmd_id);
}


void ProfilerXrdMon::reportXrdRedirCmd(const Location &loc, const int cmd_id)
{
  const SecurityContext *secCtx = this->stack_->getSecurityContext();
  kXR_unt32 dictid = XrdMonitor::getDictIdFromDn(secCtx->user.name);

  Url url = loc[0].url;

  XrdMonitor::reportXrdRedirCmd(dictid, url.domain, url.port, url.path, cmd_id);
}
