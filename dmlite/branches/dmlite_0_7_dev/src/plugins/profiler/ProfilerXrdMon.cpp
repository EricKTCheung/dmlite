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

}


void ProfilerXrdMon::sendUserIdentOrNOP()
{
  const SecurityContext *secCtx = this->stack_->getSecurityContext();
  std::pair<kXR_unt32, bool> id_pair = XrdMonitor::getDictIdFromDnMarkNew(secCtx->user.name);

  if (id_pair.second) {
    XrdMonitor::sendUserIdent(dictid,
        // protocol
        secCtx->user.name, // user DN
        secCtx->credentials.remoteAddress, // user hostname
        // org
        // role
        secCtx->groups[0].name
        // info
    );
  }
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
