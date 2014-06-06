/// @file   ProfilerXrdMon.h
/// @brief  Profiler plugin.
/// @author Martin Hellmich <mhellmic@cern.ch>

#include "ProfilerXrdMon.h"

using namespace dmlite;


ProfilerXrdMon::~ProfilerXrdMon()
{
  if (this->stack_->contains("dictid")) {
    this->stack_->erase("dictid");
  }
  if (this->stack_->contains("sent_userident")) {
    this->stack_->erase("sent_userident");
  }
}


void ProfilerXrdMon::sendUserIdentOrNOP()
{
  if (this->stack_->contains("sent_userident")) {
    return;
  }

  if (!this->stack_->contains("dictid")) {
    this->stack_->set("dictid", XrdMonitor::getDictId());
  }
  kXR_char dictid = Extensible::anyToUnsigned(this->stack_->get("dictid"));

  //XrdMonitor::sendShortUserIdent(dictid);

  const SecurityContext *secCtx = this->stack_->getSecurityContext();

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


void ProfilerCatalog::reportXrdRedirCmd(const std::string &path, const int cmd_id)
{
  if (!this->stack_->contains("dictid")) {
    this->stack_->set("dictid", XrdMonitor::getDictId());
  }
  boost::any dictid_any = this->stack_->get("dictid");
  kXR_unt32 dictid = Extensible::anyToUnsigned(dictid_any);

  XrdMonitor::reportXrdRedirNsCmd(dictid, path, cmd_id);
}


void ProfilerCatalog::reportXrdRedirCmd(const Location &loc, const int cmd_id)
{
  if (!this->stack_->contains("dictid")) {
    this->stack_->set("dictid", XrdMonitor::getDictId());
  }
  boost::any dictid_any = this->stack_->get("dictid");
  kXR_unt32 dictid = Extensible::anyToUnsigned(dictid_any);

  Url url = loc[0].url;

  XrdMonitor::reportXrdRedirCmd(dictid, url.domain, url.port, url.path, cmd_id);
}
