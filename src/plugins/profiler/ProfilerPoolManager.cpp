/// @file   ProfilerPoolManager.cpp
/// @brief  ProfilerPoolManager implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include "Profiler.h"
#include "XrdXrootdMonData.hh"

#include <string.h>

using namespace dmlite;



ProfilerPoolManager::ProfilerPoolManager(PoolManager* decorates) throw(DmException)
{
  this->decorated_   = decorates;

  this->decoratedId_ = new char [decorates->getImplId().size() + 1];
  strcpy(this->decoratedId_, decorates->getImplId().c_str());
}



ProfilerPoolManager::~ProfilerPoolManager()
{
  delete this->decorated_;
  delete this->decoratedId_;
}



std::string ProfilerPoolManager::getImplId() const throw ()
{
  std::string implId = "ProfilerPoolManager";
  implId += " over ";
  implId += this->decoratedId_;

  return implId;
}



void ProfilerPoolManager::setStackInstance(StackInstance* si) throw (DmException)
{
  BaseInterface::setStackInstance(this->decorated_, si);
}



void ProfilerPoolManager::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  BaseInterface::setSecurityContext(this->decorated_, ctx);
}



std::vector<Pool> ProfilerPoolManager::getPools(PoolAvailability availability) throw (DmException)
{
  PROFILE_RETURN(std::vector<Pool>, getPools, availability);
}



Pool ProfilerPoolManager::getPool(const std::string& poolname) throw (DmException)
{
  PROFILE_RETURN(Pool, getPool, poolname);
}



void ProfilerPoolManager::newPool(const Pool& pool) throw (DmException)
{
  PROFILE(newPool, pool);
}



void ProfilerPoolManager::updatePool(const Pool& pool) throw (DmException)
{
  PROFILE(updatePool, pool);
}



void ProfilerPoolManager::deletePool(const Pool& pool) throw (DmException)
{
  PROFILE(deletePool, pool);
}



Location ProfilerPoolManager::whereToRead(const std::string& path) throw (DmException)
{
  PROFILE_ASSIGN(Location, whereToRead, path);
  reportXrdRedirCmd(ret, XROOTD_MON_OPENR);

  return ret;
}



Location ProfilerPoolManager::whereToRead(ino_t inode) throw (DmException)
{
  PROFILE_RETURN(Location, whereToRead, inode);
}



Location ProfilerPoolManager::whereToWrite(const std::string& path) throw (DmException)
{
  PROFILE_ASSIGN(Location, whereToWrite, path);
  reportXrdRedirCmd(ret, XROOTD_MON_OPENW);

  return ret;
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


void ProfilerCatalog::sendUserIdentOrNOP()
{
  if (this->stack_->contains("sent_userident")) {
    return;
  }

  if (!this->stack_->contains("dictid")) {
    this->stack_->set("dictid", XrdMonitor::getDictId());
  }
  kXR_char dictid = Extensible::anyToUnsigned(this->stack_->get("dictid"));

  //XrdMonitor::sendShortUserIdent(dictid);

  XrdMonitor::sendUserIdent(dictid,
      // protocol
      this->secCtx_->user.name, // user DN
      this->secCtx_->credentials.remoteAddress, // user hostname
      // org
      // role
      this->secCtx_->groups[0].name
      // info
  );

  this->stack_->set("sent_userident", true);
}
