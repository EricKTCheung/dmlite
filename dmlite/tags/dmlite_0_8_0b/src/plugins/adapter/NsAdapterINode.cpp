/// @file   NsAdapter.cpp
/// @brief  Adapter Plugin: Cns wrapper implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <serrno.h>
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/utils/checksums.h>
#include <dmlite/cpp/utils/urls.h>
#include <vector>
#include <dlfcn.h>
#include <syslog.h>

#include "Adapter.h"
#include "FunctionWrapper.h"
#include "NsAdapter.h"

#include "NotImplemented.h"

using namespace dmlite;

static void ns_init_routine()
{
  typedef int (*set_selectsrvr_t)(int);
  set_selectsrvr_t ns_set_srvr;

  ns_set_srvr = reinterpret_cast<set_selectsrvr_t>(reinterpret_cast<size_t>
    (dlsym(RTLD_DEFAULT,"dpns_set_selectsrvr")));

  if (ns_set_srvr) {
    /* disable nameserver hostname selection based on path */
    ns_set_srvr(1);
  }
}


NsAdapterINode::NsAdapterINode(unsigned retryLimit, bool hostDnIsRoot, std::string hostDn,
                               std::string dpnsHost)
  throw (DmException): si_(NULL), retryLimit_(retryLimit), dpnsHost_(dpnsHost),
                       fqans_(NULL), nFqans_(0), hostDnIsRoot_(hostDnIsRoot), hostDn_(hostDn),
                       secCtx_(NULL)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, " hostDn: " << hostDn);
  static pthread_once_t once_control = PTHREAD_ONCE_INIT;
  pthread_once(&once_control, ns_init_routine);
}

NsAdapterINode::~NsAdapterINode() 
{
  if (this->fqans_ != NULL) {
    for (unsigned i = 0; i < this->nFqans_; ++i)
      delete [] this->fqans_[i];
    delete [] this->fqans_;
  }
}

// setDpnsApiIdentity should be called by any public methods of the catalog
// which use the dpns api; this method makes sure that the dpns' api per-thread
// identity is set according to the content of the catalog's security context
//
void NsAdapterINode::setDpnsApiIdentity()
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "");
  
  FunctionWrapper<int> reset(dpns_client_resetAuthorizationId);
  reset();

  // can not do any more if there is no security context
  if (!secCtx_) {
    Log(Logger::Lvl4, adapterlogmask, adapterlogname, "No security context. Exiting.");
    return;
    
  }

  uid_t uid = secCtx_->user.getUnsigned("uid");
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "uid=" << uid);
  
  // nothing more to do for root
  if (uid == 0) { return; }

  FunctionWrapper<int, uid_t, gid_t, const char*, char*>(
      dpns_client_setAuthorizationId,
        uid, secCtx_->groups[0].getUnsigned("gid"), "GSI",
        (char*)secCtx_->user.name.c_str())();

  if (fqans_ && nFqans_) {
    FunctionWrapper<int, char*, char**, int>(
        dpns_client_setVOMS_data,
          fqans_[0], fqans_, nFqans_)();
  }
}


IDirectory::~IDirectory()
{
  // Nothing
}


std::string NsAdapterINode::getImplId(void) const throw ()
{
  return std::string("NsAdapterINode");
}


void NsAdapterINode::setStackInstance(StackInstance* si) throw (DmException)
{
  this->si_ = si;
}



void NsAdapterINode::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  // String => const char*
  if (this->fqans_ != NULL) {
    for (unsigned i = 0; i < this->nFqans_; ++i)
      delete [] this->fqans_[i];
    delete [] this->fqans_;
  }

  this->fqans_ = NULL;
  this->nFqans_ = 0;

  this->secCtx_ = ctx;

  if (!ctx) { return; }

  // Root is a special case
  if (ctx->user.getUnsigned("uid") != 0 && ctx->groups.empty())
      throw DmException(DMLITE_SYSERR(DMLITE_NO_SUCH_GROUP),
                        "Need at least one group");

  this->nFqans_ = ctx->groups.size();
  this->fqans_  = new char* [this->nFqans_];
  for (unsigned i = 0; i < this->nFqans_; ++i) {
    this->fqans_[i] = new char [ctx->groups[i].name.length() + 1];
    strcpy(this->fqans_[i], ctx->groups[i].name.c_str());
  }
}


void NsAdapterINode::updateExtendedAttributes(ino_t inode,
                                          const Extensible& attr) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "");
  
  setDpnsApiIdentity();

  char path[kPathMax];
  FunctionWrapper<int, char*, u_signed64, char*>(dpns_getpath, (char*) this->dpnsHost_.c_str(),
                                                 (u_signed64) inode, (char*) path)();

  Log(Logger::Lvl4, adapterlogmask, adapterlogname, this->getImplId().c_str() << "::" <<
     "updateExtendedAttributes: Full file path = " << path << " provided by " << this->dpnsHost_.c_str());
  
  std::string path_str((const char *) path);

  this->si_->getCatalog()->updateExtendedAttributes(path_str, attr);
}

NOT_IMPLEMENTED(void NsAdapterINode::begin(void) throw (DmException));
NOT_IMPLEMENTED(void NsAdapterINode::commit(void) throw (DmException));
NOT_IMPLEMENTED(void NsAdapterINode::rollback(void) throw (DmException));
NOT_IMPLEMENTED(ExtendedStat NsAdapterINode::create(const ExtendedStat&) throw (DmException));
NOT_IMPLEMENTED(void NsAdapterINode::symlink(ino_t, const std::string &) throw (DmException));
NOT_IMPLEMENTED(void NsAdapterINode::unlink(ino_t) throw (DmException));
NOT_IMPLEMENTED(void NsAdapterINode::move(ino_t, ino_t) throw (DmException));
NOT_IMPLEMENTED(void NsAdapterINode::rename(ino_t, const std::string&) throw (DmException));
NOT_IMPLEMENTED(ExtendedStat NsAdapterINode::extendedStat(ino_t) throw (DmException));
NOT_IMPLEMENTED(ExtendedStat NsAdapterINode::extendedStat(ino_t, const std::string&) throw (DmException));
NOT_IMPLEMENTED(ExtendedStat NsAdapterINode::extendedStat(const std::string&) throw (DmException));
NOT_IMPLEMENTED(SymLink NsAdapterINode::readLink(ino_t) throw (DmException));
NOT_IMPLEMENTED(void NsAdapterINode::addReplica(const Replica&) throw (DmException));
NOT_IMPLEMENTED(void NsAdapterINode::deleteReplica(const Replica&) throw (DmException));
NOT_IMPLEMENTED(Replica NsAdapterINode::getReplica(int64_t) throw (DmException));
NOT_IMPLEMENTED(Replica NsAdapterINode::getReplica(const std::string&) throw (DmException));
NOT_IMPLEMENTED(void NsAdapterINode::updateReplica(const Replica&) throw (DmException));
NOT_IMPLEMENTED(std::vector<Replica> NsAdapterINode::getReplicas(ino_t) throw (DmException));
NOT_IMPLEMENTED(void NsAdapterINode::utime(ino_t, const struct utimbuf*) throw (DmException));
NOT_IMPLEMENTED(void NsAdapterINode::setMode(ino_t, uid_t, gid_t, mode_t, const Acl&) throw (DmException));
NOT_IMPLEMENTED(void NsAdapterINode::setSize(ino_t, size_t) throw (DmException));
NOT_IMPLEMENTED(void NsAdapterINode::setChecksum(ino_t, const std::string&, const std::string&) throw (DmException));
NOT_IMPLEMENTED(std::string NsAdapterINode::getComment(ino_t) throw (DmException));
NOT_IMPLEMENTED(void NsAdapterINode::setComment(ino_t, const std::string&) throw (DmException));
NOT_IMPLEMENTED(void NsAdapterINode::deleteComment(ino_t) throw (DmException));
NOT_IMPLEMENTED(void NsAdapterINode::setGuid(ino_t, const std::string&) throw (DmException));
//NOT_IMPLEMENTED(void NsAdapterINode::updateExtendedAttributes(ino_t,const Extensible&) throw (DmException));
NOT_IMPLEMENTED(IDirectory* NsAdapterINode::openDir(ino_t) throw (DmException));
NOT_IMPLEMENTED(void NsAdapterINode::closeDir(IDirectory*) throw (DmException));
NOT_IMPLEMENTED(ExtendedStat* NsAdapterINode::readDirx(IDirectory*) throw (DmException));
NOT_IMPLEMENTED(struct dirent* NsAdapterINode::readDir (IDirectory*) throw (DmException));
