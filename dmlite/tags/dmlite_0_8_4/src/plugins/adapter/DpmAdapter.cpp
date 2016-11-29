/// @file   DpmAdapter.cpp
/// @brief  DPM wrapper.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <serrno.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dmlite/common/errno.h>
#include <dmlite/cpp/utils/urls.h>
#include <dpm_api.h>
#include <map>
#include <vector>
#include <sys/select.h>
#include <sys/time.h>

#include "Adapter.h"
#include "DpmAdapter.h"
#include "FunctionWrapper.h"

using namespace dmlite;



DpmAdapterCatalog::DpmAdapterCatalog(NsAdapterFactory* factory, unsigned retryLimit, bool hostDnIsRoot, std::string hostDn) throw (DmException):
  NsAdapterCatalog(retryLimit, hostDnIsRoot, hostDn), factory_(factory)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, " hostDn: " << hostDn);

  factory_ = factory;
  factory_->getPool()->acquire();
}



DpmAdapterCatalog::~DpmAdapterCatalog()
{
  factory_->getPool()->release(1);
}


// setDpmApiIdentity should be called by any public methods of the catalog
// which use the dpm api; this method makes sure that the dpm's api per-thread
// identity is set according to the content of the catalog's security context
//
void DpmAdapterCatalog::setDpmApiIdentity()
{
  FunctionWrapper<int> reset(dpm_client_resetAuthorizationId);
  reset();

  // can not do any more if there is no security context
  if (!secCtx_) { return; }

  uid_t uid = secCtx_->user.getUnsigned("uid");

  // nothing more to do for root
  if (uid == 0) { return; }

  FunctionWrapper<int, uid_t, gid_t, const char*, char*>(
      dpm_client_setAuthorizationId,
        uid, secCtx_->groups[0].getUnsigned("gid"), "GSI",
        (char*)secCtx_->user.name.c_str())();

  if (fqans_ && nFqans_) {
    FunctionWrapper<int, char*, char**, int>(
        dpm_client_setVOMS_data,
          fqans_[0], fqans_, nFqans_)();
  }
}




void DpmAdapterCatalog::getChecksum(const std::string& path,
                                 const std::string& csumtype,
                                 std::string& csumvalue,
                                 const std::string &pfn,
                                 const bool forcerecalc, const int waitsecs) throw (DmException) {
                                   // We can also pass a long checksum name (e.g. checksum.adler32)
                                   
                                   // Gets a checksum of the required type. Utility function
                                   // By default it is extracted from the extendedstat information.
                                   // By default we assume that the backends are not able to calcolate it on the fly.
                                   // Other backends (e.g. DOME) may  support calculating it on the fly. In this case this func will have to be specialised in a plugin
                                   
                                   Log(Logger::Lvl4, adapterlogmask, adapterlogname, "csumtype:" << csumtype << " path:" << path);
                                   
                                   setDpmApiIdentity();
                                   
                                   ExtendedStat ckx;
                                   
                                   if (!path.empty())
                                    ckx = this->extendedStat(path, true);
                                   else
                                     ckx = this->extendedStatByRFN(pfn);
                                   
                                   std::string k = csumtype;
                                   
                                   // If it looks like a legacy chksum then try to xlate its name
                                   if (csumtype.length() == 2)
                                     k = checksums::fullChecksumName(csumtype);
                                   
                                   if (!checksums::isChecksumFullName(k))
                                     throw DmException(EINVAL, "'" + csumtype + "' is not a valid checksum type.");
                                   
                                   csumvalue = ckx.getString((const std::string)k, "");
                                   
                                   if (!forcerecalc && (csumvalue.length() > 0)) return;
                                   
                                   IODriver *h;
                                   IOHandler *d;
                                   if (si_) {
                                     try {
                                       h = si_->getIODriver();
                                       d = h->createIOHandler(pfn, IODriver::kInsecure | O_RDONLY, ckx, 0);
                                       
                                       if (strcmp("checksum.md5", k.c_str()) == 0) {
                                         csumvalue = dmlite::checksums::md5(d, 0, 0);
                                       }                             
                                       else if (strcmp("checksum.adler32", k.c_str()) == 0) {
                                         csumvalue = dmlite::checksums::adler32(d, 0, 0);
                                       }
                                       else if (strcmp("checksum.crc32", k.c_str()) == 0) {
                                         csumvalue = dmlite::checksums::crc32(d, 0, 0);
                                       }
                                       else
                                         throw DmException(EINVAL, "'" + csumtype + "' is unknown.");
                                       
                                       delete d;
                                       
                                       // Now try to save the calculated value, but don't bother too much
                                       try {
                                        setChecksum(path, csumtype, csumvalue);
                                       } catch (DmException e) {};
                                       
                                       return;
                                       
                                     } catch (...) {
                                       delete d;
                                       throw;
                                     }
                                     
                                   }
                                   
                                   // If we did not find the wanted chksum in the hash, then we may want
                                   // to calculate it in a plugin that is more specialized than this one
                                   throw DmException(EINVAL, "'" + csumtype + "' cannot be calculated by the base Catalog implementation. You may want to use a more specialized plugin.");
                                   
                                   
                                   
                                 }
                                 
                                 
std::string DpmAdapterCatalog::getImplId() const throw ()
{
  return std::string("DpmAdapterCatalog");
}



void DpmAdapterCatalog::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  NsAdapterCatalog::setSecurityContext(ctx);
}



void DpmAdapterCatalog::unlink(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl0, adapterlogmask, adapterlogname, " Path: " << path);

  setDpmApiIdentity();

  int                    nReplies;
  struct dpm_filestatus *statuses;

  std::string            absolute;

  if (path[0] == '/')
    absolute = path;
  else
    absolute = this->cwdPath_ + "/" + path;

  struct stat stat = NsAdapterCatalog::extendedStat(absolute, false).stat;
  if (S_ISLNK(stat.st_mode)) {
    NsAdapterCatalog::unlink(absolute);
  }
  else {
    const char *path_c = absolute.c_str();
    FunctionWrapper<int, int, char**, int*, dpm_filestatus**>(dpm_rm, 1, (char**)&path_c, &nReplies, &statuses)(this->retryLimit_);
    dpm_free_filest(nReplies, statuses);
  }
}



DpmAdapterPoolManager::DpmAdapterPoolManager(DpmAdapterFactory* factory,
                                             unsigned retryLimit,
                                             const std::string& passwd,
                                             bool useIp,
                                             unsigned life) throw (DmException):
  si_(NULL), retryLimit_(retryLimit), tokenPasswd_(passwd), tokenUseIp_(useIp),
  tokenLife_(life), userId_(""), fqans_(NULL), nFqans_(0), factory_(factory), secCtx_(NULL)
{
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "");

  // Nothing
  factory_->getPool()->acquire();
}



DpmAdapterPoolManager::~DpmAdapterPoolManager()
{
  if (this->fqans_ != NULL) {
    for (size_t i = 0; i < this->nFqans_; ++i)
      delete [] this->fqans_[i];
    delete [] this->fqans_;
  }

  factory_->getPool()->release(1);
}


// setDpmApiIdentity should be called by any public methods of the pool manager
// which use the dpm api; this method makes sure that the dpm's api per-thread
// identity is set according to the content of the pool manager's security
// context
//
void DpmAdapterPoolManager::setDpmApiIdentity()
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "Entering");
  FunctionWrapper<int> reset(dpm_client_resetAuthorizationId);
  reset();

  // can not do any more if there is no security context
  if (!secCtx_) {
    Log(Logger::Lvl4, adapterlogmask, adapterlogname, "No security context... exiting");
    return; }

  uid_t uid = secCtx_->user.getUnsigned("uid");
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "uid=" << uid);

  // nothing more to do for root
  if (uid == 0) { return; }

  if (secCtx_->groups.size() == 0) {
    Err(adapterlogname, "No groups in the security context. Exiting.");
    return;
  }

  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "gid=" << secCtx_->groups[0].getUnsigned("gid"));

  FunctionWrapper<int, uid_t, gid_t, const char*, char*>(
      dpm_client_setAuthorizationId,
        uid, secCtx_->groups[0].getUnsigned("gid"), "GSI",
        (char*)secCtx_->user.name.c_str())();

  if (fqans_ && nFqans_) {
    Log(Logger::Lvl4, adapterlogmask, adapterlogname, "fqan=" << fqans_[0]);
    FunctionWrapper<int, char*, char**, int>(
        dpm_client_setVOMS_data,
          fqans_[0], fqans_, nFqans_)();
  }
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. uid=" << uid <<
      " gid=" << ( (secCtx_->groups.size() > 0) ? secCtx_->groups[0].getUnsigned("gid"):-1) <<
      " fqan=" << ( (fqans_ && nFqans_) ? fqans_[0]:"none") );
}


std::string DpmAdapterPoolManager::getImplId() const throw()
{
  return std::string("DpmAdapter");
}



void DpmAdapterPoolManager::setStackInstance(StackInstance* si) throw (DmException)
{
  // Nothing
  this->si_ = si;
}



void DpmAdapterPoolManager::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "Entering");

  // String => const char*
  if (this->fqans_ != NULL) {
    for (size_t i = 0; i < this->nFqans_; ++i)
      delete [] this->fqans_[i];
    delete [] this->fqans_;
  }
  this->fqans_ = NULL;
  this->nFqans_ = 0;
  this->userId_.clear();

  this->secCtx_ = ctx;


  if (!ctx) {
    Log(Logger::Lvl4, adapterlogmask, adapterlogname, "Context is null. Exiting.");
    return;
  }

  this->nFqans_ = ctx->groups.size();
  this->fqans_  = new char* [this->nFqans_];
  for (size_t i = 0; i < this->nFqans_; ++i) {
    this->fqans_[i] = new char [ctx->groups[i].name.length() + 1];
    strcpy(this->fqans_[i], ctx->groups[i].name.c_str());
    Log(Logger::Lvl4, adapterlogmask, adapterlogname, "fqans_[" << i << "]='" << this->fqans_[i] << "'");
  }

  if (this->tokenUseIp_)
    this->userId_ = ctx->credentials.remoteAddress;
  else
    this->userId_ = ctx->credentials.clientName;

  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. uid=" << this->userId_ <<
      " gid=" << ( (ctx->groups.size() > 0) ? ctx->groups[0].getUnsigned("gid"):-1) <<
      " fqan=" << ( (fqans_ && nFqans_) ? fqans_[0]:"none") );
}



std::vector<Pool> DpmAdapterPoolManager::getPools(PoolAvailability availability) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, " PoolAvailability: " << availability);

  setDpmApiIdentity();

  struct dpm_pool* dpmPools = NULL;
  int              nPools;

  try {
    // Get all pools
    FunctionWrapper<int, int*, dpm_pool**>(dpm_getpools, &nPools, &dpmPools)(this->retryLimit_);

    std::vector<Pool> pools;
    Pool              pool;

    for (int i = 0; i < nPools; ++i) {
      Log(Logger::Lvl4, adapterlogmask, adapterlogname, "dpmPools[" << i << "] name:" << dpmPools[i].poolname << " type:" << "filesystem");
      pool.name = dpmPools[i].poolname;
      pool.type = "filesystem";
      pools.push_back(pool);

    }

    free(dpmPools);

    if (availability == kAny)
      return pools;

    // A pool is available if it has at least one fs available
    struct dpm_fs    *dpm_fs;
    std::vector<Pool> filtered;

    Log(Logger::Lvl4, adapterlogmask, adapterlogname, "Filtering pools");

    for (unsigned i = 0; i < pools.size(); ++i) {
      int  nFs;
      bool anyFsAvailable;
      Log(Logger::Lvl4, adapterlogmask, adapterlogname, "Invoking dpm_getpoolfs(" << pools[i].name << ")");

      if (dpm_getpoolfs((char*)pools[i].name.c_str(), &nFs, &dpm_fs) < 0)
        ThrowExceptionFromSerrno(serrno);

      anyFsAvailable = false;
      for (int j = 0; j < nFs && !anyFsAvailable; ++j) {

        switch (availability) {
          case kForWrite: case kForBoth:
            if (dpm_fs[j].status == 0)
              anyFsAvailable = true;
            break;
          default:
            if (dpm_fs[j].status != FS_DISABLED)
              anyFsAvailable = true;
        }

      }

      // Unless kNone, anyFsAvailable means a match
      if ((availability == kNone && !anyFsAvailable) ||
          (availability != kNone && anyFsAvailable)) {
	Log(Logger::Lvl4, adapterlogmask, adapterlogname, "pool available:" << pools[i].name);
        filtered.push_back(pools[i]);
      }

      if (nFs > 0)
        free(dpm_fs);
    }
    Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Returning " << filtered.size() << " pools.");
    return filtered;
  }
  catch (...) {
    if (dpmPools != NULL)
      free(dpmPools);
    throw;
  }
}



Pool DpmAdapterPoolManager::getPool(const std::string& poolname) throw (DmException)
{
  setDpmApiIdentity();

  std::vector<Pool> pools = this->getPools();

  for (unsigned i = 0; i < pools.size(); ++i) {
    if (poolname == pools[i].name)
      return pools[i];
  }

  Err(adapterlogname, " Pool poolname: " << poolname << " not found.");
  throw DmException(DMLITE_NO_SUCH_POOL,
                    "Pool " + poolname + " not found");
}



void DpmAdapterPoolManager::newPool(const Pool&) throw (DmException)
{
  throw DmException(DMLITE_SYSERR(ENOSYS),
                    "DpmAdapterPoolManager::newPool not implemented");
}



void DpmAdapterPoolManager::updatePool(const Pool&) throw (DmException)
{
  throw DmException(DMLITE_SYSERR(ENOSYS),
                    "DpmAdapterPoolManager::updatePool not implemented");
}



void DpmAdapterPoolManager::deletePool(const Pool&) throw (DmException)
{
  throw DmException(DMLITE_SYSERR(ENOSYS),
                    "DpmAdapterPoolManager::deletePool not implemented");
}



Location DpmAdapterPoolManager::whereToRead(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, " Path: " << path);

  setDpmApiIdentity();

  struct dpm_getfilereq     request;
  struct dpm_getfilestatus *statuses = NULL;
  int                       nReplies, wait;
  char                      r_token[CA_MAXDPMTOKENLEN + 1];
  std::string               rfn;
  struct timeval            delay, tmpdel;
  int                       npoll;

  request.from_surl  = (char*)path.c_str();
  request.s_token[0] = '\0';
  request.ret_policy = '\0';
  request.flags      = 0;

  if (si_->contains("lifetime"))
    request.lifetime = Extensible::anyToLong(si_->get("lifetime"));
  else
    request.lifetime = 0;

  if (si_->contains("f_type"))
    request.f_type = Extensible::anyToString(si_->get("f_type"))[0];
  else
    request.f_type = '\0';

  try {
    // Request
    char *protocols[] = {(char*)"rfio", (char*)"file"};
    char description[] = "dmlite::adapter::whereToRead";

    FunctionWrapper<int, int, dpm_getfilereq*, int, char**, char*, time_t, char*, int*, dpm_getfilestatus**>
      (dpm_get, 1, &request, 2, protocols, description, 0, r_token, &nReplies, &statuses)(this->retryLimit_);

    if (nReplies < 1) {
      Log(Logger::Lvl1, adapterlogmask, adapterlogname, "No replicas found for: " << path);
      throw DmException(DMLITE_NO_REPLICAS, "No replicas found for " + path);
    }

    // Wait for answer
    wait = (statuses[0].status == DPM_QUEUED ||
            statuses[0].status == DPM_RUNNING ||
            statuses[0].status == DPM_ACTIVE);

    FunctionWrapper <int, char*, int, char**, int*, dpm_getfilestatus**> getReq(dpm_getstatus_getreq, r_token, 1,
                                                                                &request.from_surl, &nReplies, &statuses);

    npoll = 0;
    delay.tv_sec = 0; delay.tv_usec = 125000;
    while (wait) {
      if (npoll >= 24) {
	Err(adapterlogname, "No result from dpm for get : " << path);
        throw DmException(DMLITE_INTERNAL_ERROR, "No result from dpm for get "
          "request for " + path);
      }
      tmpdel = delay;
      select(0, 0, 0, 0, &tmpdel);
      dpm_free_gfilest(nReplies, statuses);
      statuses = 0;
      getReq();
      ++npoll;
      if (!nReplies) {
	Log(Logger::Lvl1, adapterlogmask, adapterlogname, "No replicas found for: " << path);
        throw DmException(DMLITE_NO_REPLICAS, "No replicas found for " + path);
      }

      wait = (statuses[0].status == DPM_QUEUED ||
              statuses[0].status == DPM_RUNNING ||
              statuses[0].status == DPM_ACTIVE);

      timeradd(&delay, &delay, &delay);
      if (delay.tv_sec >= 120) { delay.tv_sec = 120; delay.tv_usec = 0; }
    }

    switch(statuses[0].status & 0xF000) {
      case DPM_FAILED: case DPM_ABORTED: case DPM_EXPIRED:
	Err(adapterlogname, "No error string returned from DPM : " << path);
        throw DmException(DMLITE_SYSERR(statuses[0].status & 0x0FFF),
                          "The DPM get request failed (%s)",
                          statuses[0].errstring?statuses[0].errstring:"No error string returned from DPM");
    }

    rfn = statuses[0].turl;
    dpm_free_gfilest(nReplies, statuses);
    statuses = 0;

    // Build the answer
    Url rloc(rfn);
    rloc.path = Url::normalizePath(rloc.path);

    Chunk single;

    single.url.domain = rloc.domain;
    single.url.path   = rloc.path;

    single.offset = 0;
    single.size   = this->si_->getCatalog()->extendedStat(path).stat.st_size;

    single.url.query["token"] = dmlite::generateToken(this->userId_, rloc.path,
                                                      this->tokenPasswd_, this->tokenLife_);

    Log(Logger::Lvl3, adapterlogmask, adapterlogname, " Path: " << path << " --> " << rloc.toString());
    return Location(1, single);
  }
  catch (...) {
    // On exceptions, free first!
    if (statuses != NULL)
      dpm_free_gfilest(nReplies, statuses);
    throw;
  }
}



Location DpmAdapterPoolManager::whereToRead(ino_t) throw (DmException)
{
  throw DmException(DMLITE_SYSERR(ENOSYS),
                    "DpmAdapter: Access by inode not supported");
}



Location DpmAdapterPoolManager::whereToWrite(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, " Path: " << path);

  setDpmApiIdentity();

  struct dpm_putfilereq     reqfile;
  struct dpm_putfilestatus *statuses = NULL;
  int                       nReplies, wait;
  char                      token[CA_MAXDPMTOKENLEN + 1];
  std::string               spaceToken, userTokenDescription;
  struct timeval            delay, tmpdel;
  int                       npoll;

  mode_t createMode = 0664;
  Acl    acl;

  reqfile.to_surl        = (char*)path.c_str();
  reqfile.s_token[0]     = '\0';

  if (this->si_->contains("f_type"))
    reqfile.f_type = Extensible::anyToString(this->si_->get("f_type"))[0];
  else
    reqfile.f_type = 'P';

  if (this->si_->contains("lifetime"))
    reqfile.lifetime = Extensible::anyToLong(this->si_->get("lifetime"));
  else
    reqfile.lifetime = 0;

  if (this->si_->contains("requested_size"))
    reqfile.requested_size = Extensible::anyToU64(this->si_->get("requested_size"));
  else
    reqfile.requested_size = 0;

  if (this->si_->contains("f_lifetime"))
    reqfile.f_lifetime = Extensible::anyToLong(this->si_->get("f_lifetime"));
  else
    reqfile.f_lifetime = 0;

  if (this->si_->contains("ret_policy"))
    reqfile.ret_policy = Extensible::anyToString(this->si_->get("ret_policy"))[0];
  else
    reqfile.ret_policy     = '\0';

  if (this->si_->contains("ac_latency"))
    reqfile.ac_latency = Extensible::anyToString(this->si_->get("ac_latency"))[0];
  else
    reqfile.ac_latency     = '\0';

  if (this->si_->contains("overwrite") && Extensible::anyToBoolean(this->si_->get("overwrite"))) {
    try {
      ExtendedStat xstat = this->si_->getCatalog()->extendedStat(path);
      acl        = xstat.acl;
      createMode = xstat.stat.st_mode;
      this->si_->getCatalog()->unlink(path);
    }
    catch (DmException& e) {
      if (e.code() != ENOENT) throw;
    }
  }

  if (this->si_->contains("SpaceToken"))
    spaceToken           = Extensible::anyToString(this->si_->get("SpaceToken"));
  else if (this->si_->contains("UserSpaceTokenDescription"))
    userTokenDescription = Extensible::anyToString(this->si_->get("UserSpaceTokenDescription"));

  if (!spaceToken.empty()) {
    strncpy(reqfile.s_token, spaceToken.c_str(), sizeof(reqfile.s_token));
  }
  else if (!userTokenDescription.empty()) {
    char **space_ids = NULL;

    FunctionWrapper<int, const char*, int*, char***>
      (dpm_getspacetoken, userTokenDescription.c_str(), &nReplies, &space_ids)(this->retryLimit_);

    if (nReplies > 0)
      strncpy(reqfile.s_token, space_ids[0], sizeof(reqfile.s_token));

    for(int i = 0; i < nReplies; ++i)
      free(space_ids[i]);
    free(space_ids);

    if (nReplies == 0)
      throw DmException(EINVAL, "Could not get the space token from the token description %s",
                        userTokenDescription.c_str());
  }

  this->si_->getCatalog()->create(path, createMode);
  if (!acl.empty())
    this->si_->getCatalog()->setAcl(path, acl);

  try {
    char *protocols[] = { (char *)"rfio", (char *)"file" };
    char description[] = "dmlite::adapter::whereToWrite::put";
    FunctionWrapper<int, int, dpm_putfilereq*, int, char**, char*, int, time_t, char*, int*, dpm_putfilestatus**>
      (dpm_put, 1, &reqfile, 2, protocols, description, 1, 0, token, &nReplies, &statuses)(this->retryLimit_);

    wait = (statuses[0].status == DPM_QUEUED  ||
            statuses[0].status == DPM_RUNNING ||
            statuses[0].status == DPM_ACTIVE);

    switch(statuses[0].status & 0xF000) {
      case DPM_FAILED: case DPM_ABORTED: case DPM_EXPIRED:
	Err(adapterlogname, " No error string returned from DPM: " << path << " " <<
	  (statuses[0].errstring?statuses[0].errstring:"No error string returned from DPM")
	);
        throw DmException(DMLITE_SYSERR(statuses[0].status & 0x0FFF),
                          "The DPM put request failed (%s)",
                          statuses[0].errstring?statuses[0].errstring:"No error string returned from DPM");
    }

    FunctionWrapper <int, char*, int, char**, int*, dpm_putfilestatus**> putReq(dpm_getstatus_putreq, token, 1,
                                                                                &reqfile.to_surl, &nReplies, &statuses);
    npoll = 0;
    delay.tv_sec = 0; delay.tv_usec = 125000;
    while (wait) {
      if (npoll >= 24) {
	Err(adapterlogname, " No result from dpm for put: " << path);
       	throw DmException(DMLITE_INTERNAL_ERROR, "No result from dpm for put "
          "request for " + path);
      }
      tmpdel = delay;
      select(0, 0, 0, 0, &tmpdel);
      dpm_free_pfilest(nReplies, statuses);
      statuses = 0;
      putReq();
      ++npoll;
      if (!nReplies) {
	Err(adapterlogname, " Didn't get a destination from DPM: " << path);
        throw DmException(DMLITE_SYSERR(serrno),
                          "Didn't get a destination from DPM");
      }

      wait = (statuses[0].status == DPM_QUEUED  ||
                  statuses[0].status == DPM_RUNNING ||
                  statuses[0].status == DPM_ACTIVE);

      timeradd(&delay, &delay, &delay);
      if (delay.tv_sec >= 120) { delay.tv_sec = 120; delay.tv_usec = 0; }
    }

    switch(statuses[0].status & 0xF000) {

      case DPM_FAILED: case DPM_ABORTED: case DPM_EXPIRED:
	Err(adapterlogname, " Error: " << path << " " <<
	  (statuses[0].errstring?statuses[0].errstring:"No error string returned from DPM"));
        throw DmException(DMLITE_SYSERR(statuses[0].status & 0x0FFF),
                          "The DPM put request failed (%s)",
                          statuses[0].errstring?statuses[0].errstring:"No error string returned from DPM");
    }

    Url rloc(statuses[0].turl);
    dpm_free_pfilest(nReplies, statuses);
    statuses = 0;

    rloc.path = Url::normalizePath(rloc.path);

    Chunk single;

    single.url.domain = rloc.domain;
    single.url.path   = rloc.path;

    single.offset = 0;
    single.size   = 0;

    single.url.query["sfn"]      = path;
    single.url.query["dpmtoken"] = std::string(token);
    single.url.query["token"]    = dmlite::generateToken(this->userId_, rloc.path,
                                                         this->tokenPasswd_, this->tokenLife_, true);

    Log(Logger::Lvl3, adapterlogmask, adapterlogname, " Path: " << path << " --> " << rloc.toString());

    return Location(1, single);
  }
  catch (...) {
    Err(adapterlogname, " Exception! path:" << path << " statuses=" << statuses);
    // On exceptions, free first!
    if (statuses != NULL)
      dpm_free_pfilest(nReplies, statuses);
    throw;
  }
}



void DpmAdapterPoolManager::cancelWrite(const Location& loc) throw (DmException)
{
  Log(Logger::Lvl0, adapterlogmask, adapterlogname, " Location: " << loc.toString());
  setDpmApiIdentity();

  FunctionWrapper<int, char*>(dpm_abortreq, (char*)loc[0].url.query.getString("dpmtoken").c_str())();

  // Ignore errors, since dpm_abortreq may have removed the file already
  try {
    si_->getCatalog()->unlink(loc[0].url.query.getString("sfn").c_str());
  } catch(...) {
    // ignore
  }
}
