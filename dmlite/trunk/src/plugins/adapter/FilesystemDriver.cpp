/// @file   FilesystemDriver.cpp
/// @brief  Regular Filesystem pool
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/utils/urls.h>
#include <dmlite/cpp/utils/security.h>
#include <dpm/dpm_api.h>
#include <errno.h>
#include <dpm/serrno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/time.h>


#include "Adapter.h"
#include "DpmAdapter.h"
#include "FilesystemDriver.h"
#include "FunctionWrapper.h"


using namespace dmlite;

FilesystemPoolDriver::FilesystemPoolDriver(const std::string& passwd, bool useIp,
                                           unsigned life, unsigned retryLimit,
                                           const std::string& adminUsername):
    secCtx_(NULL), tokenPasswd_(passwd), tokenUseIp_(useIp), tokenLife_(life),
    retryLimit_(retryLimit), fqans_(NULL), nFqans_(0), adminUsername_(adminUsername)
{
  // nothing to do
}



FilesystemPoolDriver::~FilesystemPoolDriver()
{
  if (this->fqans_ != NULL) {
    for (int i = 0; i < this->nFqans_; ++i)
      delete [] this->fqans_[i];
    delete [] this->fqans_;
  }
}

// setDpmApiIdentity should be called by any public methods of the pool driver
// or its handlers which use the dpm api; this method makes sure that the dpm's
// api per-thread identity is set according to the content of the pool driver's
// security context
//
void FilesystemPoolDriver::setDpmApiIdentity()
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

std::string FilesystemPoolDriver::getImplId() const throw ()
{
  return "FilesystemPoolDriver";
}



void FilesystemPoolDriver::setStackInstance(StackInstance* si) throw (DmException)
{
  this->si_ = si;
}



void FilesystemPoolDriver::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{

  if (this->fqans_ != NULL) {
    for (int i = 0; i < this->nFqans_; ++i)
      delete [] this->fqans_[i];
    delete [] this->fqans_;
  }
  this->fqans_ = NULL;
  this->nFqans_ = 0;
  this->userId_.clear();

  // Store
  this->secCtx_ = ctx;

  if (!ctx) { return; }

  // String => const char*
  this->nFqans_ = ctx->groups.size();
  this->fqans_  = new char* [this->nFqans_];
  for (int i = 0; i < this->nFqans_; ++i) {
    this->fqans_[i] = new char [ctx->groups[i].name.length() + 1];
    strcpy(this->fqans_[i], ctx->groups[i].name.c_str());
  }

  // Id mechanism
  if (this->tokenUseIp_)
    this->userId_ = this->secCtx_->credentials.remoteAddress;
  else
    this->userId_ = this->secCtx_->credentials.clientName;
}



PoolHandler* FilesystemPoolDriver::createPoolHandler(const std::string& poolName) throw (DmException)
{
  return new FilesystemPoolHandler(this, poolName);
}



void FilesystemPoolDriver::toBeCreated(const Pool& pool) throw (DmException)
{
  setDpmApiIdentity();

  // Need to create the pool here, or DPM won't see it!
  std::vector<boost::any> groups = pool.getVector("groups");
  struct dpm_pool dpmPool;
  std::memset(&dpmPool, 0, sizeof(dpmPool));
  
  if (groups.size() > 0) {
    dpmPool.gids = new gid_t [groups.size()];
    unsigned i;
    for (i = 0; i < groups.size(); ++i)
      dpmPool.gids[i] = Extensible::anyToUnsigned(groups[i]);
    dpmPool.nbgids = i;
  }
  else {
    dpmPool.gids = new gid_t [1];
    dpmPool.gids[0] = 0;
    dpmPool.nbgids  = 0;
  }   
  
  strncpy(dpmPool.poolname, pool.name.c_str(), sizeof(dpmPool.poolname));
  dpmPool.defsize         = pool.getLong("defsize");
  dpmPool.gc_start_thresh = pool.getLong("gc_start_thresh");
  dpmPool.gc_stop_thresh  = pool.getLong("gc_stop_thresh");
  dpmPool.def_lifetime    = pool.getLong("def_lifetime");
  dpmPool.defpintime      = pool.getLong("defpintime");
  dpmPool.max_lifetime    = pool.getLong("max_lifetime");
  dpmPool.maxpintime      = pool.getLong("maxpintime");
  dpmPool.ret_policy      = pool.getString("ret_policy")[0];
  dpmPool.s_type          = pool.getString("s_type")[0];
  
  strncpy(dpmPool.fss_policy, pool.getString("fss_policy").c_str(),
          sizeof(dpmPool.fss_policy));
  strncpy(dpmPool.gc_policy, pool.getString("gc_policy").c_str(),
          sizeof(dpmPool.gc_policy));
  strncpy(dpmPool.mig_policy, pool.getString("mig_policy").c_str(),
          sizeof(dpmPool.mig_policy));
  strncpy(dpmPool.rs_policy, pool.getString("rs_policy").c_str(),
          sizeof(dpmPool.rs_policy));

  try {
    FunctionWrapper<int, dpm_pool*>(dpm_addpool, &dpmPool)();
    delete [] dpmPool.gids;
  }
  catch (...) {
    delete [] dpmPool.gids;
    throw;
  }
  
  // Pool added, now need the filesystems
  std::vector<boost::any> filesystems = pool.getVector("filesystems");
  
  for (unsigned i = 0; i < filesystems.size(); ++i) {
    Extensible fs = boost::any_cast<Extensible>(filesystems[i]);
    
    FunctionWrapper<int, char*, char*, char*, int, int>(dpm_addfs,
                                                        dpmPool.poolname,
                                                        (char*)fs.getString("server").c_str(),
                                                        (char*)fs.getString("fs").c_str(),
                                                        fs.getLong("status"),
                                                        fs.getLong("weight"))();
  }
}



void FilesystemPoolDriver::justCreated(const Pool& pool) throw (DmException)
{
  // Nothing here
}



void FilesystemPoolDriver::update(const Pool& pool) throw (DmException)
{
  setDpmApiIdentity();

  std::vector<boost::any> groups = pool.getVector("groups");
  struct dpm_pool dpmPool;
  std::memset(&dpmPool, 0, sizeof(dpmPool));
  
  if (groups.size() > 0) {
    dpmPool.gids = new gid_t [groups.size()];
    unsigned i;
    for (i = 0; i < groups.size(); ++i)
      dpmPool.gids[i] = Extensible::anyToUnsigned(groups[i]);
    dpmPool.nbgids = i;
  }
  else {
    dpmPool.gids = new gid_t [1];
    dpmPool.gids[0] = 0;
    dpmPool.nbgids  = 0;
  }
  
  strncpy(dpmPool.poolname, pool.name.c_str(), sizeof(dpmPool.poolname));
  dpmPool.defsize         = pool.getLong("defsize");
  dpmPool.gc_start_thresh = pool.getLong("gc_start_thresh");
  dpmPool.gc_stop_thresh  = pool.getLong("gc_stop_thresh");
  dpmPool.def_lifetime    = pool.getLong("def_lifetime");
  dpmPool.defpintime      = pool.getLong("defpintime");
  dpmPool.max_lifetime    = pool.getLong("max_lifetime");
  dpmPool.maxpintime      = pool.getLong("maxpintime");
  dpmPool.ret_policy      = pool.getString("ret_policy")[0];
  dpmPool.s_type          = pool.getString("s_type")[0];
  
  strncpy(dpmPool.fss_policy, pool.getString("fss_policy").c_str(),
          sizeof(dpmPool.fss_policy));
  strncpy(dpmPool.gc_policy, pool.getString("gc_policy").c_str(),
          sizeof(dpmPool.gc_policy));
  strncpy(dpmPool.mig_policy, pool.getString("mig_policy").c_str(),
          sizeof(dpmPool.mig_policy));
  strncpy(dpmPool.rs_policy, pool.getString("rs_policy").c_str(),
          sizeof(dpmPool.rs_policy));

  try {
    FunctionWrapper<int, dpm_pool*>(dpm_modifypool, &dpmPool)();
    delete [] dpmPool.gids;
  }
  catch (...) {
    delete [] dpmPool.gids;
    throw;
  }
  
  // Pool updated, now need the filesystems
  std::vector<boost::any> filesystems = pool.getVector("filesystems");
  
  for (unsigned i = 0; i < filesystems.size(); ++i) {
    Extensible fs = boost::any_cast<Extensible>(filesystems[i]);
    
    FunctionWrapper<int, char*, char*, int, int>(dpm_modifyfs,
                                                 (char*)fs.getString("server").c_str(),
                                                 (char*)fs.getString("fs").c_str(),
                                                 fs.getLong("status"),
                                                 fs.getLong("weight"))();
  }
}



void FilesystemPoolDriver::toBeDeleted(const Pool& pool) throw (DmException)
{
  setDpmApiIdentity();

  // Delete filesystems
  int nFs;
  struct dpm_fs* dpmFs = NULL;
  
  if (dpm_getpoolfs((char*)pool.name.c_str(), &nFs, &dpmFs) == 0) {
    for (int i = 0; i < nFs; ++i)
      FunctionWrapper<int, char*, char*>(dpm_rmfs, dpmFs[i].server, dpmFs[i].fs)();
    free(dpmFs);
  }
  else if (serrno != EINVAL) {
    ThrowExceptionFromSerrno(serrno);
  }
  
  // Delete it here
  try {
    FunctionWrapper<int, char*>(dpm_rmpool, (char*)pool.name.c_str())();
  }
  catch (DmException& e) {
    if (DMLITE_ERRNO(e.code()) != ENOENT) throw;
    throw DmException(DMLITE_SYSERR(DMLITE_NO_SUCH_POOL),
                      "Pool %s not found", pool.name.c_str());
  }
}



FilesystemPoolHandler::FilesystemPoolHandler(FilesystemPoolDriver* driver, const std::string& poolName):
  driver_(driver), poolName_(poolName)
{
  // Nothing
}



FilesystemPoolHandler::~FilesystemPoolHandler()
{
  // Nothing
}



std::string FilesystemPoolHandler::getPoolType(void) throw (DmException)
{
  return "filesystem";
}



std::string FilesystemPoolHandler::getPoolName(void) throw (DmException)
{
  return this->poolName_;
}



uint64_t FilesystemPoolHandler::getTotalSpace(void) throw (DmException)
{
  driver_->setDpmApiIdentity();
  this->update();
  return this->total_;
}



uint64_t FilesystemPoolHandler::getFreeSpace(void) throw (DmException)
{
  driver_->setDpmApiIdentity();
  this->update();
  return this->free_;
}



bool FilesystemPoolHandler::poolIsAvailable(bool write = true) throw (DmException)
{
  driver_->setDpmApiIdentity();

  this->getFilesystems();
  
    {
        boost::lock_guard< boost::mutex > l(mtx);
        for (unsigned i = 0; i < dpmfs_.size(); ++i) {
            if ((write && dpmfs_[i].status == 0) || (!write && dpmfs_[i].status != FS_DISABLED))
                return true;
        }
    }
  return false;
}



bool FilesystemPoolHandler::replicaIsAvailable(const Replica& replica) throw (DmException)
{
  if (replica.status != dmlite::Replica::kAvailable)
    return false;

  driver_->setDpmApiIdentity();

  getFilesystems();

  {
  boost::lock_guard< boost::mutex> l(mtx);
  std::string filesystem = Extensible::anyToString(replica["filesystem"]);
  for (unsigned i = 0; i < dpmfs_.size(); ++i) {
    if (filesystem == dpmfs_[i].fs && replica.server == dpmfs_[i].server) {
      return (dpmfs_[i].status != FS_DISABLED);
    }
  }
  }
  return false;
}



void FilesystemPoolHandler::update() throw (DmException)
{
  int npools;
  struct dpm_pool *pool_array;

  if (dpm_getpools(&npools, &pool_array) != 0)
    ThrowExceptionFromSerrno(serrno);

  bool found = false;
  for (int i = 0; i < npools && !found; ++i) {
    if (this->poolName_ == pool_array[i].poolname) {
      found = true;
      
      this->total_ = pool_array[i].capacity;
      if (pool_array[i].free >= 0)
        this->free_  = pool_array[i].free;
      else
        this->free_ = 0;
    }
  }
  
  // Free
  for (int i = 0; i < npools; ++i)
    free(pool_array[i].gids);
  free(pool_array);

  // Failed?
  if (!found)
    throw DmException(DMLITE_SYSERR(DMLITE_NO_SUCH_POOL),
                      "Pool %s not found", this->poolName_.c_str());
}



Location FilesystemPoolHandler::whereToRead(const Replica& replica) throw (DmException)
{
  driver_->setDpmApiIdentity();

  Url rloc(replica.rfn);
  
  Chunk single;
  
  single.url.domain = rloc.domain;
  single.url.path   = rloc.path;
  single.offset = 0;
  
  try {
    single.size   = this->driver_->si_->getCatalog()->extendedStatByRFN(replica.rfn).stat.st_size;
  }
  catch (DmException& e) {
    switch (DMLITE_ERRNO(e.code())) {
      case ENOSYS: case DMLITE_NO_INODE:
        break;
      default:
        throw;
    }
    single.size = 0;
  }

  single.url.query["token"] = dmlite::generateToken(this->driver_->userId_, rloc.path,
                                                    this->driver_->tokenPasswd_,
                                                    this->driver_->tokenLife_);
  
  return Location(1, single);
}



void FilesystemPoolHandler::removeReplica(const Replica& replica) throw (DmException)
{
  driver_->setDpmApiIdentity();

  if (dpm_delreplica((char*)replica.rfn.c_str()) != 0)
    ThrowExceptionFromSerrno(serrno);
}



Location FilesystemPoolHandler::whereToWrite(const std::string& sfn) throw (DmException)
{
  driver_->setDpmApiIdentity();

  struct dpm_putfilereqx    reqfile;
  struct dpm_putfilestatus *statuses = NULL;
  int                       nReplies, wait;
  char                      token[CA_MAXDPMTOKENLEN + 1];
  std::string               spaceToken, userTokenDescription;
  struct timeval            delay, tmpdel;
  int                       npoll;
  int                       flags;

  reqfile.to_surl        = (char*)sfn.c_str();
  reqfile.s_token[0]     = '\0';
  reqfile.reserved       = 0;
  flags                  = OVERWRITE_FLAG;

  if (this->driver_->si_->contains("f_type"))
    reqfile.f_type = Extensible::anyToString(this->driver_->si_->get("f_type"))[0];
  else
    reqfile.f_type = 'P';

  if (this->driver_->si_->contains("lifetime"))
    reqfile.lifetime = Extensible::anyToLong(this->driver_->si_->get("lifetime"));
  else
    reqfile.lifetime = 0;

  if (this->driver_->si_->contains("requested_size"))
    reqfile.requested_size = Extensible::anyToU64(this->driver_->si_->get("requested_size"));
  else
    reqfile.requested_size = 0;

  if (this->driver_->si_->contains("f_lifetime"))
    reqfile.f_lifetime = Extensible::anyToLong(this->driver_->si_->get("f_lifetime"));
  else
    reqfile.f_lifetime = 0;

  if (this->driver_->si_->contains("ret_policy"))
    reqfile.ret_policy = Extensible::anyToString(this->driver_->si_->get("ret_policy"))[0];
  else
    reqfile.ret_policy     = '\0';

  if (this->driver_->si_->contains("ac_latency"))
    reqfile.ac_latency = Extensible::anyToString(this->driver_->si_->get("ac_latency"))[0];
  else
    reqfile.ac_latency     = '\0';

  if (this->driver_->si_->contains("SpaceToken"))
    spaceToken           = Extensible::anyToString(this->driver_->si_->get("SpaceToken"));
  else if (this->driver_->si_->contains("UserSpaceTokenDescription"))
    userTokenDescription = Extensible::anyToString(this->driver_->si_->get("UserSpaceTokenDescription"));

  if (!spaceToken.empty()) {
    strncpy(reqfile.s_token, spaceToken.c_str(), sizeof(reqfile.s_token));
  }
  else if (!userTokenDescription.empty()) {
    char **space_ids = NULL;

    FunctionWrapper<int, const char*, int*, char***>
      (dpm_getspacetoken, userTokenDescription.c_str(), &nReplies, &space_ids)(this->driver_->retryLimit_);
    
    if (nReplies > 0)
      strncpy(reqfile.s_token, space_ids[0], sizeof(reqfile.s_token));

    for(int i = 0; i < nReplies; ++i)
      free(space_ids[i]);
    free(space_ids);

    if (nReplies == 0)
      throw DmException(EINVAL, "Could not get the space token from the token description %s",
                        userTokenDescription.c_str());
  }

  dpm_fs fs;
  
  
  if (getFilesystems() == 0)
    throw DmException(DMLITE_SYSERR(ENODEV),
                      "There are no filesystems inside the pool " + this->poolName_);
  
  bool useParams = false;

  // choose either specific or randomly

  if (this->driver_->secCtx_->user.name == this->driver_->adminUsername_) {
    if (this->driver_->si_->contains("filesystem")) {
      boost::any any = this->driver_->si_->get("filesystem");
      std::string requestedFs = Extensible::anyToString(any);
      fs = chooseFilesystem(requestedFs);
      strncpy(reqfile.server, fs.server, sizeof(reqfile.server));
      strncpy(reqfile.pfnhint, fs.fs, sizeof(reqfile.pfnhint));
      useParams = true;
    } else if (this->driver_->si_->contains("pool")) {
      
      boost::lock_guard< boost::mutex> l(mtx);
      fs = dpmfs_[rand() % dpmfs_.size()];
      
      strncpy(reqfile.server, fs.server, sizeof(reqfile.server));
      strncpy(reqfile.pfnhint, fs.fs, sizeof(reqfile.pfnhint));
      useParams = true;
    }

    // do a replication, i.e. only create an additional replica
    if (this->driver_->si_->contains("replicate")) {
      flags = REPLICATE_FLAG;
    }
  }

  try {
    char *rfiov[] = { (char *)"rfio", (char *)"file" };

    if (useParams) {
      bool root_putdone = false;

      if (this->driver_->secCtx_->user.getUnsigned("uid") != 0) {
        root_putdone = true;
        FunctionWrapper<int> reset(dpm_client_resetAuthorizationId);
        reset();
      }

      FunctionWrapper<int, int, dpm_putfilereqx*, int, char**, char*, int,
        time_t, char*, int*, dpm_putfilestatus**>
          (dpm_putx, 1, &reqfile, 2, rfiov, (char *)"dmlite::adapter::put", flags,
           0, token, &nReplies, &statuses)(this->driver_->retryLimit_);

      if (root_putdone) {
        // Invoking this we will trigger a reset of the dpm legacy client auth info
        driver_->setDpmApiIdentity();
      }

    } else {
      FunctionWrapper<int, int, dpm_putfilereq*, int, char**, char*, int,
        time_t, char*, int*, dpm_putfilestatus**>
          (dpm_put, 1, reinterpret_cast<struct dpm_putfilereq *>(&reqfile), 2, rfiov,
           (char *)"dmlite::adapter::put", flags, 0, token, &nReplies, &statuses)(this->driver_->retryLimit_);
    }

    wait = (statuses[0].status == DPM_QUEUED  ||
            statuses[0].status == DPM_RUNNING ||
            statuses[0].status == DPM_ACTIVE);

    FunctionWrapper <int, char*, int, char**, int*, dpm_putfilestatus**> putReq(dpm_getstatus_putreq, token, 1,
                                                                                &reqfile.to_surl, &nReplies, &statuses);
    npoll = 0;
    delay.tv_sec = 0; delay.tv_usec = 125000;
    while (wait) {
      if (npoll >= 24) {
       	throw DmException(DMLITE_INTERNAL_ERROR, "No result from dpm for put "
          "request for " + sfn);
      }
      tmpdel = delay;
      select(0, 0, 0, 0, &tmpdel);
      dpm_free_pfilest(nReplies, statuses);
      statuses = 0;
      putReq();
      ++npoll;
      if (!nReplies)
        throw DmException(DMLITE_SYSERR(serrno),
                          "Didn't get a destination from DPM");

      wait = (statuses[0].status == DPM_QUEUED  ||
              statuses[0].status == DPM_RUNNING ||
              statuses[0].status == DPM_ACTIVE);

      timeradd(&delay, &delay, &delay);
      if (delay.tv_sec >= 120) { delay.tv_sec = 120; delay.tv_usec = 0; }
    }
    
    switch(statuses[0].status & 0xF000) {
      case DPM_FAILED: case DPM_ABORTED: case DPM_EXPIRED:
        throw DmException(DMLITE_SYSERR(statuses[0].status & 0x0FFF),
                          "The DPM put request failed (%s)",
                          statuses[0].errstring?statuses[0].errstring:"No error string returned from DPM");
    }
    
    Url url(statuses[0].turl);
    dpm_free_pfilest(nReplies, statuses);
    statuses = 0;
    
    // Return the location with the token
    url.path = dmlite::Url::normalizePath(url.path);
    
    Chunk single;
    
    single.url.domain = url.domain;
    single.url.path   = url.path;
    single.offset = 0;
    single.size   = 0;
    
    single.url.query["sfn"]      = sfn;
    single.url.query["dpmtoken"] = std::string(token);

    std::string userId;
    if (this->driver_->si_->contains("replicate"))
      userId = dmlite::kGenericUser;
    else
      userId = this->driver_->userId_;

    single.url.query["token"]    = dmlite::generateToken(userId,
                                                         url.path,
                                                         this->driver_->tokenPasswd_,
                                                         this->driver_->tokenLife_, true);
    return Location(1, single);
  }
  catch (...) {
    // On exceptions, free first!
    if (statuses != NULL)
      dpm_free_pfilest(nReplies, statuses);
    throw;
  }
}


dpm_fs FilesystemPoolHandler::chooseFilesystem(std::string& requestedFs) throw (DmException)
{
    dpm_fs chosenFs;
    bool fsFound = false;

    getFilesystems();

    
    {
        boost::lock_guard< boost::mutex> l(mtx);

        std::string fs;
        std::vector<dpm_fs>::const_iterator i;
        for (i = dpmfs_.begin(); i != dpmfs_.end(); ++i) {
            fs = (*i).server;
            fs += ":";
            fs += (*i).fs;
            if (fs == requestedFs) {
                chosenFs = *i;
                fsFound = true;
                break;
            }
        }
        if (!fsFound)
            throw DmException(ENOSPC, "The specified filesystem could not be selected, it must be of format <server>:<filesystem>: %s", requestedFs.c_str());

    }

    return chosenFs;
}

/*
std::string FilesystemPoolHandler::getDateString() throw ()
{
  // from stackoverflow
  // http://stackoverflow.com/questions/997946/how-to-get-current-time-and-date-in-c

  time_t now = time(0);
  struct tm tstruct;
  char buf[80];
  tstruct = *localtime(&now);
  strftime(buf, sizeof(buf), "%Y-%m-%d", &tstruct);

  return std::string(buf);
}
*/

void FilesystemPoolHandler::cancelWrite(const Location& loc) throw (DmException)
{
  driver_->setDpmApiIdentity();

  if (loc.empty())
    throw DmException(EINVAL, "Empty location");
  FunctionWrapper<int, char*>(dpm_abortreq, (char*)loc[0].url.query.getString("dpmtoken").c_str())();
}



int FilesystemPoolHandler::getFilesystems() throw (DmException)
{
  int nfs;
  struct dpm_fs* fs_array = NULL;
  
  // Don't update if the last update is too recent  
  // Also don't bother synchronizing here, we accept to be wrong sometimes
  if (dpmfs_lastupd >= time(0) - 30) return dpmfs_.size();
  
  if (dpm_getpoolfs((char*)poolName_.c_str(),  &nfs, &fs_array) != 0)
    ThrowExceptionFromSerrno(serrno);

    {
        boost::lock_guard< boost::mutex> l(mtx);
        dpmfs_.clear();
        for (int i = 0; i < nfs; ++i) {
            dpmfs_.push_back(fs_array[i]);
        }
        free(fs_array);

        // Update the last update time, this time we need sync
        
        dpmfs_lastupd = time(0);
        
    }
  return nfs;
}
