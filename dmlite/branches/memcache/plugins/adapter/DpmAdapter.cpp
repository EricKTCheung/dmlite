/// @file   plugins/adapter/DpmAdapter.cpp
/// @brief  DPM wrapper.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dmlite/common/dm_errno.h>
#include <dmlite/cpp/utils/dm_security.h>
#include <dmlite/cpp/utils/dm_urls.h>
#include <dpm_api.h>
#include <serrno.h>
#include <vector>
#include <map>

#include "Adapter.h"
#include "DpmAdapter.h"

using namespace dmlite;



DpmAdapterCatalog::DpmAdapterCatalog(unsigned retryLimit,
                                     const std::string& passwd, bool useIp, unsigned life) throw (DmException):
  NsAdapterCatalog(retryLimit), tokenPasswd_(passwd), tokenUseIp_(useIp), tokenLife_(life),
  userId_("")
{
  dpm_client_resetAuthorizationId();
}



DpmAdapterCatalog::~DpmAdapterCatalog()
{
  // Nothing
}



std::string DpmAdapterCatalog::getImplId() throw ()
{
  return std::string("DpmAdapterCatalog");
}



void DpmAdapterCatalog::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  NsAdapterCatalog::setSecurityContext(ctx);
  // Call DPM API
  wrapCall(dpm_client_setAuthorizationId(ctx->getUser().uid,
                                         ctx->getGroup(0).gid,
                                         "GSI",
                                         (char*)ctx->getUser().name));

  if (ctx->groupCount() > 0)
    wrapCall(dpm_client_setVOMS_data((char*)ctx->getGroup(0).name,
                                     (char**)ctx->getCredentials().fqans,
                                     ctx->groupCount()));
  
  if (this->tokenUseIp_)
    this->userId_ = ctx->getCredentials().remote_addr;
  else
    this->userId_ = ctx->getCredentials().client_name;
}



Location DpmAdapterCatalog::get(const std::string& path) throw (DmException)
{
  struct dpm_getfilereq     request;
  struct dpm_getfilestatus *statuses = 0x00;
  int                       nReplies, wait;
  char                      r_token[CA_MAXDPMTOKENLEN + 1];
  char                      rfn[URI_MAX];
  std::string               absolute;

  if (path[0] == '/')
    absolute = path;
  else
    absolute = this->cwdPath_ + "/" + path;

  request.from_surl  = (char*)absolute.c_str();
  request.lifetime   = 0;
  request.f_type     = '\0';
  request.s_token[0] = '\0';
  request.ret_policy = '\0';
  request.flags      = 0;

  try {
    // Request
    RETRY(dpm_get(1, &request, 1, (char*[]){(char *)"rfio"}, (char *)"libdm::dummy::dpm::get", 0,
                 r_token, &nReplies, &statuses), this->retryLimit_);
    if (nReplies < 1)
      throw DmException(DM_NO_REPLICAS, "No replicas found for " + path);

    // Wait for answer
    wait = statuses[0].status == DPM_QUEUED || statuses[0].status == DPM_RUNNING || statuses[0].status == DPM_ACTIVE;

    while (wait) {
      RETRY(dpm_getstatus_getreq(r_token, 1, &request.from_surl, &nReplies, &statuses), this->retryLimit_);
      if (!nReplies)
        throw DmException(DM_NO_REPLICAS, "No replicas found for " + path);

      switch(statuses[0].status) {
        case DPM_FAILED: case DPM_ABORTED: case DPM_EXPIRED:
          throw DmException(DM_QUERY_FAILED, "The DPM get request failed");
          break;
        case DPM_READY: case DPM_SUCCESS: case DPM_DONE:
          wait = 0;
          break;
      }
    }

    strncpy(rfn, statuses[0].turl, URI_MAX);
    dpm_free_gfilest(nReplies, statuses);
    
    Url rloc = dmlite::splitUrl(rfn);
    dmlite::normalizePath(rloc.path);
    return Location(rloc.host, rloc.path, true,
                    1,
                    "token",
                    dmlite::generateToken(this->userId_, rloc.path,
                                          this->tokenPasswd_, this->tokenLife_).c_str());          
  }
  catch (...) {
    // On exceptions, free first!
    if (statuses != 0x00)
      dpm_free_gfilest(nReplies, statuses);
    throw;
  }
}



Location DpmAdapterCatalog::put(const std::string& path) throw (DmException)
{
  struct dpm_putfilereq     reqfile;
  struct dpm_putfilestatus *statuses = 0x00;
  int                       nReplies, wait;
  char                      token[CA_MAXDPMTOKENLEN + 1];
  std::string               absolute;
  const char*               spaceToken;

  if (path[0] == '/')
    absolute = path;
  else
    absolute = this->cwdPath_ + "/" + path;

  reqfile.to_surl        = (char*)absolute.c_str();
  reqfile.f_type         = 'P';
  reqfile.lifetime       = 0;
  reqfile.requested_size = 0;
  reqfile.f_lifetime     = 0;
  reqfile.ret_policy     = '\0';
  reqfile.ac_latency     = '\0';
  reqfile.s_token[0]     = '\0';
  
  try {
    spaceToken = this->si_->get("SpaceToken").array.str;
  }
  catch (...) {
    spaceToken = 0x00;
  }

  if (spaceToken != 0x00 && spaceToken[0] != '\0') {
    char **space_ids;

    RETRY(dpm_getspacetoken(spaceToken, &nReplies, &space_ids),
          this->retryLimit_);
    
    strncpy(reqfile.s_token, space_ids[0], sizeof(reqfile.s_token));

    for(int i = 0; i < nReplies; ++i)
      free(space_ids[i]);
    free(space_ids);
  }

  try {
    RETRY(dpm_put(1, &reqfile, 1, (char*[]){ (char *)"rfio"}, (char *)"libdm::dummy::dpm::put", 0,
                  0, token, &nReplies, &statuses), this->retryLimit_);

    wait = statuses[0].status == DPM_QUEUED  ||
           statuses[0].status == DPM_RUNNING ||
           statuses[0].status == DPM_ACTIVE;

    while (wait) {
      RETRY(dpm_getstatus_putreq(token, 1, &reqfile.to_surl, &nReplies, &statuses),
            this->retryLimit_);
      if (!nReplies)
        throw DmException(DM_PUT_ERROR, "Didn't get a destination from DPM");

      switch(statuses[0].status) {
        case DPM_FAILED: case DPM_ABORTED: case DPM_EXPIRED:
          throw DmException(DM_QUERY_FAILED, "The DPM put request failed");
          break;
        case DPM_READY: case DPM_SUCCESS:
          wait = 0;
          break;
      }
    }
    
    Url rloc = splitUrl(statuses[0].turl);
    dpm_free_pfilest(nReplies, statuses);
    
    dmlite::normalizePath(rloc.path);
    return Location(rloc.host, rloc.path, true, 3,
                    "sfn", absolute.c_str(),
                    "dpmtoken", token,
                    "token", dmlite::generateToken(this->userId_, rloc.path,
                                                   this->tokenPasswd_, this->tokenLife_, true).c_str());
  }
  catch (...) {
    // On exceptions, free first!
    if (statuses != 0x00)
      dpm_free_pfilest(nReplies, statuses);
    throw;
  }
}



Location DpmAdapterCatalog::put(const std::string&, const std::string&) throw (DmException)
{
  throw DmException(DM_NOT_IMPLEMENTED, "put with guid not implemented for DpmAdapter");
}



void DpmAdapterCatalog::putDone(const std::string& host, const std::string& rfn,
                                const std::map<std::string, std::string>& extras) throw (DmException)
{
  struct dpm_filestatus *statuses;
  int                    nReplies;
  const char            *sfn;
  std::string            absolute;
  std::map<std::string, std::string>::const_iterator i;
  
  // Need the sfn
  i = extras.find("sfn");
  if (i == extras.end())
    throw DmException(DM_INVALID_VALUE, "sfn not specified");
  sfn = i->second.c_str();

  // Need the dpm token
  const char* token;
  i = extras.find("dpmtoken");
  if (i == extras.end())
    throw DmException(DM_INVALID_VALUE, "dpmtoken not specified");
  
  token = i->second.c_str();

  RETRY(dpm_putdone((char*)token, 1, (char**)&sfn, &nReplies, &statuses),
        this->retryLimit_);

  dpm_free_filest(nReplies, statuses);
}



void DpmAdapterCatalog::unlink(const std::string& path) throw (DmException)
{
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
    RETRY(dpm_rm(1, (char**)&path_c, &nReplies, &statuses), this->retryLimit_);
    dpm_free_filest(nReplies, statuses);
  }
}



DpmAdapterPoolManager::DpmAdapterPoolManager(unsigned retryLimit) throw (DmException):
          retryLimit_(retryLimit)
{
  // Nothing
}



DpmAdapterPoolManager::~DpmAdapterPoolManager()
{
  // Nothing
}



std::string DpmAdapterPoolManager::getImplId() throw()
{
  return std::string("DpmAdapter");
}



void DpmAdapterPoolManager::setStackInstance(StackInstance* si) throw (DmException)
{
  // Nothing
}



void DpmAdapterPoolManager::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  // Call DPM API
  wrapCall(dpm_client_setAuthorizationId(ctx->getUser().uid,
                                         ctx->getGroup(0).gid,
                                         "GSI",
                                         (char*)ctx->getUser().name));

  if (ctx->groupCount() > 0)
    wrapCall(dpm_client_setVOMS_data((char*)ctx->getGroup(0).name,
                                     (char**)ctx->getCredentials().fqans,
                                     ctx->groupCount()));
}



PoolMetadata* DpmAdapterPoolManager::getPoolMetadata(const std::string&) throw (DmException)
{
  throw DmException(DM_NOT_IMPLEMENTED, "DpmAdapterPoolManager does not support pluggable pool types");
}



std::vector<Pool> DpmAdapterPoolManager::getPools(void) throw (DmException)
{
  struct dpm_pool* dpmPools = 0x00;
  int              nPools;

  RETRY(dpm_getpools(&nPools, &dpmPools), this->retryLimit_);

  try {
    std::vector<Pool> pools;
    Pool              pool;

    for (int i = 0; i < nPools; ++i) {
      strncpy(pool.pool_name, dpmPools[i].poolname, POOL_MAX);
      strcpy(pool.pool_type, "filesystem");
      pools.push_back(pool);
    }
    
    free(dpmPools);
    return pools;
  }
  catch (...) {
    if (dpmPools != 0x00)
      free(dpmPools);
    throw;
  }
}



Pool DpmAdapterPoolManager::getPool(const std::string& poolname) throw (DmException)
{
  std::vector<Pool> pools = this->getPools();
  
  for (unsigned i = 0; i < pools.size(); ++i) {
    if (poolname == pools[i].pool_name)
      return pools[i];
  }
  
  throw DmException(DM_NO_SUCH_POOL, "Pool " + poolname + " not found");
}



std::vector<Pool> DpmAdapterPoolManager::getAvailablePools(bool write) throw (DmException)
{
  std::vector<Pool> pools = this->getPools();
  std::vector<Pool> available;
  int nFs;
  struct dpm_fs *dpm_fs;
  
  // A pool is available if it has at least one fs available
  for (unsigned i = 0; i < pools.size(); ++i) {
    if (dpm_getpoolfs(pools[i].pool_name, &nFs, &dpm_fs) < 0)
      ThrowExceptionFromSerrno(serrno);
    
    for (int j = 0; j < nFs; ++j) {
      if ((write && dpm_fs[j].status == 0) || (!write && dpm_fs[i].status != FS_DISABLED))
        available.push_back(pools[i]);
    }
    
    free(dpm_fs);
  }
  
  return available;
}
