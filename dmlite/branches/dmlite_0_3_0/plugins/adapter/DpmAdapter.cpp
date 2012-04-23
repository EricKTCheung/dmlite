/// @file   plugins/adapter/DpmAdapter.cpp
/// @brief  DPM wrapper.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstdlib>
#include <cstring>
#include <dmlite/dm_errno.h>
#include <dmlite/common/Uris.h>
#include <dpm_api.h>
#include <serrno.h>
#include <vector>

#include "Adapter.h"
#include "DpmAdapter.h"

using namespace dmlite;



DpmAdapterCatalog::DpmAdapterCatalog(unsigned retryLimit)
  throw (DmException): NsAdapterCatalog(retryLimit)
{
  dpm_client_resetAuthorizationId();
}



DpmAdapterCatalog::~DpmAdapterCatalog()
{
  // Nothing
}



std::string DpmAdapterCatalog::getImplId() throw ()
{
  return std::string("DpmAdapter");
}



void DpmAdapterCatalog::set(const std::string& key, va_list value) throw (DmException)
{
  if (key == "SpaceToken") {
    const char* sToken = va_arg(value, const char*);
    if (sToken == 0x00)
      this->spaceToken_.clear();
    else
      this->spaceToken_ = std::string(sToken);
  }
  else
    NsAdapterCatalog::set(key, value);
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
}



Uri DpmAdapterCatalog::get(const std::string& path) throw (DmException)
{
  struct dpm_getfilereq     request;
  struct dpm_getfilestatus *statuses = 0x00;
  int                       nReplies, wait;
  char                      r_token[CA_MAXDPMTOKENLEN + 1];
  FileReplica               replica;
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

    memset(&replica, 0x00, sizeof(replica));
    strncpy(replica.url, statuses[0].turl, URI_MAX);
    replica.fileid = -1;
    replica.status = statuses[0].status;
    dpm_free_gfilest(nReplies, statuses);

    return dmlite::splitUri(replica.url);
  }
  catch (...) {
    // On exceptions, free first!
    if (statuses != 0x00)
      dpm_free_gfilest(nReplies, statuses);
    throw;
  }
}



std::string DpmAdapterCatalog::put(const std::string& path, Uri* uri) throw (DmException)
{
  struct dpm_putfilereq     reqfile;
  struct dpm_putfilestatus *statuses = 0x00;
  int                       nReplies, wait;
  char                      token[CA_MAXDPMTOKENLEN + 1];
  std::string               absolute;

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

  if (!this->spaceToken_.empty()) {
    char **space_ids;

    RETRY(dpm_getspacetoken(this->spaceToken_.c_str(), &nReplies, &space_ids),
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
    
    *uri = splitUri(statuses[0].turl);
    dpm_free_pfilest(nReplies, statuses);
    return std::string(token);
  }
  catch (...) {
    // On exceptions, free first!
    if (statuses != 0x00)
      dpm_free_pfilest(nReplies, statuses);
    throw;
  }
}



std::string DpmAdapterCatalog::put(const std::string&, Uri*, const std::string&) throw (DmException)
{
  throw DmException(DM_NOT_IMPLEMENTED, "put with guid not implemented for DpmAdapter");
}



void DpmAdapterCatalog::putStatus(const std::string& path, const std::string& token, Uri* uri) throw (DmException)
{
  struct dpm_putfilestatus *statuses;
  int                       nReplies, status;
  const char               *path_c;
  std::string               absolute;

  if (path[0] == '/')
    absolute = path;
  else
    absolute = this->cwdPath_ + "/" + path;

  path_c = absolute.c_str();

  if (dpm_getstatus_putreq((char*)token.c_str(), 1, (char**)&path_c, &nReplies, &statuses) < 0)
    ThrowExceptionFromSerrno(serrno);

  status = statuses[0].status;
  *uri = splitUri(statuses[0].turl);

  dpm_free_pfilest(nReplies, statuses);
}



void DpmAdapterCatalog::putDone(const std::string& path, const std::string& token) throw (DmException)
{
  struct dpm_filestatus *statuses;
  int                    nReplies;
  const char            *path_c;
  std::string            absolute;

  if (path[0] == '/')
    absolute = path;
  else
    absolute = this->cwdPath_ + "/" + path;

  path_c = absolute.c_str();

  RETRY(dpm_putdone((char*)token.c_str(), 1, (char**)&path_c, &nReplies, &statuses),
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



PoolMetadata* DpmAdapterPoolManager::getPoolMetadata(const Pool& pool) throw (DmException)
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
  
  for (int i = 0; i < pools.size(); ++i) {
    if (poolname == pools[i].pool_name)
      return pools[i];
  }
  
  throw DmException(DM_NO_SUCH_POOL, "Pool " + poolname + " not found");
}
