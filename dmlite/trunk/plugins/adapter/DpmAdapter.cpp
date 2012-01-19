/// @file   plugins/adapter/DpmAdapter.cpp
/// @brief  DPM wrapper.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstdlib>
#include <cstring>
#include <dmlite/dm_errno.h>
#include <dpm_api.h>
#include <serrno.h>
#include <vector>

#include "Adapter.h"
#include "DpmAdapter.h"
#include "../common/Uris.h"

using namespace dmlite;



DpmAdapterCatalog::DpmAdapterCatalog(const std::string& dpmHost, unsigned retryLimit)
  throw (DmException): NsAdapterCatalog(dpmHost, retryLimit)
{
  const char *envDpm;

  if (dpmHost.empty() || dpmHost[0] == '\0') {
    envDpm = getenv("DPM_HOST");
    if (envDpm)
      this->dpmHost_ = std::string(envDpm);
    else
      this->dpmHost_ = std::string("localhost");
  }
  else {
    this->dpmHost_ = dpmHost;
  }

  setenv("DPMHOST",   this->dpmHost_.c_str(), 1);
  setenv("CSEC_MECH", "ID", 1);

  dpm_client_resetAuthorizationId();
}



DpmAdapterCatalog::~DpmAdapterCatalog()
{
  // Nothing
}



std::string DpmAdapterCatalog::getImplId()
{
  return std::string("DpmAdapterCatalog");
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



FileReplica DpmAdapterCatalog::get(const std::string& path) throw (DmException)
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
   RETRY(dpm_get(1, &request, 1, (char*[]){"rfio"}, "libdm::dummy::dpm::get", 0,
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

    replica.location = splitUri(statuses[0].turl);
    strncpy(replica.unparsed_location, statuses[0].turl, URI_MAX);
    replica.fileid = -1;
    replica.status = statuses[0].status;
    dpm_free_gfilest(nReplies, statuses);

    return replica;
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
    RETRY(dpm_put(1, &reqfile, 1, (char*[]){"rfio"}, "libdm::dummy::dpm::put", 0,
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
  const char            *path_c;
  std::string            absolute;

  if (path[0] == '/')
    absolute = path;
  else
    absolute = this->cwdPath_ + "/" + path;

  path_c = absolute.c_str();

  RETRY(dpm_rm(1, (char**)&path_c, &nReplies, &statuses), this->retryLimit_);
  dpm_free_filest(nReplies, statuses);
}



void DpmAdapterCatalog::setUserId(uid_t uid, gid_t gid, const std::string& dn) throw (DmException)
{
  NsAdapterCatalog::setUserId(uid, gid, dn);
  RETRY(dpm_client_setAuthorizationId(uid, gid, "GSI", (char*)dn.c_str()),
        this->retryLimit_);
}



void DpmAdapterCatalog::setVomsData(const std::string& vo, const std::vector<std::string>& fqans) throw (DmException)
{
  NsAdapterCatalog::setVomsData(vo, fqans);
  RETRY(dpm_client_setVOMS_data((char*)this->vo_, this->fqans_, this->nFqans_),
        this->retryLimit_);
}



std::vector<Pool> DpmAdapterCatalog::getPools(void) throw (DmException)
{
  struct dpm_pool* dpmPools = 0x00;
  int              nPools;

  RETRY(dpm_getpools(&nPools, &dpmPools), this->retryLimit_);

  try {
    std::vector<Pool> pools;
    Pool              pool;

    for (int i = 0; i < nPools; ++i) {
      // Do a memcpy
      // This will work as the structures match
      // but if they change, beware!
      memcpy(&pool, &dpmPools[i], sizeof(Pool));
      pool.free = dpmPools[i].free;

      // Copy gids! (Array)
      pool.gids = new gid_t[dpmPools[i].nbgids];
      memcpy(pool.gids, dpmPools[i].gids, sizeof(gid_t) * dpmPools[i].nbgids);

      // Fill the filesystems
      std::vector<FileSystem> fss = this->getPoolFilesystems(pool.poolname);
      pool.nbelem = fss.size();
      pool.elemp = new filesystem[pool.nbelem];
      for (int i = 0; i < pool.nbelem; ++i) {
        pool.elemp[i] = fss[i];
      }

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



std::vector<FileSystem> DpmAdapterCatalog::getPoolFilesystems(const std::string& poolname) throw (DmException)
{
  struct dpm_fs*          dpmFs;
  int                     nFs;
  std::vector<FileSystem> filesystems;
  FileSystem              fs;
  
  RETRY(dpm_getpoolfs((char*)poolname.c_str(), &nFs, &dpmFs), this->retryLimit_);

  for (int i = 0; i < nFs; ++i) {
    fs.capacity = dpmFs[i].capacity;
    fs.free     = dpmFs[i].free;
    fs.status   = dpmFs[i].status;

    strcpy(fs.fs,       dpmFs[i].fs);
    strcpy(fs.poolname, dpmFs[i].poolname);
    strcpy(fs.server,   dpmFs[i].server);

    filesystems.push_back(fs);
  }

  free(dpmFs);
  return filesystems;
}
