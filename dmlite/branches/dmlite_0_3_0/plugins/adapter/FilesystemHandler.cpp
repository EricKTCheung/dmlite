/// @file   plugins/adapter/FilesystemHandler.cpp
/// @brief  Regular Filesystem pool
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include "FilesystemHandler.h"
#include "Adapter.h"

#include <dmlite/common/Uris.h>
#include <dpm/dpm_api.h>
#include <dpm/serrno.h>
#include <stdlib.h>
#include <string.h>

using namespace dmlite;



FilesystemPoolHandler::FilesystemPoolHandler(PoolManager* pm, Pool* pool):
    manager_(pm), pool_(pool)
{
  // Nothing
}



FilesystemPoolHandler::~FilesystemPoolHandler()
{
  // Nothing
}



void FilesystemPoolHandler::setSecurityContext(const SecurityContext* ctx) throw (DmException)
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



std::string FilesystemPoolHandler::getPoolType(void) throw (DmException)
{
  return this->pool_->pool_type;
}



std::string FilesystemPoolHandler::getPoolName(void) throw (DmException)
{
  return this->pool_->pool_name;
}



uint64_t FilesystemPoolHandler::getTotalSpace(void) throw (DmException)
{
  this->update();
  return this->total_;
}



uint64_t FilesystemPoolHandler::getFreeSpace(void) throw (DmException)
{
  this->update();
  return this->free_;
}



bool FilesystemPoolHandler::replicaAvailable(const std::string& sfn, const FileReplica& replica) throw (DmException)
{
  int nfs;
  struct dpm_fs *fs_array;
  
  // No API call for getting one specific FS
  if (dpm_getpoolfs((char*)replica.pool,  &nfs, &fs_array) != 0)
    ThrowExceptionFromSerrno(serrno);

  if (nfs == 0)
    throw DmException(DM_NO_SUCH_FS, "There are no filesystems inside the pool %s", replica.pool);

  bool found     = false;
  bool available = false;
  for (int i = 0; i < nfs && !found; ++i) {
    if (strcmp(replica.filesystem, fs_array[i].fs) == 0) {
      found = true;
      available = (fs_array[i].status != FS_DISABLED);
    }
  }
  free(fs_array);

  return available;
}



void FilesystemPoolHandler::update() throw (DmException)
{
  int npools;
  struct dpm_pool *pool_array;

  if (dpm_getpools(&npools, &pool_array) != 0)
    ThrowExceptionFromSerrno(serrno);

  bool found = false;
  for (int i = 0; i < npools && !found; ++i) {
    if (strcmp(pool_array[i].poolname, this->pool_->pool_name) == 0) {
      found = true;
      
      this->total_ = pool_array[i].capacity;
      this->free_  = pool_array[i].free;
    }
  }
  
  // Free
  for (int i = 0; i < npools; ++i)
    free(pool_array[i].gids);
  free(pool_array);

  // Failed?
  if (!found)
    throw DmException(DM_NO_SUCH_POOL, "Pool %s not found", this->pool_->pool_name);
}



Uri FilesystemPoolHandler::getLocation(const std::string &sfn, const FileReplica& replica) throw (DmException)
{
  return dmlite::splitUri(replica.url);
}



void FilesystemPoolHandler::remove(const std::string& sfn, const FileReplica& replica) throw (DmException)
{
  if (dpm_delreplica((char*)replica.url) != 0)
    ThrowExceptionFromSerrno(serrno);
}



std::string FilesystemPoolHandler::putLocation(const std::string& sfn, Uri* uri) throw (DmException)
{
  struct dpm_putfilereq     reqfile;
  struct dpm_putfilestatus *statuses = 0x00;
  int                       nReplies, wait;
  char                      token[CA_MAXDPMTOKENLEN + 1];

  reqfile.to_surl        = (char*)sfn.c_str();
  reqfile.f_type         = 'P';
  reqfile.lifetime       = 0;
  reqfile.requested_size = 0;
  reqfile.f_lifetime     = 0;
  reqfile.ret_policy     = '\0';
  reqfile.ac_latency     = '\0';
  reqfile.s_token[0]     = '\0';

  try {
    // 4 on overwrite allows to add additional replicas
    // This way, dmlite can take care of handling the catalog creation in advance
    // No, this is not documented, done as seen on dpm_replicate.c
    dpm_put(1, &reqfile, 1, (char*[]){ (char *)"rfio"}, (char *)"dmlite::adapter::put", 4,
            0, token, &nReplies, &statuses);;

    wait = statuses[0].status == DPM_QUEUED  ||
           statuses[0].status == DPM_RUNNING ||
           statuses[0].status == DPM_ACTIVE;

    while (wait) {
      dpm_getstatus_putreq(token, 1, &reqfile.to_surl, &nReplies, &statuses);
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



void FilesystemPoolHandler::putDone(const std::string& sfn, const std::string& token) throw (DmException)
{
  struct dpm_filestatus *statuses;
  int                    nReplies;
  const char            *path_c;
  std::string            absolute;

  dpm_putdone((char*)sfn.c_str(), 1, (char**)&path_c, &nReplies, &statuses);

  dpm_free_filest(nReplies, statuses);
}
