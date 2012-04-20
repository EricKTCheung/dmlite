/// @file   plugins/adapter/FilesystemHandler.cpp
/// @brief  Regular Filesystem pool
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include "FilesystemHandler.h"
#include "Adapter.h"

#include <dmlite/common/Uris.h>
#include <dpm/dpm_api.h>
#include <dpm/serrno.h>

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



Uri FilesystemPoolHandler::getPhysicalLocation(const std::string &sfn, const FileReplica& replica) throw (DmException)
{
  return dmlite::splitUri(replica.url);
}



void FilesystemPoolHandler::remove(const std::string& sfn, const FileReplica& replica) throw (DmException)
{
  int nReplies;
  struct dpm_filestatus *statuses;
  const char *path_c = sfn.c_str();
  dpm_rm(1, (char**)&path_c, &nReplies, &statuses);
  dpm_free_filest(nReplies, statuses);
}
