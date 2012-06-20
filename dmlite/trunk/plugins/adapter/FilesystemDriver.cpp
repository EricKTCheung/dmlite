/// @file   plugins/adapter/FilesystemDriver.cpp
/// @brief  Regular Filesystem pool
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <dmlite/common/Urls.h>
#include <dmlite/common/Security.h>
#include <dmlite/dmlite++.h>
#include <dpm_api.h>
#include <serrno.h>
#include <stdlib.h>
#include <string.h>

#include "FilesystemDriver.h"
#include "Adapter.h"


using namespace dmlite;



FilesystemPoolDriver::FilesystemPoolDriver(StackInstance* si, const Pool& pool,
                                           const std::string& passwd, bool useIp, unsigned life):
    secCtx_(0x00), manager_(0x00), pool_(pool), total_(0), free_(0),
    tokenPasswd_(passwd), tokenUseIp_(useIp), tokenLife_(life)
{
  this->manager_ = si->getPoolManager();
  
}



FilesystemPoolDriver::~FilesystemPoolDriver()
{
  // Nothing
}



void FilesystemPoolDriver::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  if (ctx) {
    // Call DPM API
    wrapCall(dpm_client_setAuthorizationId(ctx->getUser().uid,
                                          ctx->getGroup(0).gid,
                                          "GSI",
                                          (char*)ctx->getUser().name));

    if (ctx->groupCount() > 0)
      wrapCall(dpm_client_setVOMS_data((char*)ctx->getGroup(0).name,
                                      (char**)ctx->getCredentials().fqans,
                                      ctx->groupCount()));

    // Store
    this->secCtx_ = ctx;

    // Id mechanism
    if (this->tokenUseIp_)
      this->userId_ = this->secCtx_->getCredentials().remote_addr;
    else
      this->userId_ = this->secCtx_->getCredentials().client_name;
  }
}



std::string FilesystemPoolDriver::getPoolType(void) throw (DmException)
{
  return this->pool_.pool_type;
}



std::string FilesystemPoolDriver::getPoolName(void) throw (DmException)
{
  return this->pool_.pool_name;
}



uint64_t FilesystemPoolDriver::getTotalSpace(void) throw (DmException)
{
  this->update();
  return this->total_;
}



uint64_t FilesystemPoolDriver::getFreeSpace(void) throw (DmException)
{
  this->update();
  return this->free_;
}



bool FilesystemPoolDriver::isAvailable(bool write = true) throw (DmException)
{
  std::vector<dpm_fs> fs = this->getFilesystems(this->pool_.pool_name);
  
  for (unsigned i = 0; i < fs.size(); ++i) {
    if ((write && fs[i].status == 0) || (!write && fs[i].status != FS_DISABLED))
      return true;
  }
  
  return false;
}



bool FilesystemPoolDriver::replicaAvailable(const std::string& sfn, const FileReplica& replica) throw (DmException)
{
  // No API call for getting one specific FS
  std::vector<dpm_fs> fs = this->getFilesystems(replica.pool);
  
  for (unsigned i = 0; i < fs.size(); ++i) {
    if (strcmp(replica.filesystem, fs[i].fs) == 0) {
      return (fs[i].status != FS_DISABLED);
    }
  }

  return false;
}



void FilesystemPoolDriver::update() throw (DmException)
{
  int npools;
  struct dpm_pool *pool_array;

  if (dpm_getpools(&npools, &pool_array) != 0)
    ThrowExceptionFromSerrno(serrno);

  bool found = false;
  for (int i = 0; i < npools && !found; ++i) {
    if (strcmp(pool_array[i].poolname, this->pool_.pool_name) == 0) {
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
    throw DmException(DM_NO_SUCH_POOL, "Pool %s not found", this->pool_.pool_name);
}



Location FilesystemPoolDriver::getLocation(const std::string &sfn, const FileReplica& replica) throw (DmException)
{

  Url rloc = dmlite::splitUrl(replica.rfn);
  
  return Location(rloc.host, rloc.path, this->replicaAvailable(sfn, replica),
                  1,
                  "token",
                  dmlite::generateToken(this->userId_, rloc.path,
                                        this->tokenPasswd_, this->tokenLife_).c_str());
}



void FilesystemPoolDriver::remove(const std::string& sfn, const FileReplica& replica) throw (DmException)
{
  if (dpm_delreplica((char*)replica.rfn) != 0)
    ThrowExceptionFromSerrno(serrno);
}



Location FilesystemPoolDriver::putLocation(const std::string& sfn) throw (DmException)
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
    dpm_put(1, &reqfile, 1, (char*[]){ (char *)"rfio"}, (char *)"dmlite::adapter::put", 1,
            0, token, &nReplies, &statuses);;

    wait = statuses[0].status == DPM_QUEUED  ||
           statuses[0].status == DPM_RUNNING ||
           statuses[0].status == DPM_ACTIVE;

    while (wait) {
      if (dpm_getstatus_putreq(token, 1, &reqfile.to_surl, &nReplies, &statuses) < 0)
        ThrowExceptionFromSerrno(serrno);
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
    
    
    Url url = dmlite::splitUrl(statuses[0].turl);
    dpm_free_pfilest(nReplies, statuses);
    
    // Return the location with the token
    dmlite::normalizePath(url.path);
    return Location(url.host, url.path, true, 3,
                    "sfn", sfn.c_str(),
                    "dpmtoken", token,
                    "token", dmlite::generateToken(this->userId_,
                                                   url.path, this->tokenPasswd_, tokenLife_, true).c_str());
  }
  catch (...) {
    // On exceptions, free first!
    if (statuses != 0x00)
      dpm_free_pfilest(nReplies, statuses);
    throw;
  } 
}



void FilesystemPoolDriver::putDone(const FileReplica& replica,
                                   const std::map<std::string, std::string>& extras) throw (DmException)
{
  struct dpm_filestatus *statuses;
  int                    nReplies;
  const char            *sfn;
  const char            *token;
  std::map<std::string, std::string>::const_iterator i;
  
  // Need the sfn
  i = extras.find("sfn");
  if (i == extras.end())
    throw DmException(DM_INVALID_VALUE, "sfn not present");
  
  sfn = i->second.c_str();
  
  // Need dpm token
  i = extras.find("dpmtoken");
  if (i == extras.end())
    throw DmException(DM_INVALID_VALUE, "dpmtoken not present");
  
  token = i->second.c_str();

  // Put done
  if (dpm_putdone((char*)token, 1, (char**)&sfn, &nReplies, &statuses) < 0)
    ThrowExceptionFromSerrno(serrno);

  dpm_free_filest(nReplies, statuses);
}



std::vector<dpm_fs> FilesystemPoolDriver::getFilesystems(const std::string& poolname) throw (DmException)
{
  std::vector<dpm_fs> fsV;
  int nfs;
  struct dpm_fs* fs_array;
    
  if (dpm_getpoolfs((char*)poolname.c_str(),  &nfs, &fs_array) != 0)
    ThrowExceptionFromSerrno(serrno);

  if (nfs == 0)
    throw DmException(DM_NO_SUCH_FS, "There are no filesystems inside the pool " + poolname);

  for (int i = 0; i < nfs; ++i) {
    fsV.push_back(fs_array[i]);
  }
  free(fs_array);
  
  return fsV;
}
