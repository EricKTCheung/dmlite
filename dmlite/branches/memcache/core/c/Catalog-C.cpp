/// @file   core/Catalog-C.cpp
/// @brief  C wrapper for dmlite::Catalog.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <cstring>
#include <dmlite/c/dmlite.h>
#include <dmlite/c/dm_catalog.h>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/dm_exceptions.h>
#include <dmlite/cpp/utils/dm_security.h>
#include <dmlite/cpp/utils/dm_urls.h>
#include <set>
#include <vector>

#include "Private.h"



int dm_set(dm_context *context, const char *k, union value v)
{
  TRY(context, set)
  NOT_NULL(k);
  context->stack->set(k, v);
  CATCH(context, set)
}



int dm_chdir(dm_context* context, const char* path)
{
  TRY(context, chdir)
  NOT_NULL(path);
  context->stack->getCatalog()->changeDir(path);
  CATCH(context, chdir)
}



char* dm_getcwd(dm_context* context, char* buffer, size_t size)
{
  TRY(context, getcwd)
  std::string wd = context->stack->getCatalog()->getWorkingDir();
  if (buffer != NULL)
    return strncpy(buffer, wd.c_str(), size);
  else
    return strdup(wd.c_str());
  CATCH_POINTER(context, getcwd)
}



int dm_stat(dm_context *context, const char* path, struct stat* buf)
{
  TRY(context, stat)
  NOT_NULL(path);
  *buf = context->stack->getCatalog()->extendedStat(path).stat;
  CATCH(context, stat)
}



int dm_lstat(dm_context* context, const char* path, struct stat* buf)
{
  TRY(context, lstat)
  NOT_NULL(path);
  NOT_NULL(buf);
  *buf = context->stack->getCatalog()->extendedStat(path, false).stat;
  CATCH(context, lstat);
}



int dm_xstat(dm_context* context, const char* path, struct xstat* buf)
{
  TRY(context, lstat)
  NOT_NULL(path);
  NOT_NULL(buf);
  *buf = context->stack->getCatalog()->extendedStat(path);
  CATCH(context, lstat);
}



int dm_istat(dm_context* context, ino_t inode, struct stat* buf)
{
  TRY(context, lstat)
  NOT_NULL(buf);
  *buf = context->stack->getINode()->extendedStat(inode).stat;
  CATCH(context, lstat);
}



int dm_ixstat(dm_context* context, ino_t inode, struct xstat* buf)
{
  TRY(context, ixstat)
  NOT_NULL(buf);
  *buf = context->stack->getINode()->extendedStat(inode);
  CATCH(context, ixstat);
}



int dm_addreplica(dm_context* context, const char* guid, int64_t id,
                  const char* server, const char* surl, const char status,
                  const char fileType, const char* poolName,
                  const char* fileSystem)
{
  TRY(context, addReplica)
  NOT_NULL(surl);
  context->stack->getCatalog()->addReplica(SAFE_STRING(guid), id, SAFE_STRING(server),
                               surl, status, fileType,
                               SAFE_STRING(poolName), SAFE_STRING(fileSystem));
  CATCH(context, addReplica)
}



int dm_delreplica(dm_context* context, const char* guid, int64_t id,
                  const char* surl)
{
  TRY(context, delreplica)
  NOT_NULL(surl);
  context->stack->getCatalog()->deleteReplica(SAFE_STRING(guid), id, surl);
  CATCH(context, delreplica)
}



int dm_getreplicas(dm_context* context, const char* path, int *nReplicas,
                  struct filereplica** fileReplicas)
{
  TRY(context, getreplicas)
  NOT_NULL(path);
  NOT_NULL(nReplicas);
  NOT_NULL(fileReplicas);

  std::vector<filereplica> replicaSet = context->stack->getCatalog()->getReplicas(path);

  *fileReplicas = new filereplica[replicaSet.size()];
  *nReplicas = replicaSet.size();

  std::copy(replicaSet.begin(), replicaSet.end(), *fileReplicas);

  CATCH(context, getreplicas)
}



int dm_freereplicas(dm_context* context, int nReplicas, struct filereplica* fileReplicas)
{
  delete [] fileReplicas;
  return 0;
}



int dm_freelocation(dm_context* context, struct location* loc)
{
  Location *locp = (Location*)loc;
  delete locp;
  return 0;
}



int dm_get(dm_context* context, const char* path, struct location** loc)
{
  TRY(context, get)
  NOT_NULL(path);
  NOT_NULL(loc);
  Location *locp = new Location(context->stack->getCatalog()->get(path));
  *loc = locp;
  CATCH(context, get)
}



int dm_getlocation(dm_context* context, const FileReplica* replica, struct location** loc)
{
  TRY(context, getlocation)
  NOT_NULL(replica);
  
  Pool pool = context->stack->getPoolManager()->getPool(replica->pool);
  dmlite::PoolDriver*  driver = context->stack->getPoolDriver(pool.pool_type);
  dmlite::PoolHandler* handler = driver->createPoolHandler(pool.pool_name);
  
  try {
    Location *locp = new Location(handler->getLocation(*replica));
    *loc = locp;
    delete handler;
  }
  catch (...) {
    delete handler;
    throw;
  }
  
  CATCH(context, getlocation)
}



int dm_create(dm_context* context, const char* path, mode_t mode)
{
  TRY(context, create)
  NOT_NULL(path);
  context->stack->getCatalog()->create(path, mode);
  CATCH(context, create)
}



int dm_put(dm_context* context, const char* path, struct location** loc)
{
  TRY(context, put)
  NOT_NULL(path);
  NOT_NULL(loc);
  Location *locp = new Location(context->stack->getCatalog()->put(path));
  *loc = locp;
  CATCH(context, put)
}



int dm_putg(dm_context* context, const char* path, struct location** loc, const char* guid)
{
  TRY(context, putg)
  NOT_NULL(path);
  NOT_NULL(loc);
  Location *locp;
  if (guid != NULL)
    locp = new Location(context->stack->getCatalog()->put(path, guid));
  else
    locp = new Location(context->stack->getCatalog()->put(path));
  *loc = locp;
  CATCH(context, putg)
}



int dm_putdone(dm_context* context, const char* host, const char* rfn, unsigned nextras, struct keyvalue* extrasp)
{
  TRY(context, putdone)
  NOT_NULL(host);
  NOT_NULL(rfn);
  
  std::map<std::string, std::string> extras;
  
  for (unsigned i = 0; i < nextras; ++i)
    extras.insert(std::pair<std::string, std::string>(extrasp[i].key, extrasp[i].value));
  
  context->stack->getCatalog()->putDone(host, rfn, extras);
  CATCH(context, putdone)
}



int dm_unlink(dm_context* context, const char* path)
{
  TRY(context, unlink)
  NOT_NULL(path);
  context->stack->getCatalog()->unlink(path);
  CATCH(context, unlink)
}



int dm_chmod(dm_context* context, const char* path, mode_t mode)
{
  TRY(context, chmod)
  NOT_NULL(path);
  context->stack->getCatalog()->changeMode(path, mode);
  CATCH(context, chmod)
}



int dm_chown(dm_context* context, const char* path, uid_t newUid, gid_t newGid)
{
  TRY(context, chown)
  NOT_NULL(path);
  context->stack->getCatalog()->changeOwner(path, newUid, newGid);
  CATCH(context, chown)
}



int dm_lchown(dm_context* context, const char* path, uid_t newUid, gid_t newGid)
{
  TRY(context, lchown)
  NOT_NULL(path);
  context->stack->getCatalog()->changeOwner(path, newUid, newGid, false);
  CATCH(context, lchown)
}



int dm_setacl(dm_context* context, const char* path, int nEntries, struct dm_acl* acl)
{
  TRY(context, setacl)
  NOT_NULL(path);
  NOT_NULL(acl);

  std::vector<Acl> aclV(nEntries);
  for (int i = 0; i < nEntries; ++i)
    aclV.push_back(acl[i]);

  context->stack->getCatalog()->setAcl(path, aclV);

  CATCH(context, setacl)
}



int dm_utime(dm_context* context, const char* path, const struct utimbuf* buf)
{
  TRY(context, utime)
  NOT_NULL(path);
  context->stack->getCatalog()->utime(path, buf);
  CATCH(context, utime)
}



int dm_getcomment(dm_context* context, const char* path, char* comment)
{
  TRY(context, getcomment)
  NOT_NULL(path);
  NOT_NULL(comment);
  std::string c = context->stack->getCatalog()->getComment(path);
  strcpy(comment, c.c_str());
  CATCH(context, getcomment)
}



int dm_setcomment(dm_context* context, const char* path, const char* comment)
{
  TRY(context, setcomment)
  NOT_NULL(path);
  NOT_NULL(comment);
  context->stack->getCatalog()->setComment(path, comment);
  CATCH(context, setcomment)
}




int dm_getgrpbygid(dm_context* context, gid_t gid, char* groupName)
{
  TRY(context, getgrpbygid)
  NOT_NULL(groupName);
  groupinfo group = context->stack->getUserGroupDb()->getGroup(groupName);
  strcpy(groupName, group.name);
  CATCH(context, getgrpbygid)
}



int dm_getgrpbynam(dm_context* context, const char* groupName, gid_t* gid)
{
  TRY(context, getgrpbynam)
  NOT_NULL(groupName);
  NOT_NULL(gid);
  groupinfo group = context->stack->getUserGroupDb()->getGroup(groupName);
  *gid = group.gid;
  CATCH(context, getgrpbynam)
}



int dm_getusrbynam(dm_context* context, const char* userName, uid_t* uid)
{
  TRY(context, getusrbyuid)
  NOT_NULL(userName);
  userinfo user = context->stack->getUserGroupDb()->getUser(userName);
  *uid = user.uid;
  CATCH(context, getusrbyuid)
}



int dm_getusrbyuid(dm_context* context, uid_t uid, char* userName)
{
  TRY(context, getusrbyuid)
  NOT_NULL(userName);
  userinfo user = context->stack->getUserGroupDb()->getUser(uid);
  strcpy(userName, user.name);
  CATCH(context, getusrbyuid)
}



void* dm_opendir(dm_context* context, const char* path)
{
  TRY(context, opendir)
  NOT_NULL(path);
  return context->stack->getCatalog()->openDir(path);
  CATCH_POINTER(context, opendir)
}



int dm_closedir(dm_context* context, void* dir)
{
  TRY(context, closedir)
  NOT_NULL(dir);
  context->stack->getCatalog()->closeDir(dir);
  CATCH(context, closedir)
}



struct dirent* dm_readdir(dm_context* context, void* dir)
{
  TRY(context, readdir)
  NOT_NULL(dir);
  return context->stack->getCatalog()->readDir(dir);
  CATCH_POINTER(context, readdir)
}



struct xstat* dm_readdirx(dm_context* context, void* dir)
{
  TRY(context, readdirx)
  NOT_NULL(dir);
  return context->stack->getCatalog()->readDirx(dir);
  CATCH_POINTER(context, readdirx)
}



int dm_mkdir(dm_context* context, const char* path, mode_t mode)
{
  TRY(context, mkdir)
  NOT_NULL(path);
  context->stack->getCatalog()->makeDir(path, mode);
  CATCH(context, mkdir)
}



int dm_rename(dm_context* context, const char* oldPath, const char* newPath)
{
  TRY(context, rename)
  NOT_NULL(oldPath);
  NOT_NULL(newPath);
  context->stack->getCatalog()->rename(oldPath, newPath);
  CATCH(context, rename)
}



int dm_rmdir(dm_context* context, const char* path)
{
  TRY(context, rmdir)
  NOT_NULL(path);
  context->stack->getCatalog()->removeDir(path);
  CATCH(context, rmdir)
}



int dm_replica_setltime(dm_context* context, const char* replica, time_t ltime)
{
  TRY(context, setltime)
  NOT_NULL(replica);
  context->stack->getCatalog()->replicaSetLifeTime(replica, ltime);
  CATCH(context, setltime);
}



int dm_replica_setatime(dm_context* context, const char* replica)
{
  TRY(context, setatime)
  NOT_NULL(replica);
  context->stack->getCatalog()->replicaSetAccessTime(replica);
  CATCH(context, setatime);
}



int dm_replica_settype(dm_context* context, const char* replica, char ftype)
{
  TRY(context, settype)
  NOT_NULL(replica);
  context->stack->getCatalog()->replicaSetType(replica, ftype);
  CATCH(context, settype);
}



int dm_replica_setstatus(dm_context* context, const char* replica, char status)
{
  TRY(context, setstatus)
  NOT_NULL(replica);
  context->stack->getCatalog()->replicaSetStatus(replica, status);
  CATCH(context, setlstatus);
}



int dm_errno(dm_context* context)
{
  if (context == NULL)
    return DM_NULL_POINTER;
  else
    return context->errorCode;
}



const char* dm_error(dm_context* context)
{
  if (context == NULL)
    return "The context is a NULL pointer";
  return context->errorString.c_str();
}
