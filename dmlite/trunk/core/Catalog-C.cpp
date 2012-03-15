/// @file   core/Catalog-C.cpp
/// @brief  C wrapper for dmlite::Catalog.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <cstdarg>
#include <cstring>
#include <dmlite/dmlite++.h>
#include <dmlite/dmlite.h>
#include <dmlite/dm_exceptions.h>
#include <set>
#include <vector>

#include "Private.h"
#include "dmlite/common/Uris.h"
#include "dmlite/common/Security.h"



/// Open the try statement.
#define TRY(context, method)\
try {\
  context->errorCode = DM_NO_ERROR;\
  context->errorString.clear();

/// Catch block.
#define CATCH(context, method)\
  return 0;\
} catch (dmlite::DmException e) {\
  context->errorCode   = e.code();\
  context->errorString = e.what();\
  return e.code();\
} catch (...) {\
  context->errorCode   = DM_UNEXPECTED_EXCEPTION;\
  context->errorString = "An unexpected exception was thrown while executing "#method;\
  return DM_UNEXPECTED_EXCEPTION;\
}



/// Catch block for functions that return a pointer.
#define CATCH_POINTER(context, method)\
} catch (dmlite::DmException e) {\
  context->errorCode   = e.code();\
  context->errorString = e.what();\
  return NULL;\
} catch (...) {\
  context->errorCode   = DM_UNEXPECTED_EXCEPTION;\
  context->errorString = "An unexpected exception was thrown while executing "#method;\
  return NULL;\
}



/// Safe strings (empty instead of NULL)
#define SAFE_STRING(s) s==NULL?"":s


/// Throw an exception if the string is NULL
#define NOT_NULL(s) if (s==NULL) throw dmlite::DmException(DM_NULL_POINTER, #s" is a NULL pointer")


int dm_set(dm_context *context, const char *key, ...)
{
  va_list vargs;

  TRY(context, set)
  NOT_NULL(key);
  va_start(vargs, key);
  context->catalog->set(key, vargs);
  va_end(vargs);
  CATCH(context, set)
}



int dm_chdir(dm_context* context, const char* path)
{
  TRY(context, chdir)
  NOT_NULL(path);
  context->catalog->changeDir(path);
  CATCH(context, chdir)
}



char* dm_getcwd(dm_context* context, char* buffer, size_t size)
{
  TRY(context, getcwd)
  std::string wd = context->catalog->getWorkingDir();
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
  *buf = context->catalog->stat(path);
  CATCH(context, stat)
}



int dm_lstat(dm_context* context, const char* path, struct stat* buf)
{
  TRY(context, lstat)
  NOT_NULL(path);
  NOT_NULL(buf);
  *buf = context->catalog->linkStat(path);
  CATCH(context, lstat);
}



int dm_xstat(dm_context* context, const char* path, struct xstat* buf)
{
  TRY(context, lstat)
  NOT_NULL(path);
  NOT_NULL(buf);
  *buf = context->catalog->extendedStat(path);
  CATCH(context, lstat);
}



int dm_istat(dm_context* context, ino_t inode, struct stat* buf)
{
  TRY(context, lstat)
  NOT_NULL(buf);
  *buf = context->catalog->stat(inode);
  CATCH(context, lstat);
}



int dm_ixstat(dm_context* context, ino_t inode, struct xstat* buf)
{
  TRY(context, lstat)
  NOT_NULL(buf);
  *buf = context->catalog->extendedStat(inode);
  CATCH(context, lstat);
}



int dm_addreplica(dm_context* context, const char* guid, int64_t id,
                  const char* server, const char* surl, const char status,
                  const char fileType, const char* poolName,
                  const char* fileSystem)
{
  TRY(context, addReplica)
  NOT_NULL(surl);
  context->catalog->addReplica(SAFE_STRING(guid), id, SAFE_STRING(server),
                               surl, status, fileType,
                               SAFE_STRING(poolName), SAFE_STRING(fileSystem));
  CATCH(context, addReplica)
}



int dm_delreplica(dm_context* context, const char* guid, int64_t id,
                  const char* surl)
{
  TRY(context, delreplica)
  NOT_NULL(surl);
  context->catalog->deleteReplica(SAFE_STRING(guid), id, surl);
  CATCH(context, delreplica)
}



int dm_getreplicas(dm_context* context, const char* path, int *nEntries,
                  struct filereplica** fileReplicas)
{
  TRY(context, getreplicas)
  NOT_NULL(path);
  NOT_NULL(nEntries);
  NOT_NULL(fileReplicas);

  std::vector<filereplica> replicaSet = context->catalog->getReplicas(path);

  *fileReplicas = new filereplica[replicaSet.size()];
  *nEntries = replicaSet.size();

  std::copy(replicaSet.begin(), replicaSet.end(), *fileReplicas);

  CATCH(context, getreplicas)
}



int dm_freereplicas(dm_context* context, int nReplicas, struct filereplica* fileReplicas)
{
  delete [] fileReplicas;
  return 0;
}



int dm_get(dm_context* context, const char* path, struct filereplica* replica)
{
  TRY(context, get)
  NOT_NULL(path);
  NOT_NULL(replica);
  *replica = context->catalog->get(path);
  CATCH(context, get)
}



int dm_create(dm_context* context, const char* path, mode_t mode)
{
  TRY(context, create)
  NOT_NULL(path);
  context->catalog->create(path, mode);
  CATCH(context, create)
}



int dm_put(dm_context* context, const char* path, struct uri* uri, char* token)
{
  TRY(context, put)
  NOT_NULL(path);
  NOT_NULL(uri);
  NOT_NULL(token);
  std::string t = context->catalog->put(path, uri);
  strcpy(token, t.c_str());
  CATCH(context, put)
}



int dm_putg(dm_context* context, const char* path, struct uri* uri, const char* guid, char* token)
{
  TRY(context, putg)
  NOT_NULL(path);
  NOT_NULL(uri);
  NOT_NULL(token);
  std::string t;
  if (guid != NULL)
    t = context->catalog->put(path, uri, guid);
  else
    t = context->catalog->put(path, uri);
  strcpy(token, t.c_str());
  CATCH(context, putg)
}



int dm_putstatus(dm_context* context, const char* path, const char* token, struct uri* uri)
{
  TRY(context, putstatus)
  NOT_NULL(path);
  NOT_NULL(token);
  NOT_NULL(uri);
  context->catalog->putStatus(path, token, uri);
  CATCH(context, putstatus)
}



int dm_putdone(dm_context* context, const char* path, const char* token)
{
  TRY(context, putdone)
  NOT_NULL(path);
  NOT_NULL(token);
  context->catalog->putDone(path, token);
  CATCH(context, putdone)
}



int dm_unlink(dm_context* context, const char* path)
{
  TRY(context, unlink)
  NOT_NULL(path);
  context->catalog->unlink(path);
  CATCH(context, unlink)
}



int dm_chmod(dm_context* context, const char* path, mode_t mode)
{
  TRY(context, chmod)
  NOT_NULL(path);
  context->catalog->changeMode(path, mode);
  CATCH(context, chmod)
}



int dm_chown(dm_context* context, const char* path, uid_t newUid, gid_t newGid)
{
  TRY(context, chown)
  NOT_NULL(path);
  context->catalog->changeOwner(path, newUid, newGid);
  CATCH(context, chown)
}



int dm_lchown(dm_context* context, const char* path, uid_t newUid, gid_t newGid)
{
  TRY(context, lchown)
  NOT_NULL(path);
  context->catalog->linkChangeOwner(path, newUid, newGid);
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

  context->catalog->setAcl(path, aclV);

  CATCH(context, setacl)
}



int dm_utime(dm_context* context, const char* path, const struct utimbuf* buf)
{
  TRY(context, utime)
  NOT_NULL(path);
  context->catalog->utime(path, buf);
  CATCH(context, utime)
}



int dm_getcomment(dm_context* context, const char* path, char* comment)
{
  TRY(context, getcomment)
  NOT_NULL(path);
  NOT_NULL(comment);
  std::string c = context->catalog->getComment(path);
  strcpy(comment, c.c_str());
  CATCH(context, getcomment)
}



int dm_setcomment(dm_context* context, const char* path, const char* comment)
{
  TRY(context, setcomment)
  NOT_NULL(path);
  NOT_NULL(comment);
  context->catalog->setComment(path, comment);
  CATCH(context, setcomment)
}




int dm_getgrpbygid(dm_context* context, gid_t gid, char* groupName)
{
  TRY(context, getgrpbygid)
  NOT_NULL(groupName);
  groupinfo group = context->catalog->getGroup(groupName);
  strcpy(groupName, group.name);
  CATCH(context, getgrpbygid)
}



int dm_getgrpbynam(dm_context* context, const char* groupName, gid_t* gid)
{
  TRY(context, getgrpbynam)
  NOT_NULL(groupName);
  NOT_NULL(gid);
  groupinfo group = context->catalog->getGroup(groupName);
  *gid = group.gid;
  CATCH(context, getgrpbynam)
}



int dm_getidmap(dm_context* context, const char* username, int nGroups,
                const char** groupNames, uid_t* uid, gid_t* gids)
{
  TRY(context, getidmap)
  NOT_NULL(username);
  if (nGroups > 0) NOT_NULL(groupNames);
  NOT_NULL(uid);
  NOT_NULL(gids);

  std::vector<std::string> groupSet;
  std::vector<gid_t>       gidSet;

  for(int i = 0; i < nGroups; ++i)
    groupSet.push_back(groupNames[i]);

  context->catalog->getIdMap(username, groupSet, uid, &gidSet);

  std::copy(gidSet.begin(), gidSet.end(), gids);

  CATCH(context, getidmap)
}



int dm_getusrbynam(dm_context* context, const char* userName, uid_t* uid)
{
  TRY(context, getusrbyuid)
  NOT_NULL(userName);
  userinfo user = context->catalog->getUser(userName);
  *uid = user.uid;
  CATCH(context, getusrbyuid)
}



int dm_getusrbyuid(dm_context* context, uid_t uid, char* userName)
{
  TRY(context, getusrbyuid)
  NOT_NULL(userName);
  userinfo user = context->catalog->getUser(uid);
  strcpy(userName, user.name);
  CATCH(context, getusrbyuid)
}



void* dm_opendir(dm_context* context, const char* path)
{
  TRY(context, opendir)
  NOT_NULL(path);
  return context->catalog->openDir(path);
  CATCH_POINTER(context, opendir)
}



int dm_closedir(dm_context* context, void* dir)
{
  TRY(context, closedir)
  NOT_NULL(dir);
  context->catalog->closeDir(dir);
  CATCH(context, closedir)
}



struct dirent* dm_readdir(dm_context* context, void* dir)
{
  TRY(context, readdir)
  NOT_NULL(dir);
  return context->catalog->readDir(dir);
  CATCH_POINTER(context, readdir)
}



struct direntstat* dm_readdirx(dm_context* context, void* dir)
{
  TRY(context, readdirx)
  NOT_NULL(dir);
  return context->catalog->readDirx(dir);
  CATCH_POINTER(context, readdirx)
}



int dm_mkdir(dm_context* context, const char* path, mode_t mode)
{
  TRY(context, mkdir)
  NOT_NULL(path);
  context->catalog->makeDir(path, mode);
  CATCH(context, mkdir)
}



int dm_rename(dm_context* context, const char* oldPath, const char* newPath)
{
  TRY(context, rename)
  NOT_NULL(oldPath);
  NOT_NULL(newPath);
  context->catalog->rename(oldPath, newPath);
  CATCH(context, rename)
}



int dm_rmdir(dm_context* context, const char* path)
{
  TRY(context, rmdir)
  NOT_NULL(path);
  context->catalog->removeDir(path);
  CATCH(context, rmdir)
}



int dm_replica_setltime(dm_context* context, const char* replica, time_t ltime)
{
  TRY(context, setltime)
  NOT_NULL(replica);
  context->catalog->replicaSetLifeTime(replica, ltime);
  CATCH(context, setltime);
}



int dm_replica_setatime(dm_context* context, const char* replica)
{
  TRY(context, setatime)
  NOT_NULL(replica);
  context->catalog->replicaSetAccessTime(replica);
  CATCH(context, setatime);
}



int dm_replica_settype(dm_context* context, const char* replica, char ftype)
{
  TRY(context, settype)
  NOT_NULL(replica);
  context->catalog->replicaSetType(replica, ftype);
  CATCH(context, settype);
}



int dm_replica_setstatus(dm_context* context, const char* replica, char status)
{
  TRY(context, setstatus)
  NOT_NULL(replica);
  context->catalog->replicaSetStatus(replica, status);
  CATCH(context, setlstatus);
}



int dm_setuserid(dm_context* context, uid_t uid, gid_t gid, const char* dn)
{
  TRY(context, setuserid)
  context->catalog->setUserId(uid, gid, SAFE_STRING(dn));
  CATCH(context, setuserid)
}



int dm_setvomsdata(dm_context* context, const char* vo, const char** fqans, int nFqans)
{
  TRY(context, setvomsdata)
  NOT_NULL(vo);
  if (nFqans > 0) NOT_NULL(fqans);

  std::vector<std::string> fqansSet(nFqans);
  fqansSet.assign(fqans, fqans + nFqans);

  context->catalog->setVomsData(vo, fqansSet);
  CATCH(context, setvomsdata)
}



int dm_getpools(dm_context* context, int* nbpools, struct pool** pools)
{
  if (context->pool == 0x00) {
    context->errorCode   = DM_NO_POOL_MANAGER;
    context->errorString = "There is no Pool Manager Plugin";
    return -1;
  }

  TRY(context, getpools)
  NOT_NULL(nbpools);
  NOT_NULL(pools);
  
  std::vector<pool> poolSet = context->pool->getPools();

  *pools   = new pool[poolSet.size()];
  *nbpools = poolSet.size();

  std::copy(poolSet.begin(), poolSet.end(), *pools);

  CATCH(context, getpools)
}



int dm_freepools(dm_context* context, int npools, struct pool* pools)
{
  for (int i = 0; i < npools; ++i) {
    delete [] pools[i].gids;
    delete [] pools[i].elemp;
  }
  delete [] pools;
  return 0;
}



int dm_getpoolfs(dm_context* context, const char *poolname, int *nbfs, struct filesystem **fs)
{
  if (context->pool == 0x00) {
    context->errorCode   = DM_NO_POOL_MANAGER;
    context->errorString = "There is no Pool Manager Plugin";
    return -1;
  }
  
  TRY(context, getpoolfs)
  NOT_NULL(poolname);
  NOT_NULL(nbfs);
  NOT_NULL(fs);

  std::vector<filesystem> filesystems = context->pool->getPoolFilesystems(poolname);

  *fs   = new filesystem[filesystems.size()];
  *nbfs = filesystems.size();
  std::copy(filesystems.begin(), filesystems.end(), *fs);

  CATCH(context, getpoolfs)
}



int dm_freefs(dm_context* context, int nFs, struct filesystem *fs)
{
  delete [] fs;
  return 0;
}



int dm_errno(dm_context* context)
{
  if (context == NULL || context->catalog == NULL)
    return DM_NULL_POINTER;
  else
    return context->errorCode;
}



const char* dm_error(dm_context* context)
{
  if (context == NULL)
    return "The context is a NULL pointer";
  if (context->catalog == NULL)
    return "There is no catalog implementation";
  return context->errorString.c_str();
}



void dm_parse_uri(const char* source, struct uri* dest)
{
  *dest = dmlite::splitUri(source);
}



void dm_serialize_acls(int nAcls, struct dm_acl* acls, char* buffer, size_t bsize)
{
  std::vector<Acl> aclV(nAcls);
  aclV.assign(acls, acls + nAcls);

  std::string aclStr = dmlite::serializeAcl(aclV);

  std::strncpy(buffer, aclStr.c_str(), bsize);
}

void dm_deserialize_acls(const char* buffer, int* nAcls, struct dm_acl** acls)
{
  std::vector<Acl> aclV = dmlite::deserializeAcl(buffer);

  *nAcls = aclV.size();
  *acls  = new Acl[*nAcls];

  std::copy(aclV.begin(), aclV.end(), *acls);
}



void dm_freeacls(int nAcls, struct dm_acl* acls)
{
  delete [] acls;
}
