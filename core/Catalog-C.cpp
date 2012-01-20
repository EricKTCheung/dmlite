/// @file   core/Catalog-C.cpp
/// @brief  C wrapper for dmlite::Catalog.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <cstdarg>
#include <dmlite/dmlite++.h>
#include <dmlite/dmlite.h>
#include <dmlite/dm_exceptions.h>
#include <set>
#include <vector>

#include "Private.h"


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



int dm_set(dm_context *context, const char *key, ...)
{
  va_list vargs;

  TRY(context, set)
  va_start(vargs, key);
  context->catalog->set(key, vargs);
  va_end(vargs);
  CATCH(context, set)
}



int dm_chdir(dm_context* context, const char* path)
{
  TRY(context, chdir)
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
  *buf = context->catalog->stat(path);
  CATCH(context, stat)
}



int dm_lstat(dm_context* context, const char* path, struct stat* buf)
{
  TRY(context, lstat)
  *buf = context->catalog->linkStat(path);
  CATCH(context, lstat);
}



int dm_xstat(dm_context* context, const char* path, struct xstat* buf)
{
  TRY(context, lstat)
  *buf = context->catalog->extendedStat(path);
  CATCH(context, lstat);
}



int dm_addreplica(dm_context* context, const char* guid, int64_t id,
                  const char* server, const char* path, const char status,
                  const char fileType, const char* poolName,
                  const char* fileSystem)
{
  TRY(context, addReplica)
  context->catalog->addReplica(guid, id, server, path, status, fileType, poolName, fileSystem);
  CATCH(context, addReplica)
}



int dm_delreplica(dm_context* context, const char* guid, int64_t id,
                  const char* path)
{
  TRY(context, delreplica)
  context->catalog->deleteReplica(guid, id, path);
  CATCH(context, delreplica)
}



int dm_getreplicas(dm_context* context, const char* path, int *nEntries,
                  struct filereplica** fileReplicas)
{
  TRY(context, getreplicas)

  std::vector<filereplica> replicaSet = context->catalog->getReplicas(path);

  *fileReplicas = new filereplica[replicaSet.size()];

  std::vector<filereplica>::const_iterator i;
  for (i = replicaSet.begin(), *nEntries = 0;
       i != replicaSet.end();
       ++i, ++*nEntries) {
    (*fileReplicas)[*nEntries] = *i;
  }

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
  *replica = context->catalog->get(path);
  CATCH(context, get)
}



int dm_put(dm_context* context, const char* path, struct uri* uri, char* token)
{
  TRY(context, put)
  std::string t = context->catalog->put(path, uri);
  strcpy(token, t.c_str());
  CATCH(context, put)
}



int dm_putg(dm_context* context, const char* path, struct uri* uri, const char* guid, char* token)
{
  TRY(context, putg)
  std::string t = context->catalog->put(path, uri, guid);
  strcpy(token, t.c_str());
  CATCH(context, putg)
}



int dm_putstatus(dm_context* context, const char* path, const char* token, struct uri* uri)
{
  TRY(context, putstatus)
  context->catalog->putStatus(path, token, uri);
  CATCH(context, putstatus)
}



int dm_putdone(dm_context* context, const char* path, const char* token)
{
  TRY(context, putdone)
  context->catalog->putDone(path, token);
  CATCH(context, putdone)
}



int dm_unlink(dm_context* context, const char* path)
{
  TRY(context, unlink)
  context->catalog->unlink(path);
  CATCH(context, unlink)
}



int dm_chmod(dm_context* context, const char* path, mode_t mode)
{
  TRY(context, chmod)
  context->catalog->changeMode(path, mode);
  CATCH(context, chmod)
}



int dm_chown(dm_context* context, const char* path, uid_t newUid, gid_t newGid)
{
  TRY(context, chown)
  context->catalog->changeOwner(path, newUid, newGid);
  CATCH(context, chown)
}



int dm_lchown(dm_context* context, const char* path, uid_t newUid, gid_t newGid)
{
  TRY(context, lchown)
  context->catalog->linkChangeOwner(path, newUid, newGid);
  CATCH(context, lchown)
}



int dm_getcomment(dm_context* context, const char* path, char* comment)
{
  TRY(context, getcomment)
  std::string c = context->catalog->getComment(path);
  strcpy(comment, c.c_str());
  CATCH(context, getcomment)
}



int dm_setcomment(dm_context* context, const char* path, const char* comment)
{
  TRY(context, setcomment)
  context->catalog->setComment(path, comment);
  CATCH(context, setcomment)
}




int dm_getgrpbygid(dm_context* context, gid_t gid, char* groupName)
{
  TRY(context, getgrpbygid)
  groupinfo group = context->catalog->getGroup(groupName);
  strcpy(groupName, group.name);
  CATCH(context, getgrpbygid)
}



int dm_getgrpbynam(dm_context* context, const char* groupName, gid_t* gid)
{
  TRY(context, getgrpbynam)
  groupinfo group = context->catalog->getGroup(groupName);
  *gid = group.gid;
  CATCH(context, getgrpbynam)
}



int dm_getidmap(dm_context* context, const char* username, int nGroups,
                const char** groupNames, uid_t* uid, gid_t* gids)
{
  TRY(context, getidmap)
  std::vector<std::string> groupSet;
  std::vector<gid_t>       gidSet;

  for(int i = 0; i < nGroups; ++i)
    groupSet.push_back(groupNames[i]);

  context->catalog->getIdMap(std::string(username), groupSet, uid, &gidSet);

  int i;
  std::vector<gid_t>::const_iterator j;
  for (i = 0, j = gidSet.begin(); j != gidSet.end(); ++i, ++j)
    gids[i] = *j;

  CATCH(context, getidmap)
}



int dm_getusrbynam(dm_context* context, const char* userName, uid_t* uid)
{
  TRY(context, getusrbyuid)
  userinfo user = context->catalog->getUser(userName);
  *uid = user.uid;
  CATCH(context, getusrbyuid)
}



int dm_getusrbyuid(dm_context* context, uid_t uid, char* userName)
{
  TRY(context, getusrbyuid)
  userinfo user = context->catalog->getUser(uid);
  strcpy(userName, user.name);
  CATCH(context, getusrbyuid)
}



void* dm_opendir(dm_context* context, const char* path)
{
  TRY(context, opendir)
  return context->catalog->openDir(path);
  CATCH_POINTER(context, opendir)
}



int dm_closedir(dm_context* context, void* dir)
{
  TRY(context, closedir)
  context->catalog->closeDir(dir);
  CATCH(context, closedir)
}



struct dirent* dm_readdir(dm_context* context, void* dir)
{
  TRY(context, readdir)
  return context->catalog->readDir(dir);
  CATCH_POINTER(context, readdir)
}



struct direntstat* dm_readdirx(dm_context* context, void* dir)
{
  TRY(context, readdirx)
  return context->catalog->readDirx(dir);
  CATCH_POINTER(context, readdirx)
}



int dm_mkdir(dm_context* context, const char* path, mode_t mode)
{
  TRY(context, mkdir)
  context->catalog->makeDir(path, mode);
  CATCH(context, mkdir)
}



int dm_rename(dm_context* context, const char* oldPath, const char* newPath)
{
  TRY(context, rename)
  context->catalog->rename(oldPath, newPath);
  CATCH(context, rename)
}



int dm_rmdir(dm_context* context, const char* path)
{
  TRY(context, rmdir)
  context->catalog->removeDir(path);
  CATCH(context, rmdir)
}



int dm_setuserid(dm_context* context, uid_t uid, gid_t gid, const char* dn)
{
  TRY(context, setuserid)
  context->catalog->setUserId(uid, gid, dn);
  CATCH(context, setuserid)
}



int dm_setvomsdata(dm_context* context, const char* vo, const char** fqans, int nFqans)
{
  TRY(context, setvomsdata)
  std::vector<std::string> fqansSet;

  for (int i = 0; i < nFqans; ++i)
    fqansSet.push_back(fqans[i]);

  context->catalog->setVomsData(vo, fqansSet);
  CATCH(context, setvomsdata)
}



int dm_getpools(dm_context* context, int* nbpools, struct pool** pools)
{
  TRY(context, getpools)
  std::vector<pool> poolSet = context->catalog->getPools();

  *pools = new pool[poolSet.size()];

  std::vector<pool>::const_iterator i;
  for (i = poolSet.begin(), *nbpools = 0;
       i != poolSet.end();
       ++i, ++*nbpools) {
    (*pools)[*nbpools] = *i;
  }
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
  TRY(context, getpoolfs)
  std::vector<filesystem> filesystems = context->catalog->getPoolFilesystems(poolname);

  *fs = new filesystem[filesystems.size()];

  std::vector<filesystem>::const_iterator i;
  for (i = filesystems.begin(), *nbfs = 0;
       i != filesystems.end();
       ++i, ++*nbfs) {
    (*fs)[*nbfs] = *i;
  }
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
