/// @file   core/Catalog-C.cpp
/// @brief  C wrapper for dmlite::Catalog.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <cstring>
#include <dmlite/c/dmlite.h>
#include <dmlite/c/catalog.h>
#include <dmlite/cpp/catalog.h>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/exceptions.h>
#include <dmlite/cpp/utils/urls.h>
#include <set>
#include <vector>

#include "Private.h"

static void dmlite_cppxstat_to_cxstat(const dmlite::ExtendedStat& ex,
                                      dmlite_xstat* buf)
{
  std::strncpy(buf->acl,       ex.acl.serialize().c_str(), sizeof(buf->acl));
  std::strncpy(buf->csumtype,  ex.csumtype.c_str(),  sizeof(buf->csumtype));
  std::strncpy(buf->csumvalue, ex.csumvalue.c_str(), sizeof(buf->csumvalue));
  std::strncpy(buf->guid,      ex.guid.c_str(),      sizeof(buf->guid));
  std::strncpy(buf->name,      ex.name.c_str(),      sizeof(buf->name));
  buf->parent = ex.parent;
  buf->stat   = ex.stat;
  buf->status = static_cast<dmlite_file_status>(ex.status);
  
  if (buf->extra != NULL) {
    buf->extra->extensible.clear();
    buf->extra->extensible.copy(ex);
  }
}



int dmlite_chdir(dmlite_context* context, const char* path)
{
  TRY(context, chdir)
  NOT_NULL(path);
  context->stack->getCatalog()->changeDir(path);
  CATCH(context, chdir)
}



char* dmlite_getcwd(dmlite_context* context, char* buffer, size_t size)
{
  TRY(context, getcwd)
  std::string wd = context->stack->getCatalog()->getWorkingDir();
  if (buffer != NULL)
    return strncpy(buffer, wd.c_str(), size);
  else
    return strdup(wd.c_str());
  CATCH_POINTER(context, getcwd)
}



mode_t dmlite_umask(dmlite_context* context, mode_t mask)
{
  TRY(context, umask)
  return context->stack->getCatalog()->umask(mask);
  }
  catch(...) {
    return 0;
  }
}



int dmlite_stat(dmlite_context *context, const char* path, struct stat* buf)
{
  TRY(context, stat)
  NOT_NULL(path);
  *buf = context->stack->getCatalog()->extendedStat(path).stat;
  CATCH(context, stat)
}



int dmlite_statl(dmlite_context* context, const char* path, struct stat* buf)
{
  TRY(context, lstat)
  NOT_NULL(path);
  NOT_NULL(buf);
  *buf = context->stack->getCatalog()->extendedStat(path, false).stat;
  CATCH(context, lstat);
}



int dmlite_statx(dmlite_context* context, const char* path, dmlite_xstat* buf)
{
  TRY(context, lstat)
  NOT_NULL(path);
  NOT_NULL(buf);
  dmlite::ExtendedStat ex = context->stack->getCatalog()->extendedStat(path);
  dmlite_cppxstat_to_cxstat(ex, buf);
  CATCH(context, lstat);
}



int dmlite_stati(dmlite_context* context, ino_t inode, struct stat* buf)
{
  TRY(context, lstat)
  NOT_NULL(buf);
  *buf = context->stack->getINode()->extendedStat(inode).stat;
  CATCH(context, lstat);
}



int dmlite_statix(dmlite_context* context, ino_t inode, dmlite_xstat* buf)
{
  TRY(context, ixstat)
  NOT_NULL(buf);
  dmlite::ExtendedStat ex = context->stack->getINode()->extendedStat(inode);
  dmlite_cppxstat_to_cxstat(ex, buf);
  CATCH(context, ixstat);
}



int dmlite_addreplica(dmlite_context* context, const dmlite_replica* replica)
{
  TRY(context, addReplica)
  NOT_NULL(replica);  
  dmlite::Replica replicapp;
  
  replicapp.fileid = replica->fileid;
  replicapp.status = static_cast<dmlite::Replica::ReplicaStatus>(replica->status);
  replicapp.type   = static_cast<dmlite::Replica::ReplicaType>(replica->type);
  replicapp.server = replica->server;
  replicapp.rfn    = replica->rfn;
  if (replica->extra != NULL)
    replicapp.copy(replica->extra->extensible);
  
  context->stack->getCatalog()->addReplica(replicapp);
  
  CATCH(context, addReplica)
}



int dmlite_delreplica(dmlite_context* context, const dmlite_replica* replica)
{
  TRY(context, delreplica)
  NOT_NULL(replica);
  dmlite::Replica replicapp;
  
  replicapp.fileid = replica->fileid;
  replicapp.status = static_cast<dmlite::Replica::ReplicaStatus>(replica->status);
  replicapp.type   = static_cast<dmlite::Replica::ReplicaType>(replica->type);
  replicapp.server = replica->server;
  replicapp.rfn    = replica->rfn;
  if (replica->extra != NULL)
    replicapp.copy(replica->extra->extensible);
  
  context->stack->getCatalog()->deleteReplica(replicapp);
  CATCH(context, delreplica)
}



int dmlite_getreplicas(dmlite_context* context, const char* path, unsigned *nReplicas,
                       dmlite_replica** fileReplicas)
{
  TRY(context, getreplicas)
  NOT_NULL(path);
  NOT_NULL(nReplicas);
  NOT_NULL(fileReplicas);

  std::vector<dmlite::Replica> replicaSet = context->stack->getCatalog()->getReplicas(path);

  *nReplicas = replicaSet.size();
  if (*nReplicas > 0)
    *fileReplicas = new dmlite_replica[replicaSet.size()];
  else
    *fileReplicas = NULL;

  for (unsigned i = 0; i < *nReplicas; ++i) {
    (*fileReplicas)[i].atime  = replicaSet[i].atime;
    (*fileReplicas)[i].fileid = replicaSet[i].fileid;
    (*fileReplicas)[i].ltime  = replicaSet[i].ltime;
    (*fileReplicas)[i].nbaccesses = replicaSet[i].nbaccesses;
    (*fileReplicas)[i].ptime  = replicaSet[i].ptime;
    (*fileReplicas)[i].replicaid  = replicaSet[i].replicaid;
    (*fileReplicas)[i].status = static_cast<dmlite_replica_status>(replicaSet[i].status);
    (*fileReplicas)[i].type   = static_cast<dmlite_replica_type>(replicaSet[i].type);
    strncpy((*fileReplicas)[i].rfn, replicaSet[i].rfn.c_str(), sizeof((*fileReplicas)[i].rfn));
    strncpy((*fileReplicas)[i].server, replicaSet[i].server.c_str(), sizeof((*fileReplicas)[i].server));
    
    (*fileReplicas)[i].extra = new dmlite_any_dict();
    (*fileReplicas)[i].extra->extensible.copy(replicaSet[i]);
  }  

  CATCH(context, getreplicas)
}



int dmlite_replicas_free(dmlite_context* context, unsigned nReplicas,
                         dmlite_replica* fileReplicas)
{
  for (unsigned i = 0; i < nReplicas; ++i) {
    if (fileReplicas[i].extra != NULL)
      delete fileReplicas[i].extra;
  }
  delete [] fileReplicas;
  return 0;
}



int dmlite_create(dmlite_context* context, const char* path, mode_t mode)
{
  TRY(context, create)
  NOT_NULL(path);
  context->stack->getCatalog()->create(path, mode);
  CATCH(context, create)
}



int dmlite_unlink(dmlite_context* context, const char* path)
{
  TRY(context, unlink)
  NOT_NULL(path);
  context->stack->getCatalog()->unlink(path);
  CATCH(context, unlink)
}



int dmlite_chmod(dmlite_context* context, const char* path, mode_t mode)
{
  TRY(context, chmod)
  NOT_NULL(path);
  context->stack->getCatalog()->setMode(path, mode);
  CATCH(context, chmod)
}



int dmlite_chown(dmlite_context* context, const char* path, uid_t newUid, gid_t newGid)
{
  TRY(context, chown)
  NOT_NULL(path);
  context->stack->getCatalog()->setOwner(path, newUid, newGid);
  CATCH(context, chown)
}



int dmlite_lchown(dmlite_context* context, const char* path, uid_t newUid, gid_t newGid)
{
  TRY(context, lchown)
  NOT_NULL(path);
  context->stack->getCatalog()->setOwner(path, newUid, newGid, false);
  CATCH(context, lchown)
}



int dmlite_setfsize(dmlite_context* context, const char* path, uint64_t filesize)
{
  TRY(context, setfsize)
  NOT_NULL(path);
  context->stack->getCatalog()->setSize(path, filesize);
  CATCH(context, setfsize)
}



int dmlite_setfsizec(dmlite_context* context, const char* path, uint64_t filesize,
                 const char* csumtype, const char* csumvalue)
{
  TRY(context, setfsizec)
  NOT_NULL(path);
  NOT_NULL(csumtype);
  NOT_NULL(csumvalue);
  context->stack->getCatalog()->setSize(path, filesize);
  context->stack->getCatalog()->setChecksum(path, csumtype, csumvalue);
  CATCH(context, setfsizec)
}



int dmlite_setacl(dmlite_context* context, const char* path, unsigned nEntries, dmlite_aclentry* acl)
{
  TRY(context, setacl)
  NOT_NULL(path);
  NOT_NULL(acl);

  dmlite::Acl acl;
  for (unsigned i = 0; i < nEntries; ++i) {
    dmlite::AclEntry e;
    
    e.id   = acl[i].id;
    e.perm = acl[i].perm;
    e.type = static_cast<dmlite::AclEntry::AclType>(acl[i].type);
    
    acl.push_back(e);
  }

  context->stack->getCatalog()->setAcl(path, acl);

  CATCH(context, setacl)
}



int dmlite_utime(dmlite_context* context, const char* path, const struct utimbuf* buf)
{
  TRY(context, utime)
  NOT_NULL(path);
  context->stack->getCatalog()->utime(path, buf);
  CATCH(context, utime)
}



int dmlite_getcomment(dmlite_context* context, const char* path, char* comment)
{
  TRY(context, getcomment)
  NOT_NULL(path);
  NOT_NULL(comment);
  std::string c = context->stack->getCatalog()->getComment(path);
  strcpy(comment, c.c_str());
  CATCH(context, getcomment)
}



int dmlite_setcomment(dmlite_context* context, const char* path, const char* comment)
{
  TRY(context, setcomment)
  NOT_NULL(path);
  NOT_NULL(comment);
  context->stack->getCatalog()->setComment(path, comment);
  CATCH(context, setcomment)
}



int dmlite_getgrpbynam(dmlite_context* context, const char* groupName, gid_t* gid)
{
  TRY(context, getgrpbynam)
  NOT_NULL(groupName);
  NOT_NULL(gid);
  dmlite::GroupInfo group = context->stack->getAuthn()->getGroup(groupName);
  *gid = group.getUnsigned("gid");
  CATCH(context, getgrpbynam)
}



int dmlite_getusrbynam(dmlite_context* context, const char* userName, uid_t* uid)
{
  TRY(context, getusrbyuid)
  NOT_NULL(userName);
  dmlite::UserInfo user = context->stack->getAuthn()->getUser(userName);
  *uid = user.getUnsigned("uid");
  CATCH(context, getusrbyuid)
}



dmlite_dir* dmlite_opendir(dmlite_context* context, const char* path)
{
  TRY(context, opendir)
  NOT_NULL(path);
  dmlite::Directory* d = context->stack->getCatalog()->openDir(path);
  
  dmlite_dir* dirp = new dmlite_dir();
  dirp->dir = d;
  memset(&dirp->xstat, 0, sizeof(dirp->xstat));
  dirp->xstat.extra = dmlite_any_dict_new();
  
  return dirp;  
  CATCH_POINTER(context, opendir)
}



int dmlite_closedir(dmlite_context* context, dmlite_dir* dir)
{
  TRY(context, closedir)
  NOT_NULL(dir);
  context->stack->getCatalog()->closeDir(dir->dir);
  delete dir->xstat.extra;
  delete dir;
  CATCH(context, closedir)
}



struct dirent* dmlite_readdir(dmlite_context* context, dmlite_dir* dir)
{
  TRY(context, readdir)
  NOT_NULL(dir);
  return context->stack->getCatalog()->readDir(dir->dir);
  CATCH_POINTER(context, readdir)
}



struct dmlite_xstat* dmlite_readdirx(dmlite_context* context, dmlite_dir* dir)
{
  TRY(context, readdirx)
  NOT_NULL(dir);
  dmlite::ExtendedStat* ex = context->stack->getCatalog()->readDirx(dir->dir);
  if (ex == NULL)
    return NULL;
  
  dmlite_cppxstat_to_cxstat(*ex, &dir->xstat);
  return &dir->xstat;
  CATCH_POINTER(context, readdirx)
}



int dmlite_mkdir(dmlite_context* context, const char* path, mode_t mode)
{
  TRY(context, mkdir)
  NOT_NULL(path);
  context->stack->getCatalog()->makeDir(path, mode);
  CATCH(context, mkdir)
}



int dmlite_rename(dmlite_context* context, const char* oldPath, const char* newPath)
{
  TRY(context, rename)
  NOT_NULL(oldPath);
  NOT_NULL(newPath);
  context->stack->getCatalog()->rename(oldPath, newPath);
  CATCH(context, rename)
}



int dmlite_rmdir(dmlite_context* context, const char* path)
{
  TRY(context, rmdir)
  NOT_NULL(path);
  context->stack->getCatalog()->removeDir(path);
  CATCH(context, rmdir)
}



int dmlite_getreplica(dmlite_context* context, const char* rfn, dmlite_replica* replica)
{
  TRY(context, getreplica)
  NOT_NULL(rfn);
  NOT_NULL(replica);
  dmlite::Replica replicapp = context->stack->getCatalog()->getReplica(rfn);
  
  if (replica->extra != NULL) {
    replica->extra->extensible.clear();
    replica->extra->extensible.copy(replicapp);
  }
  
  replica->atime  = replicapp.atime;
  replica->fileid = replicapp.fileid;
  replica->ltime  = replicapp.ltime;
  replica->nbaccesses = replicapp.nbaccesses;
  replica->ptime  = replicapp.ptime;
  replica->replicaid = replicapp.replicaid;
  std::strncpy(replica->rfn, replicapp.rfn.c_str(), sizeof(replica->rfn));
  std::strncpy(replica->server, replicapp.server.c_str(), sizeof(replica->server));
  replica->status = static_cast<dmlite_replica_status>(replicapp.status);
  replica->type   = static_cast<dmlite_replica_type>(replicapp.type);
  
  CATCH(context, getreplica)
}



int dmlite_updatereplica(dmlite_context* context, const dmlite_replica* replica)
{
  TRY(context, updatereplica)
  NOT_NULL(replica);
  dmlite::Replica replicapp;
  
  if (replica->extra != NULL)
    replicapp.copy(replica->extra->extensible);
  
  replicapp.atime  = replica->atime;
  replicapp.fileid = replica->fileid;
  replicapp.ltime  = replica->ltime;
  replicapp.nbaccesses = replica->nbaccesses;
  replicapp.ptime  = replica->ptime;
  replicapp.replicaid = replica->replicaid;
  replicapp.rfn    = replica->rfn;
  replicapp.server = replica->server;
  replicapp.status = static_cast<dmlite::Replica::ReplicaStatus>(replica->status);
  replicapp.type   = static_cast<dmlite::Replica::ReplicaType>(replica->type);
  
  context->stack->getCatalog()->updateReplica(replicapp);
  CATCH(context, updatereplica)
}
