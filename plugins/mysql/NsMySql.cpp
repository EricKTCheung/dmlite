/// @file    plugins/mysql/NsMySql.cpp
/// @brief   MySQL NS Implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstring>
#include <list>
#include <string>
#include <time.h>
#include <vector>
#include <mysql/mysql.h>
#include <stdlib.h>
#include <dmlite/common/Uris.h>

#include "MySqlWrapper.h"
#include "NsMySql.h"
#include "Queries.h"

#define NOT_IMPLEMENTED(p)\
p {\
  throw DmException(DM_NOT_IMPLEMENTED, #p" not implemented");\
}


using namespace dmlite;



NsMySqlCatalog::NsMySqlCatalog(PoolContainer<MYSQL*>* connPool, const std::string& db,
                               unsigned int symLinkLimit) throw(DmException):
                secCtx_(), cwd_(0), umask_(022), nsDb_(db), symLinkLimit_(symLinkLimit)
{
  this->connectionPool_ = connPool;
  this->conn_           = connPool->acquire();
}



NsMySqlCatalog::~NsMySqlCatalog() throw(DmException)
{
  this->connectionPool_->release(this->conn_);
}



std::string NsMySqlCatalog::getImplId() throw ()
{
  return std::string("NsMySqlCatalog");
}



void NsMySqlCatalog::set(const std::string& key, va_list varg) throw(DmException)
{
  throw DmException(DM_UNKNOWN_OPTION, "Option " + key + " unknown");
}



SecurityContext* NsMySqlCatalog::createSecurityContext(const SecurityCredentials& cred) throw (DmException)
{
  UserInfo user;
  std::vector<GroupInfo> groups;

  this->getIdMap(cred.getClientName(), cred.getFqans(), &user, &groups);
  return new SecurityContext(cred, user, groups);
}



void NsMySqlCatalog::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  this->secCtx_ = ctx;
}



static void bindMetadata(Statement& stmt, ExtendedStat* meta) throw(DmException)
{
  stmt.bindResult( 0, &meta->stat.st_ino);
  stmt.bindResult( 1, &meta->parent);
  stmt.bindResult( 2, meta->guid, sizeof(meta->guid));
  stmt.bindResult( 3, meta->name, sizeof(meta->name));
  stmt.bindResult( 4, &meta->stat.st_mode);
  stmt.bindResult( 5, &meta->stat.st_nlink);
  stmt.bindResult( 6, &meta->stat.st_uid);
  stmt.bindResult( 7, &meta->stat.st_gid);
  stmt.bindResult( 8, &meta->stat.st_size);
  stmt.bindResult( 9, &meta->stat.st_atime);
  stmt.bindResult(10, &meta->stat.st_mtime);
  stmt.bindResult(11, &meta->stat.st_ctime);
  stmt.bindResult(12, &meta->type);
  stmt.bindResult(13, &meta->status, 1);
  stmt.bindResult(14, meta->csumtype,  sizeof(meta->csumtype));
  stmt.bindResult(15, meta->csumvalue, sizeof(meta->csumvalue));
  stmt.bindResult(16, meta->acl, sizeof(meta->acl), 0);
}



ExtendedStat NsMySqlCatalog::extendedStat(ino_t fileId) throw(DmException)
{
  Statement    stmt(this->conn_, this->nsDb_, STMT_GET_FILE_BY_ID);
  ExtendedStat meta;

  memset(&meta, 0x00, sizeof(ExtendedStat));

  stmt.bindParam(0, fileId);
  stmt.execute();
  bindMetadata(stmt, &meta);

  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_FILE, "File %ld not found", fileId);

  return meta;
}



ExtendedStat NsMySqlCatalog::guidStat(const std::string& guid) throw (DmException)
{
  Statement    stmt(this->conn_, this->nsDb_, STMT_GET_FILE_BY_GUID);
  ExtendedStat meta;

  memset(&meta, 0x00, sizeof(ExtendedStat));

  stmt.bindParam(0, guid);
  stmt.execute();

  bindMetadata(stmt, &meta);

  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_FILE, "File with guid " + guid + " not found");

  return meta;
}



ExtendedStat NsMySqlCatalog::extendedStat(ino_t parent, const std::string& name) throw(DmException)
{
  Statement    stmt(this->conn_, this->nsDb_, STMT_GET_FILE_BY_NAME);
  ExtendedStat meta;

  memset(&meta, 0x00, sizeof(ExtendedStat));

  stmt.bindParam(0, parent);
  stmt.bindParam(1, name);
  stmt.execute();

  bindMetadata(stmt, &meta);

  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_FILE, name + " not found");

  return meta;
}



SymLink NsMySqlCatalog::readLink(ino_t linkId) throw(DmException)
{
  Statement stmt(this->conn_, this->nsDb_, STMT_GET_SYMLINK);
  SymLink   link;

  memset(&link, 0x00, sizeof(SymLink));

  stmt.bindParam(0, linkId);
  stmt.execute();

  stmt.bindResult(0, &link.fileId);
  stmt.bindResult(1, link.link, sizeof(link), 0);

  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_FILE, "Link %ld not found", linkId);


  return link;
}



ExtendedStat NsMySqlCatalog::newFile(ExtendedStat& parent, const std::string& name,
                                     mode_t mode, long nlink, size_t size,
                                     short type, char status,
                                     const std::string& csumtype,
                                     const std::string& csumvalue,
                                     const std::string& acl) throw (DmException)
{
  ino_t  newFileId;
  gid_t  egid;

  // Destination must not exist!
  try {
    ExtendedStat f = this->extendedStat(parent.stat.st_ino, name);
    throw DmException(DM_EXISTS, name + " already exists");
  }
  catch (DmException e) {
    if (e.code() != DM_NO_SUCH_FILE)
      throw;
  }

  // Check the parent!
  if (checkPermissions(this->secCtx_, parent.acl, parent.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Need write access on the parent");

  // Fetch the new file ID
  Statement uniqueId(this->conn_, this->nsDb_, STMT_SELECT_UNIQ_ID_FOR_UPDATE);

  uniqueId.execute();
  uniqueId.bindResult(0, &newFileId);

  // Update the unique ID
  if (uniqueId.fetch()) {
    Statement updateUnique(this->conn_, this->nsDb_, STMT_UPDATE_UNIQ_ID);
    ++newFileId;
    updateUnique.bindParam(0, newFileId);
    updateUnique.execute();
  }
  // Couldn't get, so insert
  else {
    Statement insertUnique(this->conn_, this->nsDb_, STMT_INSERT_UNIQ_ID);
    newFileId = 1;
    insertUnique.bindParam(0, newFileId);
    insertUnique.execute();
  }

  // Check SGID
  if (parent.stat.st_mode & S_ISGID) {
    egid = parent.stat.st_gid;
    mode |= S_ISGID;
  }
  else {
    egid = this->secCtx_->getGroup(0).gid;
  }

  // Create the entry
  Statement fileStmt(this->conn_, this->nsDb_, STMT_INSERT_FILE);

  fileStmt.bindParam( 0, newFileId);
  fileStmt.bindParam( 1, parent.stat.st_ino);
  fileStmt.bindParam( 2, name);
  fileStmt.bindParam( 3, mode);
  fileStmt.bindParam( 4, nlink);
  fileStmt.bindParam( 5, this->secCtx_->getUser().uid);
  fileStmt.bindParam( 6, egid);
  fileStmt.bindParam( 7, size);
  fileStmt.bindParam( 8, type);
  fileStmt.bindParam( 9, std::string(&status, 1));
  fileStmt.bindParam(10, csumtype);
  fileStmt.bindParam(11, csumvalue);
  fileStmt.bindParam(12, acl);

  fileStmt.execute();
  
  // Increment the nlink
  Statement nlinkStmt(this->conn_, this->nsDb_, STMT_NLINK_FOR_UPDATE);
  nlinkStmt.bindParam(0, parent.stat.st_ino);
  nlinkStmt.execute();
  nlinkStmt.bindResult(0, &parent.stat.st_nlink);
  nlinkStmt.fetch();

  Statement nlinkUpdateStmt(this->conn_, this->nsDb_, STMT_UPDATE_NLINK);

  parent.stat.st_nlink++;
  nlinkUpdateStmt.bindParam(0, parent.stat.st_nlink);
  nlinkUpdateStmt.bindParam(1, parent.stat.st_ino);

  nlinkUpdateStmt.execute();

  // Return back
  return this->extendedStat(newFileId);
}



ExtendedStat NsMySqlCatalog::getParent(const std::string& path,
                                       std::string* parentPath,
                                       std::string* name) throw (DmException)
{
  std::list<std::string> components = splitPath(path);

  parentPath->clear();
  name->clear();

  // Build parent (this is, skipping last one)
  while (components.size() > 1) {
    *parentPath += components.front() + "/";
    components.pop_front();
  }
  if (path[0] == '/')
    *parentPath = "/" + *parentPath;

  *name = components.front();
  components.pop_front();

  // Get the files now
  if (!parentPath->empty())
    return this->extendedStat(*parentPath);
  else if (!this->cwdPath_.empty())
    return this->extendedStat(this->cwd_);
  else
    return this->extendedStat("/");
}



ExtendedStat NsMySqlCatalog::extendedStat(const std::string& path, bool followSym) throw(DmException)
{
  // Split the path always assuming absolute
  std::list<std::string> components = splitPath(path);

  // Iterate starting from absolute root (parent of /) (0)
  uint64_t     parent       = 0;
  unsigned     symLinkLevel = 0;
  ExtendedStat meta;
  std::string  c;

  // If path is absolute OR cwd is empty, start in root
  if (path[0] == '/' || this->cwdPath_.empty()) {
    // Push "/", as we have to look for it too
    components.push_front("/");
    // Root parent "is" a dir and world-readable :)
    memset(&meta, 0x00, sizeof(ExtendedStat));
    meta.stat.st_mode = S_IFDIR | 0555 ;
  }
  // Relative, and cwd set, so start there
  else {
    parent = this->cwd_;
    meta   = this->extendedStat(parent);
  }
  

  while (!components.empty()) {
    // Check that the parent is a directory first
    if (!S_ISDIR(meta.stat.st_mode) && !S_ISLNK(meta.stat.st_mode))
      throw DmException(DM_NOT_DIRECTORY, "%s is not a directory", meta.name);
    // New element traversed! Need to check if it is possible to keep going.
    if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IEXEC) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to list %s", meta.name);

    // Pop next component
    c = components.front();
    components.pop_front();

    // Stay here
    if (c == ".") {
      continue;
    }
    // Up one level
    else if (c == "..") {
      meta   = this->extendedStat(parent);
      parent = meta.parent;
    }
    // Regular entry
    else {
      meta = this->extendedStat(parent, c);

      // Symbolic link!, follow that instead
      if (S_ISLNK(meta.stat.st_mode) && followSym) {
        SymLink link = this->readLink(meta.stat.st_ino);

        ++symLinkLevel;
        if (symLinkLevel > this->symLinkLimit_) {
          throw DmException(DM_TOO_MANY_SYMLINKS,
                           "Symbolic links limit exceeded: > %d",
                           this->symLinkLimit_);
        }

        // We have the symbolic link now. Split it and push
        // into the component
        std::list<std::string> symPath = splitPath(link.link);
        components.splice(components.begin(), symPath);
        // Push / if absolute
        if (link.link[0] == '/') {
          parent = 0;
          components.push_front("/");
        }
      }
      // Next one!
      else {
        parent = meta.stat.st_ino;
      }
    }
    
  }

  return meta;
}



void NsMySqlCatalog::traverseBackwards(const ExtendedStat& meta) throw (DmException)
{
  ExtendedStat current = meta;

  // We want to check if we can arrive here...
  while (current.parent != 0) {
    current = this->extendedStat(current.parent);
    if (checkPermissions(this->secCtx_, current.acl, current.stat, S_IEXEC))
      throw DmException(DM_FORBIDDEN, "Can not access #%ld",
                        current.stat.st_ino);
  }
}



void NsMySqlCatalog::changeDir(const std::string& path) throw (DmException)
{
  ExtendedStat cwd = this->extendedStat(path);
  this->cwdPath_ = path;
  this->cwd_     = cwd.stat.st_ino;
}



std::string NsMySqlCatalog::getWorkingDir(void) throw (DmException)
{
  return this->cwdPath_;
}



ino_t NsMySqlCatalog::getWorkingDirI() throw (DmException)
{
  return this->cwd_;
}



Directory* NsMySqlCatalog::openDir(const std::string& path) throw(DmException)
{
  NsMySqlDir  *dir;
  ExtendedStat meta;

  // Get the directory
  meta = this->extendedStat(path);
  
  // Can we read it?
  if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to read " + path);

  // Create the handle
  dir = new NsMySqlDir();
  dir->dirId = meta.stat.st_ino;
  
  try {
    dir->stmt = new Statement(this->conn_, this->nsDb_, STMT_GET_LIST_FILES);
    dir->stmt->bindParam(0, dir->dirId);
    dir->stmt->execute();
    bindMetadata(*dir->stmt, &dir->current);
    return dir;
  }
  catch (...) {
    delete dir;
    throw;
  }
}



void NsMySqlCatalog::closeDir(Directory* dir) throw(DmException)
{
  NsMySqlDir *dirp;

  if (dir == 0x00)
    throw DmException(DM_NULL_POINTER, std::string("Tried to close a null dir"));

  dirp = (NsMySqlDir*)dir;

  delete dirp->stmt;
  delete dirp;
}



struct dirent* NsMySqlCatalog::readDir(Directory* dir) throw(DmException)
{
  if (this->readDirx(dir) == 0)
    return 0x00;
  else
    return &(((NsMySqlDir*)dir)->ds);
}



ExtendedStat* NsMySqlCatalog::readDirx(Directory* dir) throw(DmException)
{
  NsMySqlDir *dirp;

  if (dir == 0x00)
    throw DmException(DM_NULL_POINTER, "Tried to read a null dir");

  dirp = (NsMySqlDir*)dir;

  if (dirp->stmt->fetch()) {
    memset(&dirp->ds, 0x00, sizeof(struct dirent));
    dirp->ds.d_ino  = dirp->current.stat.st_ino;
    strncpy(dirp->ds.d_name,
            dirp->current.name,
            sizeof(dirp->ds.d_name));

    // Touch
    struct utimbuf tim;
    tim.actime  = time(NULL);
    tim.modtime = dirp->current.stat.st_mtime;
    this->utime(dirp->dirId, &tim);

    return &dirp->current;
  }
  else {
    return 0x00;
  }
}



void NsMySqlCatalog::addReplica(const std::string& guid, int64_t id,
                                const std::string& server, const std::string& sfn,
                                char status, char fileType,
                                const std::string& poolName,
                                const std::string& fileSystem) throw (DmException)
{
  ExtendedStat meta;
  std::string  host;

  if (guid.empty())
    meta = this->extendedStat(id);
  else
    meta = this->guidStat(guid);

  // Access has to be checked backwards!
  this->traverseBackwards(meta);

  // Can write?
  if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to add the replica");

  // If server is empty, parse the surl
  if (server.empty()) {
    Uri u = splitUri(sfn);
    host = u.host;
    if (host.empty())
      throw DmException(DM_INVALID_VALUE, "Empty server specified, and SFN does not include it: " + sfn);
  }
  else {
    host = server;
  }

  // Add it
  Statement statement(this->conn_, this->nsDb_, STMT_ADD_REPLICA);

  statement.bindParam(0, meta.stat.st_ino);
  statement.bindParam(1, NULL, 0);
  statement.bindParam(2, std::string(&status, 1));
  statement.bindParam(3, std::string(&fileType, 1));
  statement.bindParam(4, NULL, 0);
  statement.bindParam(5, poolName);
  statement.bindParam(6, host);
  statement.bindParam(7, fileSystem);
  statement.bindParam(8, sfn);

  statement.execute();
}



void NsMySqlCatalog::deleteReplica(const std::string& guid, int64_t id,
                                   const std::string& sfn) throw (DmException)
{
  ExtendedStat meta;

  if (guid.empty())
    meta = this->extendedStat(id);
  else
    meta = this->guidStat(guid);

  // Access has to be checked backwards!
  this->traverseBackwards(meta);

  // Can write?
  if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to remove the replica");

  // Remove
  Statement statement(this->conn_, this->nsDb_, STMT_DELETE_REPLICA);
  statement.bindParam(0, meta.stat.st_ino);
  statement.bindParam(1, sfn);
  statement.execute();
}



std::vector<FileReplica> NsMySqlCatalog::getReplicas(const std::string& path) throw(DmException)
{
  ExtendedStat meta;

  // Need to grab the file first
  meta = this->extendedStat(path, true);

  // The file exists, plus we have permissions to go there. Check we can read
  if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN,
                   "Not enough permissions to read " + path);

  try {
    return this->getReplicas(meta.stat.st_ino);
  }
  catch (DmException e) {
    if (e.code() == DM_NO_REPLICAS)
      throw DmException(DM_NO_REPLICAS, "No replicas available for " + path);
    throw;
  }
}



std::vector<FileReplica> NsMySqlCatalog::getReplicas(ino_t ino) throw (DmException)
{
  FileReplica   replica;
  int           nReplicas;

  // MySQL statement
  Statement stmt(this->conn_, this->nsDb_, STMT_GET_FILE_REPLICAS);

  // Execute query
  stmt.bindParam(0, ino);
  stmt.execute();

  // Bind result
  stmt.bindResult( 0, &replica.replicaid);
  stmt.bindResult( 1, &replica.fileid);
  stmt.bindResult( 2, &replica.nbaccesses);
  stmt.bindResult( 3, &replica.atime);
  stmt.bindResult( 4, &replica.ptime);
  stmt.bindResult( 5, &replica.ltime);
  stmt.bindResult( 6, &replica.status, 1);
  stmt.bindResult( 7, &replica.type, 1);
  stmt.bindResult( 8, replica.pool,       sizeof(replica.pool));
  stmt.bindResult( 9, replica.server,     sizeof(replica.server));
  stmt.bindResult(10, replica.filesystem, sizeof(replica.filesystem));
  stmt.bindResult(11, replica.url,        sizeof(replica.url));

  std::vector<FileReplica> replicas;

  if ((nReplicas = stmt.count()) == 0)
    throw DmException(DM_NO_REPLICAS, "No replicas available for %ld", ino);

  // Fetch
  int i = 0;
  while (stmt.fetch()) {
    replicas.push_back(replica);
    ++i;
  };

  return replicas;
}



Uri NsMySqlCatalog::get(const std::string& path) throw(DmException)
{
  // Get all the replicas
  std::vector<FileReplica> replicas = this->getReplicas(path);

  // Pick a random one
  int i = rand() % replicas.size();

  // Copy
  return dmlite::splitUri(replicas[i].url);
}



NOT_IMPLEMENTED(std::string NsMySqlCatalog::put(const std::string&, Uri*) throw (DmException))
NOT_IMPLEMENTED(std::string NsMySqlCatalog::put(const std::string&, Uri*, const std::string&) throw (DmException))
NOT_IMPLEMENTED(void NsMySqlCatalog::putDone(const std::string&, const Uri&, const std::string&) throw (DmException))



void NsMySqlCatalog::symlink(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
  std::string parentPath, symName;
  
  // Get the parent of the destination and file
  ExtendedStat parent = this->getParent(newPath, &parentPath, &symName);

  // Check we have write access for the parent
  if (checkPermissions(this->secCtx_, parent.acl, parent.stat, S_IWRITE | S_IEXEC) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions on " + parentPath);

  // Transaction
  Transaction transaction(this->conn_);

  // Create the file entry
  ExtendedStat linkMeta = this->newFile(parent,
                                        symName, 0777 | S_IFLNK,
                                        1, 0, 0, '-',
                                        "", "", "");
  // Create the symlink entry
  Statement stmt(this->conn_, this->nsDb_, STMT_INSERT_SYMLINK);

  stmt.bindParam(0, linkMeta.stat.st_ino);
  stmt.bindParam(1, oldPath);

  stmt.execute();

  // Done
  transaction.commit();
}



void NsMySqlCatalog::unlink(const std::string& path) throw (DmException)
{
  std::string  parentPath, name;

  // Get the parent
  ExtendedStat parent = this->getParent(path, &parentPath, &name);

  // Check we have exec access for the parent
  if (checkPermissions(this->secCtx_, parent.acl, parent.stat, S_IEXEC) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to list " + parentPath);

  // The file itself
  ExtendedStat file = this->extendedStat(parent.stat.st_ino, name);

  // Directories can not be removed with this method!
  if (S_ISDIR(file.stat.st_mode))
    throw DmException(DM_IS_DIRECTORY, path + " is a directory, can not unlink");

  // Check we can remove it
  if ((parent.stat.st_mode & S_ISVTX) == S_ISVTX) {
    // Sticky bit set
    if (this->secCtx_->getUser().uid != file.stat.st_uid &&
        this->secCtx_->getUser().uid != parent.stat.st_uid &&
        checkPermissions(this->secCtx_, file.acl, file.stat, S_IWRITE) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to unlink " +
                                      path + "( sticky bit set)");
  }
  else {
    // No sticky bit
    if (checkPermissions(this->secCtx_, parent.acl, parent.stat, S_IWRITE) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to unlink " + path);
  }

  // Check there are no replicas
  if (!S_ISLNK(file.stat.st_mode)) {
    try {
      this->getReplicas(file.stat.st_ino);
      throw DmException(DM_EXISTS, path + " has replicas, can not remove");
    }
    catch (DmException e) {
      if (e.code() != DM_NO_REPLICAS)
        throw;
    }
  }

  // All preconditions are good! Start transaction.
  Transaction transaction(this->conn_);

  // Remove associated symlink
  Statement delSymlink(this->conn_, this->nsDb_, STMT_DELETE_SYMLINK);
  delSymlink.bindParam(0, file.stat.st_ino);
  delSymlink.execute();

  // Remove associated comments
  Statement delComment(this->conn_, this->nsDb_, STMT_DELETE_COMMENT);
  delComment.bindParam(0, file.stat.st_ino);
  delComment.execute();

  // Remove file itself
  Statement delFile(this->conn_, this->nsDb_, STMT_DELETE_FILE);
  delFile.bindParam(0, file.stat.st_ino);
  delFile.execute();

  // And decrement nlink
  Statement nlinkStmt(this->conn_, this->nsDb_, STMT_NLINK_FOR_UPDATE);
  nlinkStmt.bindParam(0, parent.stat.st_ino);
  nlinkStmt.execute();
  nlinkStmt.bindResult(0, &parent.stat.st_nlink);
  nlinkStmt.fetch();

  Statement nlinkUpdate(this->conn_, this->nsDb_, STMT_UPDATE_NLINK);
  parent.stat.st_nlink--;
  nlinkUpdate.bindParam(0, parent.stat.st_nlink);
  nlinkUpdate.bindParam(1, parent.stat.st_ino);
  nlinkUpdate.execute();

  // Done!
  transaction.commit();
}



void NsMySqlCatalog::create(const std::string& path, mode_t mode) throw (DmException)
{
  int          code;
  std::string  parentPath, name;
  ExtendedStat parent = this->getParent(path, &parentPath, &name);
  ExtendedStat file;

  try {
    file = this->extendedStat(parent.stat.st_ino, name);
    // File exists, check if it has replicas
    this->getReplicas(file.stat.st_ino);
    // It has replicas, so fail!
    throw DmException(DM_EXISTS, path + " exists and has replicas. Can not truncate.");
  }
  catch (DmException e) {
    code = e.code();
    if (code != DM_NO_SUCH_FILE && code != DM_NO_REPLICAS)
      throw;
  }

  // Create new
  Transaction transaction(this->conn_);
  if (code == DM_NO_SUCH_FILE) {
    mode_t newMode = ((mode & ~S_IFMT) & ~this->umask_);

    // Generate inherited ACL's if there are defaults
    std::string aclStr;
    if (strchr (parent.acl, ACL_DEFAULT | ACL_USER_OBJ  | '@') != NULL) {
      aclStr = serializeAcl(inheritAcl(deserializeAcl(parent.acl),
                                       this->secCtx_->getUser().uid,
                                       this->secCtx_->getGroup(0).gid,
                                       &newMode, mode));
    }

    this->newFile(parent, name, newMode,
                  1, 0, 0, '-',
                  std::string(), std::string(),
                  aclStr);
  }
  // Truncate
  else if (code == DM_NO_REPLICAS) {
    if (this->secCtx_->getUser().uid != file.stat.st_uid &&
        checkPermissions(this->secCtx_, file.acl, file.stat, S_IWRITE) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to truncate " + path);
    Statement statement(this->conn_, this->nsDb_, STMT_TRUNCATE_FILE);
    statement.bindParam(0, file.stat.st_ino);
    statement.execute();
  }
  transaction.commit();
}



mode_t NsMySqlCatalog::umask(mode_t mask) throw ()
{
  mode_t prev = this->umask_;
  this->umask_ = mask & 0777;
  return prev;
}



void NsMySqlCatalog::changeMode(const std::string& path, mode_t mode) throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path);

  // User has to be the owner, or root
  if (this->secCtx_->getUser().uid != meta.stat.st_uid &&
      this->secCtx_->getUser().uid != 0)
    throw DmException(DM_FORBIDDEN, "Only the owner can change the mode of " + path);

  // Clean up unwanted bits
  mode &= ~S_IFMT;
  if (!S_ISDIR(meta.stat.st_mode) && this->secCtx_->getUser().uid != 0)
    mode &= ~S_ISVTX;
  if (this->secCtx_->getUser().uid != 0 &&
      !this->secCtx_->hasGroup(meta.stat.st_gid))
    mode &= ~S_ISGID;

  // Update, keeping type bits from db.
  mode |= (meta.stat.st_mode & S_IFMT);

  // Update the ACL
  std::string aclStr;
  if (meta.acl[0] != '\0') {
    std::vector<Acl> acls = dmlite::deserializeAcl(meta.acl);
    for (size_t i = 0; i < acls.size(); ++i) {
      switch (acls[i].type) {
        case ACL_USER_OBJ:
          acls[i].perm = mode >> 6 & 07;
          break;
        case ACL_GROUP_OBJ:
        case ACL_MASK:
          acls[i].perm = mode >> 3 & 07;
          break;
        case ACL_OTHER:
          acls[i].perm = mode & 07;
          break;
      }
    }
    aclStr = dmlite::serializeAcl(acls);
  }

  // Update DB
  Statement stmt(this->conn_, this->nsDb_, STMT_UPDATE_PERMS);
  stmt.bindParam(0, meta.stat.st_uid);
  stmt.bindParam(1, meta.stat.st_gid);
  stmt.bindParam(2, mode);
  stmt.bindParam(3, aclStr);
  stmt.bindParam(4, meta.stat.st_ino);
  stmt.execute();
}



void NsMySqlCatalog::changeOwner(ExtendedStat& meta, uid_t newUid, gid_t newGid)
  throw (DmException)
{
  // If -1, no changes
  if (newUid == (uid_t)-1)
    newUid = meta.stat.st_uid;
  if (newGid == (gid_t)-1)
    newGid = meta.stat.st_gid;

  // Make sense?
  if (newUid == meta.stat.st_uid && newGid == meta.stat.st_gid)
    return;

  // If root, skip all checks
  if (this->secCtx_->getUser().uid != 0) {
    // Only root can change the owner
    if (meta.stat.st_uid != newUid)
      throw DmException(DM_BAD_OPERATION, "Only root can change the owner");
    // If the group is changing...
    if (meta.stat.st_gid != newGid) {
      // The user has to be the owner
      if (meta.stat.st_uid != this->secCtx_->getUser().uid)
        throw DmException(DM_BAD_OPERATION, "Only root or the owner can change the group");
      // AND it has to belong to that group
      if (!this->secCtx_->hasGroup(newGid))
        throw DmException(DM_BAD_OPERATION, "The user does not belong to the group %d", newGid);
      // If it does, the group exists :)
    }
  }

  // Update the ACL's if there is any
  std::string aclStr;
  if (meta.acl[0] != '\0') {
    std::vector<Acl> acls = dmlite::deserializeAcl(meta.acl);
    for (size_t i = 0; i < acls.size(); ++i) {
      if (acls[i].type == ACL_USER_OBJ)
        acls[i].id = newUid;
      else if (acls[i].type == ACL_GROUP_OBJ)
        acls[i].id = newGid;
    }
    aclStr = dmlite::serializeAcl(acls);
  }

  // Change!
  Statement stmt(this->conn_, this->nsDb_, STMT_UPDATE_PERMS);
  stmt.bindParam(0, newUid);
  stmt.bindParam(1, newGid);
  stmt.bindParam(2, meta.stat.st_mode);
  stmt.bindParam(3, aclStr);
  stmt.bindParam(4, meta.stat.st_ino);
  stmt.execute();
}



void NsMySqlCatalog::changeOwner(const std::string& path, uid_t newUid, gid_t newGid)
  throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path);
  this->changeOwner(meta, newUid, newGid);
}



void NsMySqlCatalog::linkChangeOwner(const std::string& path, uid_t newUid, gid_t newGid)
  throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path, false);
  this->changeOwner(meta, newUid, newGid);
}



void NsMySqlCatalog::changeSize(const std::string& path, size_t newSize) throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path, false);
  if (this->secCtx_->getUser().uid != meta.stat.st_uid &&
      checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Can not change the size of " + path);
  
  Statement stmt(this->conn_, this->nsDb_, STMT_CHANGE_SIZE);
  stmt.bindParam(0, newSize);
  stmt.bindParam(1, meta.stat.st_ino);
  stmt.execute();
}



void NsMySqlCatalog::setAcl(const std::string& path, const std::vector<Acl>& acls) throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path);

  // Check we can change it
  if (this->secCtx_->getUser().uid != meta.stat.st_uid &&
      this->secCtx_->getUser().uid != 0)
    throw DmException(DM_FORBIDDEN, "Only the owner can change the ACL of " + path);

  std::vector<Acl> aclsCopy(acls);

  // Make sure the owner and group matches!
  for (size_t i = 0; i < aclsCopy.size(); ++i) {
    if (aclsCopy[i].type == ACL_USER_OBJ)
      aclsCopy[i].id = meta.stat.st_uid;
    else if (aclsCopy[i].type == ACL_GROUP_OBJ)
      aclsCopy[i].id = meta.stat.st_gid;
    else if (aclsCopy[i].type & ACL_DEFAULT && !S_ISDIR(meta.stat.st_mode))
      throw DmException(DM_INVALID_ACL, "Defaults can be only applied to directories");
  }

  // Validate the ACL
  dmlite::validateAcl(aclsCopy);

  // Update the file mode
  for (size_t i = 0; i < aclsCopy.size(); ++i) {
    switch (aclsCopy[i].type) {
      case ACL_USER_OBJ:
        meta.stat.st_mode = (meta.stat.st_mode & 0177077) | (aclsCopy[i].perm << 6);
        break;
      case ACL_GROUP_OBJ:
        meta.stat.st_mode = (meta.stat.st_mode & 0177707) | (aclsCopy[i].perm << 3);
        break;
      case ACL_MASK:
        meta.stat.st_mode = (meta.stat.st_mode & ~070) | (meta.stat.st_mode & aclsCopy[i].perm << 3);
        break;
      case ACL_OTHER:
        meta.stat.st_mode = (meta.stat.st_mode & 0177770) | (aclsCopy[i].perm);
        break;
    }
  }

  // If only 3 entries, no extended ACL
  std::string aclStr;
  if (aclsCopy.size() > 3)
    aclStr = dmlite::serializeAcl(aclsCopy);

  // Update the DB
  Statement stmt(this->conn_, this->nsDb_, STMT_UPDATE_PERMS);
  stmt.bindParam(0, meta.stat.st_uid);
  stmt.bindParam(1, meta.stat.st_gid);
  stmt.bindParam(2, meta.stat.st_mode);
  stmt.bindParam(3, aclStr);
  stmt.bindParam(4, meta.stat.st_ino);
  
  stmt.execute();
}



void NsMySqlCatalog::utime(const std::string& path, const struct utimbuf* buf) throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path);

  // The user is the owner OR buf is NULL and has write permissions
  if (this->secCtx_->getUser().uid != meta.stat.st_uid &&
      checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to modify the time of " + path);

  // Touch
  this->utime(meta.stat.st_ino, buf);
}



void NsMySqlCatalog::utime(ino_t inode, const struct utimbuf* buf) throw (DmException)
{
  // If NULL, point to ours!
  struct utimbuf internal;
  if (buf == 0x00) {
    buf = &internal;
    internal.actime  = time(NULL);
    internal.modtime = time(NULL);
  }

  // Change
  Statement stmt(this->conn_, this->nsDb_, STMT_UTIME);
  stmt.bindParam(0, buf->actime);
  stmt.bindParam(1, buf->modtime);
  stmt.bindParam(2, inode);

  stmt.execute();
}



std::string NsMySqlCatalog::getComment(const std::string& path) throw(DmException)
{
  char         comment[COMMENT_MAX];
  
  // Get the file and check we can read
  ExtendedStat meta = this->extendedStat(path);
  
  if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to read " + path);

  // Query
  Statement stmt(this->conn_, this->nsDb_, STMT_GET_COMMENT);

  stmt.bindParam(0, meta.stat.st_ino);
  stmt.execute();
  
  stmt.bindResult(0, comment, COMMENT_MAX);
  if (!stmt.fetch())
    throw DmException(DM_NO_COMMENT, "There is no comment for " + path);

  // Done here!
  return std::string(comment);
}



void NsMySqlCatalog::setComment(const std::string& path, const std::string& comment) throw (DmException)
{
  // Get the file and check we can write
  ExtendedStat meta = this->extendedStat(path);

  if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to write " + path);

  // Query
  Statement stmt(this->conn_, this->nsDb_, STMT_SET_COMMENT);

  stmt.bindParam(0, comment);
  stmt.bindParam(1, meta.stat.st_ino);

  if (stmt.execute() == 0) {
    // No update! Try inserting
    Statement stmti(this->conn_, this->nsDb_, STMT_INSERT_COMMENT);

    stmti.bindParam(0, meta.stat.st_ino);
    stmti.bindParam(1, comment);
    
    stmti.execute();
  }
}



void NsMySqlCatalog::setGuid(const std::string& path, const std::string& guid) throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path);

  if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to write " + path);

  // Query
  Statement stmt(this->conn_, this->nsDb_, STMT_SET_GUID);

  stmt.bindParam(0, guid);
  stmt.bindParam(1, meta.stat.st_ino);

  stmt.execute();
}



void NsMySqlCatalog::makeDir(const std::string& path, mode_t mode) throw (DmException)
{
  std::string parentPath, name;

  // Get the parent of the new folder
  ExtendedStat parent = this->getParent(path, &parentPath, &name);

  // Check we have write access for the parent
  if (checkPermissions(this->secCtx_, parent.acl, parent.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Need write access for " + parentPath);

  // Mode
  mode_t newMode = ((mode & ~S_IFMT) & ~this->umask_) | S_IFDIR;

  // Generate inherited ACL's if there are defaults
  std::string aclStr;
  if (strchr (parent.acl, ACL_DEFAULT | ACL_USER_OBJ  | '@') != NULL) {
    aclStr = serializeAcl(inheritAcl(deserializeAcl(parent.acl),
                                     this->secCtx_->getUser().uid,
                                     this->secCtx_->getGroup(0).gid,
                                     &newMode, mode));
  }

  // Create the file entry
  Transaction transaction(this->conn_);
  this->newFile(parent, name, newMode,
                0, 0, parent.type, '-', "", "", aclStr);
  transaction.commit();
}



void NsMySqlCatalog::removeDir(const std::string& path) throw (DmException)
{
  std::string parentPath, name;

  // Fail inmediately with '/'
  if (path == "/")
    throw DmException(DM_INVALID_VALUE, "Can not remove '/'");

  // Get the parent of the new folder
  ExtendedStat parent = this->getParent(path, &parentPath, &name);

  // Get the file, and check it is a directory and it is empty
  ExtendedStat entry = this->extendedStat(parent.stat.st_ino, name);

  if (!S_ISDIR(entry.stat.st_mode))
    throw DmException(DM_NOT_DIRECTORY, path + " is not a directory. Can not remove.");

  if (this->cwd_ == entry.stat.st_ino)
    throw DmException(DM_IS_CWD, "Can not remove the current working dir");

  if (entry.stat.st_nlink > 0)
    throw DmException(DM_EXISTS, path + " is not empty. Can not remove.");

  // Check we can remove it
  if ((parent.stat.st_mode & S_ISVTX) == S_ISVTX) {
    // Sticky bit set
    if (this->secCtx_->getUser().uid != entry.stat.st_uid &&
        this->secCtx_->getUser().uid != parent.stat.st_uid &&
        checkPermissions(this->secCtx_, entry.acl, entry.stat, S_IWRITE) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to remove " +
                                      path + "( sticky bit set)");
  }
  else {
    // No sticky bit
    if (checkPermissions(this->secCtx_, parent.acl, parent.stat, S_IWRITE) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to remove " + path);
  }

  // All preconditions are good!
  Transaction transaction(this->conn_);

  // Remove associated comments
  Statement delComment(this->conn_, this->nsDb_, STMT_DELETE_COMMENT);
  delComment.bindParam(0, entry.stat.st_ino);
  delComment.execute();

  // Remove directory itself
  Statement delDir(this->conn_, this->nsDb_, STMT_DELETE_FILE);
  delDir.bindParam(0, entry.stat.st_ino);
  delDir.execute();

  // And decrement nlink
  Statement nlinkStmt(this->conn_, this->nsDb_, STMT_NLINK_FOR_UPDATE);
  nlinkStmt.bindParam(0, parent.stat.st_ino);
  nlinkStmt.execute();
  nlinkStmt.bindResult(0, &parent.stat.st_nlink);
  nlinkStmt.fetch();

  Statement nlinkUpdate(this->conn_, this->nsDb_, STMT_UPDATE_NLINK);
  parent.stat.st_nlink--;
  nlinkUpdate.bindParam(0, parent.stat.st_nlink);
  nlinkUpdate.bindParam(1, parent.stat.st_ino);
  nlinkUpdate.execute();

  // Done!
  transaction.commit();
}



void NsMySqlCatalog::rename(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
  std::string oldParentPath, newParentPath;
  std::string oldName,       newName;

  // Do not even bother with '/'
  if (oldPath == "/" || newPath == "/")
    throw DmException(DM_INVALID_VALUE, "Not the source, neither the destination, can be '/'");

  // Get source and destination parent
  ExtendedStat oldParent = this->getParent(oldPath, &oldParentPath, &oldName);
  ExtendedStat newParent = this->getParent(newPath, &oldParentPath, &newName);

  // Source
  ExtendedStat old = this->extendedStat(oldParent.stat.st_ino, oldName);

  // Is the cwd?
  if (old.stat.st_ino == this->cwd_) {
    throw DmException(DM_IS_CWD, "Can not rename the current working directory");
  }

  // Need write permissions in both origin and destination
  if (checkPermissions(this->secCtx_, oldParent.acl, oldParent.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions on origin " + oldParentPath);

  if (checkPermissions(this->secCtx_, newParent.acl, newParent.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions on destination " + newParentPath);

  // If source is a directory, need write permissions there too
  if (S_ISDIR(old.stat.st_mode)) {
    if (checkPermissions(this->secCtx_, old.acl, old.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions on " + oldPath);

    // AND destination can not be a child
    ExtendedStat aux = newParent;

    while (aux.parent > 0) {
      if (aux.stat.st_ino == old.stat.st_ino)
        throw DmException(DM_INVALID_VALUE, "Destination is descendant of source");
      aux = this->extendedStat(aux.parent);
    }
  }

  // Check sticky
  if (oldParent.stat.st_mode & S_ISVTX &&
      this->secCtx_->getUser().uid != oldParent.stat.st_uid &&
      this->secCtx_->getUser().uid != old.stat.st_uid &&
      checkPermissions(this->secCtx_, old.acl, old.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Sticky bit set on the parent, and not enough permissions");

  // If the destination exists...
  try {
    ExtendedStat newF = this->extendedStat(newParent.stat.st_ino, newName);

    // If it is the same, leave the function
    if (newF.stat.st_ino == old.stat.st_ino)
      return;

    // It does! It has to be the same type
    if ((newF.stat.st_mode & S_IFMT) != (old.stat.st_mode & S_IFMT)) {
      if (S_ISDIR(old.stat.st_mode))
        throw DmException(DM_NOT_DIRECTORY, "Source is a directory and destination is not");
      else
        throw DmException(DM_IS_DIRECTORY, "Source is not directory and destination is");
    }

    // And it has to be empty. Just call remove or unlink
    // and they will fail if it is not
    if (S_ISDIR(newF.stat.st_mode))
      this->removeDir(newPath);
    else
      this->unlink(newPath);
  }
  catch (DmException e) {
    if (e.code() != DM_NO_SUCH_FILE)
      throw;
  }

  // We are good, so we can move now
  Transaction transaction(this->conn_);

  // Change the name if needed
  if (newName != oldName) {
    Statement changeNameStmt(this->conn_, this->nsDb_, STMT_CHANGE_NAME);

    changeNameStmt.bindParam(0, newName);
    changeNameStmt.bindParam(1, old.stat.st_ino);

    if (changeNameStmt.execute() == 0)
      throw DmException(DM_INTERNAL_ERROR, "Could not change the name");
  }

  // Change the parent if needed
  if (newParent.stat.st_ino != oldParent.stat.st_ino) {
    Statement changeParentStmt(this->conn_, this->nsDb_, STMT_CHANGE_PARENT);

    changeParentStmt.bindParam(0, newParent.stat.st_ino);
    changeParentStmt.bindParam(1, old.stat.st_ino);

    if (changeParentStmt.execute() == 0)
      throw DmException(DM_INTERNAL_ERROR, "Could not update the parent ino!");

    // Reduce nlinks from old
    Statement oldNlinkStmt(this->conn_, this->nsDb_, STMT_NLINK_FOR_UPDATE);
    oldNlinkStmt.bindParam(0, oldParent.stat.st_ino);
    oldNlinkStmt.execute();
    oldNlinkStmt.bindResult(0, &oldParent.stat.st_nlink);
    oldNlinkStmt.fetch();

    Statement oldNlinkUpdateStmt(this->conn_, this->nsDb_, STMT_UPDATE_NLINK);

    oldParent.stat.st_nlink--;
    oldNlinkUpdateStmt.bindParam(0, oldParent.stat.st_nlink);
    oldNlinkUpdateStmt.bindParam(1, oldParent.stat.st_ino);

    if (oldNlinkUpdateStmt.execute() == 0)
      throw DmException(DM_INTERNAL_ERROR, "Could not update the old parent nlink!");

    // Increment from new
    Statement newNlinkStmt(this->conn_, this->nsDb_, STMT_NLINK_FOR_UPDATE);
    newNlinkStmt.bindParam(0, newParent.stat.st_ino);
    newNlinkStmt.execute();
    newNlinkStmt.bindResult(0, &newParent.stat.st_nlink);
    newNlinkStmt.fetch();

    Statement newNlinkUpdateStmt(this->conn_, this->nsDb_, STMT_UPDATE_NLINK);

    newParent.stat.st_nlink++;
    newNlinkUpdateStmt.bindParam(0, newParent.stat.st_nlink);
    newNlinkUpdateStmt.bindParam(1, newParent.stat.st_ino);

    if (newNlinkUpdateStmt.execute() == 0)
      throw DmException(DM_INTERNAL_ERROR, "Could not update the new parent nlink!");
  }

  // Done!
  transaction.commit();
}



FileReplica NsMySqlCatalog::replicaGet(const std::string& replica) throw (DmException)
{
  Statement stmt(this->conn_, this->nsDb_, STMT_GET_REPLICA_BY_URL);
  stmt.bindParam(0, replica);
  
  stmt.execute();

  FileReplica r;

  stmt.bindResult( 0, &r.replicaid);
  stmt.bindResult( 1, &r.fileid);
  stmt.bindResult( 2, &r.nbaccesses);
  stmt.bindResult( 3, &r.atime);
  stmt.bindResult( 4, &r.ptime);
  stmt.bindResult( 5, &r.ltime);
  stmt.bindResult( 6, &r.status, 1);
  stmt.bindResult( 7, &r.type, 1);
  stmt.bindResult( 8, r.pool,       sizeof(r.pool));
  stmt.bindResult( 9, r.server,     sizeof(r.server));
  stmt.bindResult(10, r.filesystem, sizeof(r.filesystem));
  stmt.bindResult(11, r.url,        sizeof(r.url));

  if (!stmt.fetch())
    throw DmException(DM_NO_REPLICAS, "Replica " + replica + " not found");

  return r;
}



void NsMySqlCatalog::replicaSet(const FileReplica& rdata) throw (DmException)
{
  /* Get associated file */
  ExtendedStat meta = this->extendedStat(rdata.fileid);

  /* Check we can actually go here */
  this->traverseBackwards(meta);

  /* Check the user can modify */
  if (this->secCtx_->getUser().uid != meta.stat.st_uid &&
      checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to modify the replica");

  /* Update */
  Statement stmt(this->conn_, this->nsDb_, STMT_UPDATE_REPLICA);
  stmt.bindParam(0, rdata.atime);
  stmt.bindParam(1, rdata.ltime);
  stmt.bindParam(2, rdata.nbaccesses);
  stmt.bindParam(3, std::string(&rdata.status, 1));
  stmt.bindParam(4, std::string(&rdata.type, 1));
  stmt.bindParam(5, rdata.replicaid);

  stmt.execute();
}



void NsMySqlCatalog::replicaSetAccessTime(const std::string& replica) throw (DmException)
{
  FileReplica rdata = this->replicaGet(replica);
  rdata.atime = time(NULL);
  this->replicaSet(rdata);
}



void NsMySqlCatalog::replicaSetLifeTime(const std::string& replica, time_t ltime) throw (DmException)
{
  FileReplica rdata = this->replicaGet(replica);
  rdata.ltime = ltime;
  this->replicaSet(rdata);
}



void NsMySqlCatalog::replicaSetStatus(const std::string& replica, char status) throw (DmException)
{
  FileReplica rdata = this->replicaGet(replica);
  rdata.status = status;
  this->replicaSet(rdata);
}



void NsMySqlCatalog::replicaSetType(const std::string& replica, char type) throw (DmException)
{
  FileReplica rdata = this->replicaGet(replica);
  rdata.type = type;
  this->replicaSet(rdata);
}



void NsMySqlCatalog::getIdMap(const std::string& userName, const std::vector<std::string>& groupNames,
                              UserInfo* user, std::vector<GroupInfo>* groups) throw (DmException)
{
  std::string vo;
  GroupInfo   group;

  // Clear
  groups->clear();

  // User mapping
  try {
    *user = this->getUser(userName);
  }
  catch (DmException e) {
    if (e.code() == DM_NO_SUCH_USER)
      *user = this->newUser(userName, "");
    else
      throw;
  }

  // No VO information, so use the mapping file to get the group
  if (groupNames.empty()) {
    vo = voFromDn("/etc/lcgdm-mapfile", userName);
    try {
      group = this->getGroup(vo);
    }
    catch (DmException e) {
      if (e.code() == DM_NO_SUCH_GROUP)
        group = this->newGroup(vo);
      else
        throw;
    }
    groups->push_back(group);
  }
  else {
    // Get group info
    std::vector<std::string>::const_iterator i;
    for (i = groupNames.begin(); i != groupNames.end(); ++i) {
      vo = voFromRole(*i);
      try {
        group = this->getGroup(vo);
      }
      catch (DmException e) {
        if (e.code() == DM_NO_SUCH_GROUP)
          group = this->newGroup(vo);
        else
          throw;
      }
      groups->push_back(group);
    }
  }
}



UserInfo NsMySqlCatalog::getUser(const std::string& userName) throw(DmException)
{
  UserInfo  user;
  Statement stmt(this->conn_, this->nsDb_, STMT_GET_USERINFO_BY_NAME);

  stmt.bindParam(0, userName);
  stmt.execute();

  stmt.bindResult(0, &user.uid);
  stmt.bindResult(1, user.name, sizeof(user.name));
  stmt.bindResult(2, user.ca, sizeof(user.ca));
  stmt.bindResult(3, &user.banned);

  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_USER, "User " + userName + " not found");

  return user;
}



UserInfo NsMySqlCatalog::getUser(uid_t uid) throw(DmException)
{
  UserInfo  user;
  Statement stmt(this->conn_, this->nsDb_, STMT_GET_USERINFO_BY_UID);

  stmt.bindParam(0, uid);
  stmt.execute();

  stmt.bindResult(0, &user.uid);
  stmt.bindResult(1, user.name, sizeof(user.name));
  stmt.bindResult(2, user.ca, sizeof(user.ca));
  stmt.bindResult(3, &user.banned);

  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_USER, "User %ld not found", uid);

  return user;
}



GroupInfo NsMySqlCatalog::getGroup(const std::string& groupName) throw(DmException)
{
  GroupInfo group;
  Statement stmt(this->conn_, this->nsDb_, STMT_GET_GROUPINFO_BY_NAME);

  stmt.bindParam(0, groupName);
  stmt.execute();

  stmt.bindResult(0, &group.gid);
  stmt.bindResult(1, group.name, sizeof(group.name));
  stmt.bindResult(2, &group.banned);

  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_GROUP, "Group " + groupName + " not found");
  
  return group;
}



GroupInfo NsMySqlCatalog::getGroup(gid_t gid) throw(DmException)
{
  GroupInfo group;
  Statement stmt(this->conn_, this->nsDb_, STMT_GET_GROUPINFO_BY_GID);

  stmt.bindParam(0, gid);
  stmt.execute();

  stmt.bindResult(0, &group.gid);
  stmt.bindResult(1, group.name, sizeof(group.name));
  stmt.bindResult(2, &group.banned);

  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_GROUP, "Group %ld not found", gid);

  return group;
}



UserInfo NsMySqlCatalog::newUser(const std::string& uname, const std::string& ca) throw (DmException)
{
  Transaction transaction(this->conn_);

  // Get the last uid, increment and update
  Statement uidStmt(this->conn_, this->nsDb_, STMT_GET_UNIQ_UID_FOR_UPDATE);
  uid_t     uid;

  uidStmt.execute();
  uidStmt.bindResult(0, &uid);

  // Update the uid
  if (uidStmt.fetch()) {
    Statement updateUidStmt(this->conn_, this->nsDb_, STMT_UPDATE_UNIQ_UID);
    ++uid;
    updateUidStmt.bindParam(0, uid);
    updateUidStmt.execute();
  }
  // Couldn't get, so insert it instead
  else {
    Statement insertUidStmt(this->conn_, this->nsDb_, STMT_INSERT_UNIQ_UID);
    uid = 1;
    insertUidStmt.bindParam(0, uid);
    insertUidStmt.execute();
  }

  // Insert the user
  Statement userStmt(this->conn_, this->nsDb_, STMT_INSERT_USER);

  userStmt.bindParam(0, uid);
  userStmt.bindParam(1, uname);
  userStmt.bindParam(2, ca);
  userStmt.bindParam(3, 0);

  userStmt.execute();

  // Commit
  transaction.commit();

  // Build and return the UserInfo
  UserInfo u;
  u.uid = uid;
  strncpy(u.name, uname.c_str(), sizeof(u.name));
  strncpy(u.ca,   ca.c_str(),    sizeof(u.ca));
  u.banned = 0;

  return u;
}



GroupInfo NsMySqlCatalog::newGroup(const std::string& gname) throw (DmException)
{
  Transaction transaction(this->conn_);

  // Get the last gid, increment and update
  Statement gidStmt(this->conn_, this->nsDb_, STMT_GET_UNIQ_GID_FOR_UPDATE);
  gid_t     gid;

  gidStmt.execute();
  gidStmt.bindResult(0, &gid);

  // Update the gid
  if (gidStmt.fetch()) {
    Statement updateGidStmt(this->conn_, this->nsDb_, STMT_UPDATE_UNIQ_GID);
    ++gid;
    updateGidStmt.bindParam(0, gid);
    updateGidStmt.execute();
  }
  // Couldn't get, so insert it instead
  else {
    Statement insertGidStmt(this->conn_, this->nsDb_, STMT_INSERT_UNIQ_GID);
    gid = 1;
    insertGidStmt.bindParam(0, gid);
    insertGidStmt.execute();
  }

  // Insert the group
  Statement groupStmt(this->conn_, this->nsDb_, STMT_INSERT_GROUP);

  groupStmt.bindParam(0, gid);
  groupStmt.bindParam(1, gname);
  groupStmt.bindParam(2, 0);

  groupStmt.execute();

  // Commit
  transaction.commit();

  // Build and return the GroupInfo
  GroupInfo g;
  g.gid = gid;
  strncpy(g.name, gname.c_str(), sizeof(g.name));
  g.banned = 0;

  return g;
}
