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

using namespace dmlite;


/// Used to keep prepared statements
enum {
  STMT_GET_FILE_BY_ID = 0,
  STMT_GET_FILE_BY_GUID,
  STMT_GET_FILE_BY_NAME,
  STMT_GET_LIST_FILES,
  STMT_GET_SYMLINK,
  STMT_GET_USERINFO_BY_NAME,
  STMT_GET_USERINFO_BY_UID,
  STMT_GET_GROUPINFO_BY_NAME,
  STMT_GET_GROUPINFO_BY_GID,
  STMT_GET_FILE_REPLICAS,
  STMT_GET_FILE_REPLICAS_EXTENDED,
  STMT_GET_COMMENT,
  STMT_SET_GUID,
  STMT_SET_COMMENT,
  STMT_INSERT_COMMENT,
  STMT_INSERT_FILE,
  STMT_INSERT_SYMLINK,
  STMT_SELECT_UNIQ_ID_FOR_UPDATE,
  STMT_UPDATE_UNIQ_ID,
  STMT_DELETE_FILE,
  STMT_DELETE_COMMENT,
  STMT_DELETE_SYMLINK,
  STMT_UPDATE_NLINK,
  STMT_UPDATE_MODE,
  STMT_DELETE_REPLICA,
  STMT_ADD_REPLICA,
  STMT_TRUNCATE_FILE,
  STMT_SENTINEL
};

/// Used internally to define prepared statements.
/// Must match with STMT_* constants!
static const char* statements[] = {
  "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
          filesize, atime, mtime, ctime, fileclass, status,\
          csumtype, csumvalue, acl\
        FROM Cns_file_metadata\
        WHERE fileid = ?",
  "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
          filesize, atime, mtime, ctime, fileclass, status,\
          csumtype, csumvalue, acl\
        FROM Cns_file_metadata\
        WHERE guid = ?",
  "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
          filesize, atime, mtime, ctime, fileclass, status,\
          csumtype, csumvalue, acl\
        FROM Cns_file_metadata \
        WHERE parent_fileid = ? AND name = ?",
  "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
          filesize, atime, mtime, ctime, fileclass, status,\
          csumtype, csumvalue, acl\
        FROM Cns_file_metadata \
        WHERE parent_fileid = ?",
  "SELECT fileid, linkname FROM Cns_symlinks WHERE fileid = ?",
  "SELECT userid, username, user_ca, banned\
        FROM Cns_userinfo\
        WHERE username = ?",
  "SELECT userid, username, user_ca, banned\
        FROM Cns_userinfo\
        WHERE userid = ?",
  "SELECT gid, groupname, banned\
        FROM Cns_groupinfo\
        WHERE groupname = ?",
  "SELECT gid, groupname, banned\
        FROM Cns_groupinfo\
        WHERE gid = ?",
  "SELECT rowid, fileid, status, sfn\
        FROM Cns_file_replica\
        WHERE fileid = ?",
  "SELECT rowid, fileid, status, sfn, poolname, host, fs\
        FROM Cns_file_replica\
        WHERE fileid = ?",
  "SELECT comments\
        FROM Cns_user_metadata\
        WHERE u_fileid = ?",
  "UPDATE Cns_file_metadata\
        SET guid = ?\
        WHERE fileid = ?",
  "UPDATE Cns_user_metadata\
        SET comments = ?\
        WHERE u_fileid = ?",
  "INSERT INTO Cns_user_metadata\
          (u_fileid, comments)\
        VALUES\
          (?, ?)",
  "INSERT INTO Cns_file_metadata\
          (fileid, parent_fileid, name, filemode, nlink, owner_uid, gid,\
           filesize, atime, mtime, ctime, fileclass, status,\
           csumtype, csumvalue, acl)\
        VALUES\
          (?, ?, ?, ?, ?, ?, ?,\
           ?, UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), ?, ?,\
           ?, ?, ?)",
  "INSERT INTO Cns_symlinks\
          (fileid, linkname)\
        VALUES\
          (?, ?)",
  "SELECT id FROM Cns_unique_id FOR UPDATE",
  "UPDATE Cns_unique_id SET id = ?",
  "DELETE FROM Cns_file_metadata WHERE fileid = ?",
  "DELETE FROM Cns_user_metadata WHERE u_fileid = ?",
  "DELETE FROM Cns_symlinks WHERE fileid = ?",
  "UPDATE Cns_file_metadata\
        SET nlink = ?, mtime = UNIX_TIMESTAMP(), ctime = UNIX_TIMESTAMP()\
        WHERE fileid = ?",
  "UPDATE Cns_file_metadata\
        SET filemode = ?\
        WHERE fileid = ?",
  "DELETE FROM Cns_file_replica\
        WHERE fileid = ? AND sfn = ?",
  "INSERT INTO Cns_file_replica\
          (fileid, nbaccesses,\
           ctime, atime, ptime, ltime,\
           r_type, status, f_type,\
           setname, poolname, host, fs, sfn)\
        VALUES\
          (?, 0,\
           UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), UNIX_TIMESTAMP(),\
           ?, ?, ?,\
           ?, ?, ?, ?, ?)",
  "UPDATE Cns_file_metadata\
        SET filesize = 0 WHERE fileid = ?",
};



MYSQL_STMT* NsMySqlCatalog::getPreparedStatement(unsigned stId)
{
  if (this->preparedStmt_[stId] == 0x00) {

    if (mysql_select_db(this->conn_, this->nsDb_.c_str()) != 0)
      throw DmException(DM_QUERY_FAILED, std::string("Select DB: ") + mysql_error(this->conn_));

    this->preparedStmt_[stId] = mysql_stmt_init(this->conn_);
    if (mysql_stmt_prepare(this->preparedStmt_[stId],
                           statements[stId], std::strlen(statements[stId])) != 0) {
      throw DmException(DM_QUERY_FAILED, std::string("Prepare: ") + mysql_stmt_error(this->preparedStmt_[stId]));
    }
  }

  return this->preparedStmt_[stId];
}



NsMySqlCatalog::NsMySqlCatalog(PoolContainer<MYSQL*>* connPool, const std::string& db,
                               Catalog* decorates,
                               unsigned int symLinkLimit) throw(DmException):
                DummyCatalog(decorates), umask_(022),  nsDb_(db), symLinkLimit_(symLinkLimit),
                preparedStmt_(STMT_SENTINEL, 0x00)
{
  this->connectionPool_ = connPool;
  this->conn_           = connPool->acquire();
  std::memset(&this->user_,  0x00, sizeof(UserInfo));
  std::memset(&this->group_, 0x00, sizeof(GroupInfo));
}



NsMySqlCatalog::~NsMySqlCatalog() throw(DmException)
{
  std::vector<MYSQL_STMT*>::iterator i;

  for (i = this->preparedStmt_.begin(); i != this->preparedStmt_.end(); i++) {
    if (*i != 0x00)
      mysql_stmt_close(*i);
  }
  this->connectionPool_->release(this->conn_);
}



std::string NsMySqlCatalog::getImplId() throw ()
{
  return std::string("NsMySqlCatalog");
}



static void bindMetadata(Statement& stmt, FileMetadata* meta) throw(DmException)
{
  stmt.bindResult( 0, &meta->xStat.stat.st_ino);
  stmt.bindResult( 1, &meta->xStat.parent);
  stmt.bindResult( 2, meta->xStat.guid, sizeof(meta->xStat.guid));
  stmt.bindResult( 3, meta->xStat.name, sizeof(meta->xStat.name));
  stmt.bindResult( 4, &meta->xStat.stat.st_mode);
  stmt.bindResult( 5, &meta->xStat.stat.st_nlink);
  stmt.bindResult( 6, &meta->xStat.stat.st_uid);
  stmt.bindResult( 7, &meta->xStat.stat.st_gid);
  stmt.bindResult( 8, &meta->xStat.stat.st_size);
  stmt.bindResult( 9, &meta->xStat.stat.st_atime);
  stmt.bindResult(10, &meta->xStat.stat.st_mtime);
  stmt.bindResult(11, &meta->xStat.stat.st_ctime);
  stmt.bindResult(12, &meta->xStat.type);
  stmt.bindResult(13, &meta->xStat.status, 1);
  stmt.bindResult(14, meta->xStat.csumtype,  sizeof(meta->xStat.csumtype));
  stmt.bindResult(15, meta->xStat.csumvalue, sizeof(meta->xStat.csumvalue));
  stmt.bindResult(16, meta->acl, sizeof(meta->acl), 0);
}



FileMetadata NsMySqlCatalog::getFile(uint64_t fileId) throw(DmException)
{
  Statement    stmt(this->getPreparedStatement(STMT_GET_FILE_BY_ID));
  FileMetadata meta;

  memset(&meta, 0x00, sizeof(FileMetadata));

  stmt.bindParam(0, fileId);
  stmt.execute();
  bindMetadata(stmt, &meta);

  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_FILE, "File %ld not found", fileId);

  return meta;
}



FileMetadata NsMySqlCatalog::getFile(const std::string& guid) throw (DmException)
{
  Statement    stmt(this->getPreparedStatement(STMT_GET_FILE_BY_GUID));
  FileMetadata meta;

  memset(&meta, 0x00, sizeof(FileMetadata));

  stmt.bindParam(0, guid);
  stmt.execute();

  bindMetadata(stmt, &meta);

  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_FILE, "File with guid " + guid + " not found");

  return meta;
}



FileMetadata NsMySqlCatalog::getFile(const std::string& name, uint64_t parent) throw(DmException)
{
  Statement    stmt(this->getPreparedStatement(STMT_GET_FILE_BY_NAME));
  FileMetadata meta;

  memset(&meta, 0x00, sizeof(FileMetadata));

  stmt.bindParam(0, parent);
  stmt.bindParam(1, name);
  stmt.execute();

  bindMetadata(stmt, &meta);

  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_FILE, name + " not found");

  return meta;
}



SymLink NsMySqlCatalog::getLink(uint64_t linkId) throw(DmException)
{
  Statement stmt(this->getPreparedStatement(STMT_GET_SYMLINK));
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



FileMetadata NsMySqlCatalog::newFile(FileMetadata& parent, const std::string& name,
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
    FileMetadata f = this->getFile(name, parent.xStat.stat.st_ino);
    throw DmException(DM_EXISTS, name + " already exists");
  }
  catch (DmException e) {
    if (e.code() != DM_NO_SUCH_FILE)
      throw;
  }

  // Check the parent!
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       parent.acl, parent.xStat.stat,
                       S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Need write access on the parent");

  // Fetch the new file ID
  Statement uniqueId(this->getPreparedStatement(STMT_SELECT_UNIQ_ID_FOR_UPDATE));

  uniqueId.execute();
  uniqueId.bindResult(0, &newFileId);
  uniqueId.fetch();

  // Update
  Statement updateUnique(this->getPreparedStatement(STMT_UPDATE_UNIQ_ID));
  
  newFileId++;
  updateUnique.bindParam(0, newFileId);
  updateUnique.execute();

  // Check SGID
  if (parent.xStat.stat.st_mode & S_ISGID) {
    egid = parent.xStat.stat.st_gid;
    mode |= S_ISGID;
  }
  else {
    egid = this->group_.gid;
  }

  // Create the entry
  Statement fileStmt(this->getPreparedStatement(STMT_INSERT_FILE));

  fileStmt.bindParam( 0, newFileId);
  fileStmt.bindParam( 1, parent.xStat.stat.st_ino);
  fileStmt.bindParam( 2, name);
  fileStmt.bindParam( 3, mode);
  fileStmt.bindParam( 4, nlink);
  fileStmt.bindParam( 5, this->user_.uid);
  fileStmt.bindParam( 6, egid);
  fileStmt.bindParam( 7, size);
  fileStmt.bindParam( 8, type);
  fileStmt.bindParam( 9, std::string(&status, 1));
  fileStmt.bindParam(10, csumtype);
  fileStmt.bindParam(11, csumvalue);
  fileStmt.bindParam(12, acl.data(), acl.size());

  fileStmt.execute();
  
  // Increment the nlink
  Statement nlinkStmt(this->getPreparedStatement(STMT_UPDATE_NLINK));

  parent.xStat.stat.st_nlink++;
  nlinkStmt.bindParam(0, parent.xStat.stat.st_nlink);
  nlinkStmt.bindParam(1, parent.xStat.stat.st_ino);

  nlinkStmt.execute();

  // Return back
  return this->getFile(newFileId);
}



FileMetadata NsMySqlCatalog::getParent(const std::string& path,
                                       std::string* parentPath,
                                       std::string* name) throw (DmException)
{
  std::list<std::string> components = splitPath(path);

  parentPath->clear();
  name->clear();

  // Build parent (this is, skipping last one)
  while (components.size() > 1) {
    *parentPath += "/" + components.front();
    components.pop_front();
  }
  
  *name = components.front();
  components.pop_front();

  // Get the files now
  if (!parentPath->empty())
    return this->parsePath(*parentPath);
  else if (!this->cwdPath_.empty())
    return this->cwdMeta_;
  else
    return this->parsePath("/");
}



FileMetadata NsMySqlCatalog::parsePath(const std::string& path, bool followSym) throw(DmException)
{
  // Split the path always assuming absolute
  std::list<std::string> components = splitPath(path);

  // Iterate starting from absolute root (parent of /) (0)
  uint64_t     parent       = 0;
  unsigned     symLinkLevel = 0;
  FileMetadata meta;
  std::string  c;

  // If path is absolute OR cwd is empty, start in root
  if (path[0] == '/' || this->cwdPath_.empty()) {
    // Push "/", as we have to look for it too
    components.push_front("/");
    // Root parent "is" a dir and world-readable :)
    memset(&meta, 0x00, sizeof(FileMetadata));
    meta.xStat.stat.st_mode = S_IFDIR | 0555 ;
  }
  // Relative, and cwd set, so start there
  else {
    meta   = this->cwdMeta_;
    parent = meta.xStat.stat.st_ino;
  }
  

  while (!components.empty()) {
    // Check that the parent is a directory first
    if (!S_ISDIR(meta.xStat.stat.st_mode) && !S_ISLNK(meta.xStat.stat.st_mode))
      throw DmException(DM_NOT_DIRECTORY, "%s is not a directory", meta.xStat.name);
    // New element traversed! Need to check if it is possible to keep going.
    if (checkPermissions(this->user_, this->group_, this->groups_,
                         meta.acl, meta.xStat.stat, S_IEXEC) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to list %s", meta.xStat.name);

    // Pop next component
    c = components.front();
    components.pop_front();

    // Stay here
    if (c == ".") {
      continue;
    }
    // Up one level
    else if (c == "..") {
      meta   = this->getFile(parent);
      parent = meta.xStat.parent;
    }
    // Regular entry
    else {
      meta = this->getFile(c, parent);

      // Symbolic link!, follow that instead
      if (S_ISLNK(meta.xStat.stat.st_mode) && followSym) {
        SymLink link = this->getLink(meta.xStat.stat.st_ino);

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
        parent = meta.xStat.stat.st_ino;
      }
    }
    
  }

  return meta;
}



void NsMySqlCatalog::traverseBackwards(const FileMetadata& meta) throw (DmException)
{
  FileMetadata current = meta;

  // We want to check if we can arrive here...
  while (current.xStat.parent != 0) {
    current = this->getFile(current.xStat.parent);
    if (checkPermissions(this->user_, this->group_, this->groups_,
                         current.acl, current.xStat.stat, S_IEXEC))
      throw DmException(DM_FORBIDDEN, "Can not access #%ld",
                        current.xStat.stat.st_ino);
  }
}



void NsMySqlCatalog::changeDir(const std::string& path) throw (DmException)
{
  if (this->decorated_ != 0x00)
    this->decorated_->changeDir(path);
  this->cwdMeta_ = this->parsePath(path);
  this->cwdPath_ = path;
}



std::string NsMySqlCatalog::getWorkingDir(void) throw (DmException)
{
  return this->cwdPath_;
}



struct stat NsMySqlCatalog::stat(const std::string& path) throw(DmException)
{
  FileMetadata meta = this->parsePath(path);
  return meta.xStat.stat;
}



struct stat NsMySqlCatalog::stat(ino_t inode) throw (DmException)
{
  FileMetadata meta = this->getFile(inode);
  return meta.xStat.stat;
}



struct stat NsMySqlCatalog::linkStat(const std::string& path) throw(DmException)
{
  FileMetadata meta = this->parsePath(path, false);
  return meta.xStat.stat;
}



struct xstat NsMySqlCatalog::extendedStat(const std::string& path) throw (DmException)
{
  FileMetadata meta = this->parsePath(path);
  return meta.xStat;
}



struct xstat NsMySqlCatalog::extendedStat(ino_t inode) throw (DmException)
{
  FileMetadata meta = this->getFile(inode);
  return meta.xStat;
}



void NsMySqlCatalog::setUserId(uid_t uid, gid_t gid, const std::string& dn) throw(DmException)
{
  if (this->decorated_ != 0x00)
    this->decorated_->setUserId(uid, gid, dn);

  this->user_  = this->getUser(uid);
  this->group_ = this->getGroup(gid);

  if (this->user_.name != dn) {
    throw DmException(DM_INVALID_VALUE, "The specified dn doesn't match "
                                        "the one associated with the id #%ld: "
                                        "'%s' != '%s'",
                                        uid, dn.c_str(), this->user_.name);
  }
}



Directory* NsMySqlCatalog::openDir(const std::string& path) throw(DmException)
{
  NsMySqlDir  *dir;
  FileMetadata meta;

  // Get the directory
  meta = this->parsePath(path);
  
  // Can we read it?
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.xStat.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to read " + path);

  // Create the handle
  dir = new NsMySqlDir();
  dir->dirId = meta.xStat.stat.st_ino;
  
  try {
    dir->stmt = new Statement(this->getPreparedStatement(STMT_GET_LIST_FILES));
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
    return &(((NsMySqlDir*)dir)->ds.dirent);
}



struct direntstat* NsMySqlCatalog::readDirx(Directory* dir) throw(DmException)
{
  NsMySqlDir *dirp;

  if (dir == 0x00)
    throw DmException(DM_NULL_POINTER, "Tried to read a null dir");

  dirp = (NsMySqlDir*)dir;

  if (dirp->stmt->fetch()) {
    memcpy(&dirp->ds.stat, &dirp->current.xStat.stat, sizeof(struct stat));
    memset(&dirp->ds.dirent, 0x00, sizeof(struct dirent));
    dirp->ds.dirent.d_ino  = dirp->ds.stat.st_ino;
    strncpy(dirp->ds.dirent.d_name,
            dirp->current.xStat.name,
            sizeof(dirp->ds.dirent.d_name));
    return &dirp->ds;
  }
  else {
    return 0x00;
  }
}



void NsMySqlCatalog::getIdMap(const std::string& userName, const std::vector<std::string>& groupNames,
                              uid_t* uid, std::vector<gid_t>* gids) throw (DmException)
{
  std::string vo;
  UserInfo    user;
  GroupInfo   group;

  // User mapping
  try {
    user = this->getUser(userName);
    *uid = user.uid;
  }
  catch (DmException e) {
    // TODO: Add user
    throw;
  }

  // No VO information, so use the mapping file to get the group
  if (groupNames.empty()) {
    vo = voFromDn("/etc/lcgdm-mapfile", userName);
    try {
      group = this->getGroup(vo);
      gids->push_back(group.gid);
    }
    catch (DmException) {
      // TODO: Add group
      throw;
    }
  }
  else {
    // Get group info
    std::vector<std::string>::const_iterator i;
    for (i = groupNames.begin(); i != groupNames.end(); ++i) {
      vo = voFromRole(*i);
      try {
        group = this->getGroup(vo);
        gids->push_back(group.gid);
      }
      catch (DmException) {
        // TODO: Add group
        throw;
      }
    }
  }
}



UserInfo NsMySqlCatalog::getUser(const std::string& userName) throw(DmException)
{
  UserInfo  user;
  Statement stmt(this->getPreparedStatement(STMT_GET_USERINFO_BY_NAME));

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
  Statement stmt(this->getPreparedStatement(STMT_GET_USERINFO_BY_UID));

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
  Statement stmt(this->getPreparedStatement(STMT_GET_GROUPINFO_BY_NAME));

  stmt.bindParam(0, groupName);
  stmt.execute();

  stmt.bindResult(0, &group.gid);
  stmt.bindResult(1, group.name, sizeof(group.name));
  stmt.bindResult(2, &group.banned);

  stmt.fetch();
  return group;
}



GroupInfo NsMySqlCatalog::getGroup(gid_t gid) throw(DmException)
{
  GroupInfo group;
  Statement stmt(this->getPreparedStatement(STMT_GET_GROUPINFO_BY_GID));

  stmt.bindParam(0, gid);
  stmt.execute();

  stmt.bindResult(0, &group.gid);
  stmt.bindResult(1, group.name, sizeof(group.name));
  stmt.bindResult(2, &group.banned);
  
  stmt.fetch();
  return group;
}



void NsMySqlCatalog::addReplica(const std::string& guid, int64_t id,
                                const std::string& server, const std::string& sfn,
                                char status, char fileType,
                                const std::string& poolName,
                                const std::string& fileSystem) throw (DmException)
{
  FileMetadata meta;

  if (guid.empty())
    meta = this->getFile(id);
  else
    meta = this->getFile(guid);

  // Access has to be checked backwards!
  this->traverseBackwards(meta);

  // Can write?
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.xStat.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to add the replica");

  // Add it
  Statement statement(this->getPreparedStatement(STMT_ADD_REPLICA));

  statement.bindParam(0, meta.xStat.stat.st_ino);
  statement.bindParam(1, NULL, 0);
  statement.bindParam(2, std::string(&status, 1));
  statement.bindParam(3, std::string(&fileType, 1));
  statement.bindParam(4, NULL, 0);
  statement.bindParam(5, poolName);
  statement.bindParam(6, server);
  statement.bindParam(7, fileSystem);
  statement.bindParam(8, sfn);

  statement.execute();
}



void NsMySqlCatalog::deleteReplica(const std::string& guid, int64_t id,
                                   const std::string& sfn) throw (DmException)
{
  FileMetadata meta;

  if (guid.empty())
    meta = this->getFile(id);
  else
    meta = this->getFile(guid);

  // Access has to be checked backwards!
  this->traverseBackwards(meta);

  // Can write?
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.xStat.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to remove the replica");

  // Remove
  Statement statement(this->getPreparedStatement(STMT_DELETE_REPLICA));
  statement.bindParam(0, meta.xStat.stat.st_ino);
  statement.bindParam(1, sfn);
  statement.execute();
}



std::vector<FileReplica> NsMySqlCatalog::getReplicas(const std::string& path) throw(DmException)
{
  FileMetadata  meta;

  // Need to grab the file first
  meta = this->parsePath(path, true);

  // The file exists, plus we have permissions to go there. Check we can read
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.xStat.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN,
                   "Not enough permissions to read " + path);

  try {
    return this->getReplicas(meta.xStat.stat.st_ino);
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
  Statement stmt(this->getPreparedStatement(STMT_GET_FILE_REPLICAS));

  // Execute query
  stmt.bindParam(0, ino);
  stmt.execute();

  // Bind result
  stmt.bindResult(0, &replica.replicaid);
  stmt.bindResult(1, &replica.fileid);
  stmt.bindResult(2, &replica.status, 1);
  stmt.bindResult(3, replica.unparsed_location, sizeof(replica.unparsed_location));

  std::vector<FileReplica> replicas;

  if ((nReplicas = stmt.count()) == 0)
    throw DmException(DM_NO_REPLICAS, "No replicas available for %ld", ino);

  // Fetch
  int i = 0;
  while (stmt.fetch()) {
    replica.location = splitUri(replica.unparsed_location);
    replicas.push_back(replica);
    ++i;
  };

  return replicas;
}



FileReplica NsMySqlCatalog::get(const std::string& path) throw(DmException)
{
  // Get all the replicas
  std::vector<FileReplica> replicas = this->getReplicas(path);

  // Pick a random one
  int i = rand() % replicas.size();

  // Copy
  return replicas[i];
}



void NsMySqlCatalog::symlink(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
  std::string parentPath, symName;
  
  // Get the parent of the destination and file
  FileMetadata parent = this->getParent(newPath, &parentPath, &symName);
  FileMetadata file   = this->parsePath(oldPath);

  // Check we have write access for the parent
  if (checkPermissions(this->user_, this->group_, this->groups_, parent.acl,
                      parent.xStat.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Need write access for " + parentPath);

  // Transaction
  Transaction transaction(this->conn_);

  // Create the file entry
  FileMetadata linkMeta = this->newFile(parent,
                                        symName, 0777 | S_IFLNK,
                                        1, 0, 0, '-',
                                        "", "", "");
  // Create the symlink entry
  Statement stmt(this->getPreparedStatement(STMT_INSERT_SYMLINK));

  stmt.bindParam(0, linkMeta.xStat.stat.st_ino);
  stmt.bindParam(1, oldPath);

  stmt.execute();

  // Done
  transaction.commit();
}



void NsMySqlCatalog::unlink(const std::string& path) throw (DmException)
{
  std::string  parentPath, name;

  // Get the parent
  FileMetadata parent = this->getParent(path, &parentPath, &name);

  // The file itself
  FileMetadata file = this->getFile(name, parent.xStat.stat.st_ino);

  // Directories can not be removed with this method!
  if (S_ISDIR(file.xStat.stat.st_mode))
    throw DmException(DM_IS_DIRECTORY, path + " is a directory, can not unlink");

  // Check we can remove it
  if ((parent.xStat.stat.st_mode & S_ISVTX) == S_ISVTX) {
    // Sticky bit set
    if (this->user_.uid != file.xStat.stat.st_uid &&
        this->user_.uid != parent.xStat.stat.st_uid &&
        checkPermissions(this->user_, this->group_, this->groups_, file.acl,
                         file.xStat.stat, S_IWRITE) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to unlink " +
                                      path + "( sticky bit set)");
  }
  else {
    // No sticky bit
    if (checkPermissions(this->user_, this->group_, this->groups_, parent.acl,
                         parent.xStat.stat, S_IWRITE) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to unlink " + path);
  }

  // Check there are no replicas
  if (!S_ISLNK(file.xStat.stat.st_mode)) {
    try {
      this->getReplicas(file.xStat.stat.st_ino);
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
  Statement delSymlink(this->getPreparedStatement(STMT_DELETE_SYMLINK));
  delSymlink.bindParam(0, file.xStat.stat.st_ino);
  delSymlink.execute();

  // Remove associated comments
  Statement delComment(this->getPreparedStatement(STMT_DELETE_COMMENT));
  delComment.bindParam(0, file.xStat.stat.st_ino);
  delComment.execute();

  // Remove file itself
  Statement delFile(this->getPreparedStatement(STMT_DELETE_FILE));
  delFile.bindParam(0, file.xStat.stat.st_ino);
  delFile.execute();

  // And decrement nlink
  Statement nlink(this->getPreparedStatement(STMT_UPDATE_NLINK));
  parent.xStat.stat.st_nlink--;
  nlink.bindParam(0, parent.xStat.stat.st_nlink);
  nlink.bindParam(1, parent.xStat.stat.st_ino);
  nlink.execute();

  // Done!
  transaction.commit();
}



std::vector<ExtendedReplica> NsMySqlCatalog::getExReplicas(const std::string& path) throw(DmException)
{
  FileMetadata    meta;
  ExtendedReplica replica;
  int             nReplicas;

  // Need to grab the file first
  meta = this->parsePath(path, true);

  // The file exists, plus we have permissions to go there. Check we can read
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.xStat.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN,
                      "Not enough permissions to read " + path);

  // MySQL statement
  Statement stmt(this->getPreparedStatement(STMT_GET_FILE_REPLICAS_EXTENDED));

  stmt.bindParam(0, meta.xStat.stat.st_ino);
  stmt.execute();

  stmt.bindResult(0, &replica.replica.replicaid);
  stmt.bindResult(1, &replica.replica.fileid);
  stmt.bindResult(2, &replica.replica.status, 1);
  stmt.bindResult(3, replica.replica.unparsed_location, sizeof(replica.replica.unparsed_location));
  stmt.bindResult(4, replica.pool, sizeof(replica.pool));
  stmt.bindResult(5, replica.host, sizeof(replica.host));
  stmt.bindResult(6, replica.fs,   sizeof(replica.fs));
  
  std::vector<ExtendedReplica> replicas;

  nReplicas = stmt.count();
  if (nReplicas == 0)
    throw DmException(DM_NO_REPLICAS, "No replicas available for " + path);

  // Fetch
  int i = 0;
  while (stmt.fetch()) {
    replica.replica.location = splitUri(replica.replica.unparsed_location);
    replicas.push_back(replica);
    ++i;
  };

  return replicas;
}



void NsMySqlCatalog::create(const std::string& path, mode_t mode) throw (DmException)
{
  int          code;
  std::string  parentPath, name;
  FileMetadata parent = this->getParent(path, &parentPath, &name);
  FileMetadata file;

  try {
    file = this->getFile(name, parent.xStat.stat.st_ino);
    // File exists, check if it has replicas
    this->getReplicas(file.xStat.stat.st_ino);
    // It has replicas, so fail!
    throw DmException(DM_EXISTS, path + " exists and has replicas. Can not truncate.");
  }
  catch (DmException e) {
    code = e.code();
    if (code != DM_NO_SUCH_FILE && code != DM_NO_REPLICAS)
      throw;
  }

  // Create new
  if (code == DM_NO_SUCH_FILE) {
    this->newFile(parent, name, (mode & ~S_IFMT) & ~this->umask_,
                  1, 0, 0, '-',
                  std::string(), std::string(),
                  std::string());
  }
  // Truncate
  else if (code == DM_NO_REPLICAS) {
    Statement statement(this->getPreparedStatement(STMT_TRUNCATE_FILE));
    statement.bindParam(0, file.xStat.stat.st_ino);
    statement.execute();
  }
}



mode_t NsMySqlCatalog::umask(mode_t mask) throw ()
{
  if (this->decorated_ != 0x00)
    this->decorated_->umask(mask);

  mode_t prev = this->umask_;
  this->umask_ = mask & 0777;
  return prev;
}



void NsMySqlCatalog::changeMode(const std::string& path, mode_t mode) throw (DmException)
{
  FileMetadata meta = this->parsePath(path);

  // User has to be the owner, or root
  if (this->user_.uid != meta.xStat.stat.st_uid && this->user_.uid != 0)
    throw DmException(DM_FORBIDDEN, "Only the owner can change the mode of " + path);

  // Clean up unwanted bits
  mode &= ~S_IFMT;
  if (!S_ISDIR(meta.xStat.stat.st_mode) && this->user_.uid != 0)
    mode &= ~S_ISVTX;
  if (this->user_.uid != 0 &&
      meta.xStat.stat.st_gid != this->group_.gid &&
      !gidInGroups(meta.xStat.stat.st_gid, this->groups_))
    mode &= ~S_ISGID;

  // Update, keeping type bits from db.
  mode |= (meta.xStat.stat.st_mode & S_IFMT);

  Statement statement(this->getPreparedStatement(STMT_UPDATE_MODE));
  statement.bindParam(0, mode);
  statement.bindParam(1, meta.xStat.stat.st_ino);
  statement.execute();
}



std::string NsMySqlCatalog::getComment(const std::string& path) throw(DmException)
{
  char         comment[COMMENT_MAX];
  
  // Get the file and check we can read
  FileMetadata meta = this->parsePath(path, true);
  
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.xStat.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to read " + path);

  // Query
  Statement stmt(this->getPreparedStatement(STMT_GET_COMMENT));

  stmt.bindParam(0, meta.xStat.stat.st_ino);
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
  FileMetadata meta = this->parsePath(path);

  if (checkPermissions(this->user_, this->group_, this->groups_, meta.acl,
                       meta.xStat.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to write " + path);

  // Query
  Statement stmt(this->getPreparedStatement(STMT_SET_COMMENT));

  stmt.bindParam(0, comment);
  stmt.bindParam(1, meta.xStat.stat.st_ino);

  if (stmt.execute() == 0) {
    // No update! Try inserting
    Statement stmti(this->getPreparedStatement(STMT_INSERT_COMMENT));

    stmti.bindParam(0, meta.xStat.stat.st_ino);
    stmti.bindParam(1, comment);
    
    stmti.execute();
  }
}



void NsMySqlCatalog::setGuid(const std::string& path, const std::string& guid) throw (DmException)
{
  MYSQL_STMT   *stmt = this->getPreparedStatement(STMT_SET_GUID);
  MYSQL_BIND    bindParam[2];
  long unsigned guidLen = guid.length();
  FileMetadata  meta    = this->parsePath(path);

  if (checkPermissions(this->user_, this->group_, this->groups_, meta.acl,
                       meta.xStat.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to write " + path);

  memset(bindParam, 0, sizeof(bindParam));

  bindParam[0].buffer_type = MYSQL_TYPE_VARCHAR;
  bindParam[0].length      = &guidLen;
  bindParam[0].buffer      = (void*)guid.c_str();
  bindParam[1].buffer_type = MYSQL_TYPE_LONGLONG;
  bindParam[1].buffer      = &meta.xStat.stat.st_ino;

  mysql_stmt_bind_param(stmt, bindParam);

  if (mysql_stmt_execute(stmt) != 0)
    throw DmException(DM_QUERY_FAILED, mysql_stmt_error(stmt));
  mysql_stmt_free_result(stmt);
}



void NsMySqlCatalog::makeDir(const std::string& path, mode_t mode) throw (DmException)
{
  std::string parentPath, name;

  // Get the parent of the new folder
  FileMetadata parent = this->getParent(path, &parentPath, &name);

  // Check we have write access for the parent
  if (checkPermissions(this->user_, this->group_, this->groups_, parent.acl,
                      parent.xStat.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Need write access for " + parentPath);

  // Create the file entry
  Transaction transaction(this->conn_);
  this->newFile(parent, name, (mode & ~this->umask_) | S_IFDIR,
                0, 0, parent.xStat.type, '-', "", "", parent.acl);
  transaction.commit();
}



void NsMySqlCatalog::removeDir(const std::string& path) throw (DmException)
{
  std::string parentPath, name;

  // Get the parent of the new folder
  FileMetadata parent = this->getParent(path, &parentPath, &name);

  // Get the file, and check it is a directory and it is empty
  FileMetadata entry = this->getFile(name, parent.xStat.stat.st_ino);

  if (!S_ISDIR(entry.xStat.stat.st_mode))
    throw DmException(DM_NOT_DIRECTORY, path + " is not a directory. Can not remove.");

  if (entry.xStat.stat.st_nlink > 0)
    throw DmException(DM_NOT_EMPTY, path + " is not empty. Can not remove.");

  // Check we can remove it
  if ((parent.xStat.stat.st_mode & S_ISVTX) == S_ISVTX) {
    // Sticky bit set
    if (this->user_.uid != entry.xStat.stat.st_uid &&
        this->user_.uid != parent.xStat.stat.st_uid &&
        checkPermissions(this->user_, this->group_, this->groups_, entry.acl,
                         entry.xStat.stat, S_IWRITE) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to remove " +
                                      path + "( sticky bit set)");
  }
  else {
    // No sticky bit
    if (checkPermissions(this->user_, this->group_, this->groups_, parent.acl,
                         parent.xStat.stat, S_IWRITE) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to remove " + path);
  }

  // All preconditions are good!
  Transaction transaction(this->conn_);


  // Remove directory itself
  Statement delDir(this->getPreparedStatement(STMT_DELETE_FILE));
  delDir.bindParam(0, entry.xStat.stat.st_ino);
  delDir.execute();

  // And decrement nlink
  Statement nlink(this->getPreparedStatement(STMT_UPDATE_NLINK));
  parent.xStat.stat.st_nlink--;
  nlink.bindParam(0, parent.xStat.stat.st_nlink);
  nlink.bindParam(1, parent.xStat.stat.st_ino);
  nlink.execute();

  // Done!
  transaction.commit();
}
