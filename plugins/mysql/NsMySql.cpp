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

#define NOT_IMPLEMENTED(p)\
p {\
  throw DmException(DM_NOT_IMPLEMENTED, #p" not implemented");\
}



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
  STMT_INSERT_UNIQ_ID,
  STMT_DELETE_FILE,
  STMT_DELETE_COMMENT,
  STMT_DELETE_SYMLINK,
  STMT_UPDATE_NLINK,
  STMT_UPDATE_MODE,
  STMT_DELETE_REPLICA,
  STMT_ADD_REPLICA,
  STMT_TRUNCATE_FILE,
  STMT_GET_UNIQ_UID_FOR_UPDATE,
  STMT_GET_UNIQ_GID_FOR_UPDATE,
  STMT_UPDATE_UNIQ_UID,
  STMT_UPDATE_UNIQ_GID,
  STMT_INSERT_UNIQ_UID,
  STMT_INSERT_UNIQ_GID,
  STMT_INSERT_USER,
  STMT_INSERT_GROUP,
  STMT_CHANGE_NAME,
  STMT_CHANGE_PARENT,
  STMT_CHANGE_OWNER,
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
  "INSERT INTO Cns_unique_id (id) VALUES (?)",
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
  "SELECT id FROM Cns_unique_uid FOR UPDATE",
  "SELECT id FROM Cns_unique_gid FOR UPDATE",
  "UPDATE Cns_unique_uid SET id = ?",
  "UPDATE Cns_unique_gid SET id = ?",
  "INSERT INTO Cns_unique_uid (id) VALUES (?)",
  "INSERT INTO Cns_unique_gid (id) VALUES (?)",
  "INSERT INTO Cns_userinfo\
          (userid, username, user_ca, banned)\
        VALUES\
          (?, ?, ?, ?)",
  "INSERT INTO Cns_groupinfo\
          (gid, groupname, banned)\
        VALUES\
          (?, ?, ?)",
  "UPDATE Cns_file_metadata\
        SET name = ?\
        WHERE fileid = ?",
  "UPDATE Cns_file_metadata\
        SET parent_fileid = ?\
        WHERE fileid = ?",
  "UPDATE Cns_file_metadata\
        SET owner_uid = ?, gid = ?\
        WHERE fileid = ?",
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
                               unsigned int symLinkLimit) throw(DmException):
                cwd_(0), umask_(022), nsDb_(db), symLinkLimit_(symLinkLimit),
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



void NsMySqlCatalog::set(const std::string& key, va_list varg) throw(DmException)
{
  throw DmException(DM_UNKNOWN_OPTION, "Option " + key + " unknown");
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
  Statement    stmt(this->getPreparedStatement(STMT_GET_FILE_BY_ID));
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
  Statement    stmt(this->getPreparedStatement(STMT_GET_FILE_BY_GUID));
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
  Statement    stmt(this->getPreparedStatement(STMT_GET_FILE_BY_NAME));
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
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       parent.acl, parent.stat,
                       S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Need write access on the parent");

  // Fetch the new file ID
  Statement uniqueId(this->getPreparedStatement(STMT_SELECT_UNIQ_ID_FOR_UPDATE));

  uniqueId.execute();
  uniqueId.bindResult(0, &newFileId);

  // Update the unique ID
  if (uniqueId.fetch()) {
    Statement updateUnique(this->getPreparedStatement(STMT_UPDATE_UNIQ_ID));
    ++newFileId;
    updateUnique.bindParam(0, newFileId);
    updateUnique.execute();
  }
  // Couldn't get, so insert
  else {
    Statement insertUnique(this->getPreparedStatement(STMT_INSERT_UNIQ_ID));
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
    egid = this->group_.gid;
  }

  // Create the entry
  Statement fileStmt(this->getPreparedStatement(STMT_INSERT_FILE));

  fileStmt.bindParam( 0, newFileId);
  fileStmt.bindParam( 1, parent.stat.st_ino);
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

  parent.stat.st_nlink++;
  nlinkStmt.bindParam(0, parent.stat.st_nlink);
  nlinkStmt.bindParam(1, parent.stat.st_ino);

  nlinkStmt.execute();

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
    if (checkPermissions(this->user_, this->group_, this->groups_,
                         meta.acl, meta.stat, S_IEXEC) != 0)
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
    if (checkPermissions(this->user_, this->group_, this->groups_,
                         current.acl, current.stat, S_IEXEC))
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
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to read " + path);

  // Create the handle
  dir = new NsMySqlDir();
  dir->dirId = meta.stat.st_ino;
  
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
    memcpy(&dirp->ds.stat, &dirp->current.stat, sizeof(struct stat));
    memset(&dirp->ds.dirent, 0x00, sizeof(struct dirent));
    dirp->ds.dirent.d_ino  = dirp->ds.stat.st_ino;
    strncpy(dirp->ds.dirent.d_name,
            dirp->current.name,
            sizeof(dirp->ds.dirent.d_name));
    return &dirp->ds;
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

  if (guid.empty())
    meta = this->extendedStat(id);
  else
    meta = this->guidStat(guid);

  // Access has to be checked backwards!
  this->traverseBackwards(meta);

  // Can write?
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to add the replica");

  // Add it
  Statement statement(this->getPreparedStatement(STMT_ADD_REPLICA));

  statement.bindParam(0, meta.stat.st_ino);
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
  ExtendedStat meta;

  if (guid.empty())
    meta = this->extendedStat(id);
  else
    meta = this->guidStat(guid);

  // Access has to be checked backwards!
  this->traverseBackwards(meta);

  // Can write?
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to remove the replica");

  // Remove
  Statement statement(this->getPreparedStatement(STMT_DELETE_REPLICA));
  statement.bindParam(0, meta.stat.st_ino);
  statement.bindParam(1, sfn);
  statement.execute();
}



std::vector<FileReplica> NsMySqlCatalog::getReplicas(const std::string& path) throw(DmException)
{
  ExtendedStat  meta;

  // Need to grab the file first
  meta = this->extendedStat(path, true);

  // The file exists, plus we have permissions to go there. Check we can read
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.stat, S_IREAD) != 0)
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



NOT_IMPLEMENTED(std::string NsMySqlCatalog::put(const std::string&, Uri*) throw (DmException))
NOT_IMPLEMENTED(std::string NsMySqlCatalog::put(const std::string&, Uri*, const std::string&) throw (DmException))
NOT_IMPLEMENTED(void NsMySqlCatalog::putStatus(const std::string&, const std::string&, Uri*) throw (DmException))
NOT_IMPLEMENTED(void NsMySqlCatalog::putDone(const std::string&, const std::string&) throw (DmException))



void NsMySqlCatalog::symlink(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
  std::string parentPath, symName;
  
  // Get the parent of the destination and file
  ExtendedStat parent = this->getParent(newPath, &parentPath, &symName);

  // Check we have write access for the parent
  if (checkPermissions(this->user_, this->group_, this->groups_, parent.acl,
                      parent.stat, S_IWRITE | S_IEXEC) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions on " + parentPath);

  // Transaction
  Transaction transaction(this->conn_);

  // Create the file entry
  ExtendedStat linkMeta = this->newFile(parent,
                                        symName, 0777 | S_IFLNK,
                                        1, 0, 0, '-',
                                        "", "", "");
  // Create the symlink entry
  Statement stmt(this->getPreparedStatement(STMT_INSERT_SYMLINK));

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
  if (checkPermissions(this->user_, this->group_, this->groups_, parent.acl,
                       parent.stat, S_IEXEC) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to list " + parentPath);

  // The file itself
  ExtendedStat file = this->extendedStat(parent.stat.st_ino, name);

  // Directories can not be removed with this method!
  if (S_ISDIR(file.stat.st_mode))
    throw DmException(DM_IS_DIRECTORY, path + " is a directory, can not unlink");

  // Check we can remove it
  if ((parent.stat.st_mode & S_ISVTX) == S_ISVTX) {
    // Sticky bit set
    if (this->user_.uid != file.stat.st_uid &&
        this->user_.uid != parent.stat.st_uid &&
        checkPermissions(this->user_, this->group_, this->groups_, file.acl,
                         file.stat, S_IWRITE) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to unlink " +
                                      path + "( sticky bit set)");
  }
  else {
    // No sticky bit
    if (checkPermissions(this->user_, this->group_, this->groups_, parent.acl,
                         parent.stat, S_IWRITE) != 0)
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
  Statement delSymlink(this->getPreparedStatement(STMT_DELETE_SYMLINK));
  delSymlink.bindParam(0, file.stat.st_ino);
  delSymlink.execute();

  // Remove associated comments
  Statement delComment(this->getPreparedStatement(STMT_DELETE_COMMENT));
  delComment.bindParam(0, file.stat.st_ino);
  delComment.execute();

  // Remove file itself
  Statement delFile(this->getPreparedStatement(STMT_DELETE_FILE));
  delFile.bindParam(0, file.stat.st_ino);
  delFile.execute();

  // And decrement nlink
  Statement nlink(this->getPreparedStatement(STMT_UPDATE_NLINK));
  parent.stat.st_nlink--;
  nlink.bindParam(0, parent.stat.st_nlink);
  nlink.bindParam(1, parent.stat.st_ino);
  nlink.execute();

  // Done!
  transaction.commit();
}



std::vector<ExtendedReplica> NsMySqlCatalog::getExReplicas(const std::string& path) throw(DmException)
{
  ExtendedStat    meta;
  ExtendedReplica replica;
  int             nReplicas;

  // Need to grab the file first
  meta = this->extendedStat(path, true);

  // The file exists, plus we have permissions to go there. Check we can read
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN,
                      "Not enough permissions to read " + path);

  // MySQL statement
  Statement stmt(this->getPreparedStatement(STMT_GET_FILE_REPLICAS_EXTENDED));

  stmt.bindParam(0, meta.stat.st_ino);
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
  while (stmt.fetch()) {
    replica.replica.location = splitUri(replica.replica.unparsed_location);
    replicas.push_back(replica);
  };

  return replicas;
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
    this->newFile(parent, name, (mode & ~S_IFMT) & ~this->umask_,
                  1, 0, 0, '-',
                  std::string(), std::string(),
                  std::string());
  }
  // Truncate
  else if (code == DM_NO_REPLICAS) {
    Statement statement(this->getPreparedStatement(STMT_TRUNCATE_FILE));
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
  if (this->user_.uid != meta.stat.st_uid && this->user_.uid != 0)
    throw DmException(DM_FORBIDDEN, "Only the owner can change the mode of " + path);

  // Clean up unwanted bits
  mode &= ~S_IFMT;
  if (!S_ISDIR(meta.stat.st_mode) && this->user_.uid != 0)
    mode &= ~S_ISVTX;
  if (this->user_.uid != 0 &&
      meta.stat.st_gid != this->group_.gid &&
      !gidInGroups(meta.stat.st_gid, this->groups_))
    mode &= ~S_ISGID;

  // Update, keeping type bits from db.
  mode |= (meta.stat.st_mode & S_IFMT);

  Statement statement(this->getPreparedStatement(STMT_UPDATE_MODE));
  statement.bindParam(0, mode);
  statement.bindParam(1, meta.stat.st_ino);
  statement.execute();
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
  if (this->user_.uid != 0) {
    // Only root can change the owner
    if (meta.stat.st_uid != newUid)
      throw DmException(DM_BAD_OPERATION, "Only root can change the owner");
    // If the group is changing...
    if (meta.stat.st_gid != newGid) {
      // The user has to be the owner
      if (meta.stat.st_uid != this->user_.uid)
        throw DmException(DM_BAD_OPERATION, "Only root or the owner can change the group");
      // AND it has to belong to that group
      if (newGid != this->group_.gid && !gidInGroups(newGid, this->groups_))
        throw DmException(DM_BAD_OPERATION, "The user does not belong to the group %d", newGid);
      // If it does, the group exists :)
    }
  }

  // Change!
  Statement chownStmt(this->getPreparedStatement(STMT_CHANGE_OWNER));

  chownStmt.bindParam(0, newUid);
  chownStmt.bindParam(1, newGid);
  chownStmt.bindParam(2, meta.stat.st_ino);

  chownStmt.execute();
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



std::string NsMySqlCatalog::getComment(const std::string& path) throw(DmException)
{
  char         comment[COMMENT_MAX];
  
  // Get the file and check we can read
  ExtendedStat meta = this->extendedStat(path);
  
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to read " + path);

  // Query
  Statement stmt(this->getPreparedStatement(STMT_GET_COMMENT));

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

  if (checkPermissions(this->user_, this->group_, this->groups_, meta.acl,
                       meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to write " + path);

  // Query
  Statement stmt(this->getPreparedStatement(STMT_SET_COMMENT));

  stmt.bindParam(0, comment);
  stmt.bindParam(1, meta.stat.st_ino);

  if (stmt.execute() == 0) {
    // No update! Try inserting
    Statement stmti(this->getPreparedStatement(STMT_INSERT_COMMENT));

    stmti.bindParam(0, meta.stat.st_ino);
    stmti.bindParam(1, comment);
    
    stmti.execute();
  }
}



void NsMySqlCatalog::setGuid(const std::string& path, const std::string& guid) throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path);

  if (checkPermissions(this->user_, this->group_, this->groups_, meta.acl,
                       meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to write " + path);

  // Query
  Statement stmt(this->getPreparedStatement(STMT_SET_GUID));

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
  if (checkPermissions(this->user_, this->group_, this->groups_, parent.acl,
                      parent.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Need write access for " + parentPath);

  // Create the file entry
  Transaction transaction(this->conn_);
  this->newFile(parent, name, (mode & ~this->umask_) | S_IFDIR,
                0, 0, parent.type, '-', "", "", parent.acl);
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
    if (this->user_.uid != entry.stat.st_uid &&
        this->user_.uid != parent.stat.st_uid &&
        checkPermissions(this->user_, this->group_, this->groups_, entry.acl,
                         entry.stat, S_IWRITE) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to remove " +
                                      path + "( sticky bit set)");
  }
  else {
    // No sticky bit
    if (checkPermissions(this->user_, this->group_, this->groups_, parent.acl,
                         parent.stat, S_IWRITE) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to remove " + path);
  }

  // All preconditions are good!
  Transaction transaction(this->conn_);

  // Remove associated comments
  Statement delComment(this->getPreparedStatement(STMT_DELETE_COMMENT));
  delComment.bindParam(0, entry.stat.st_ino);
  delComment.execute();

  // Remove directory itself
  Statement delDir(this->getPreparedStatement(STMT_DELETE_FILE));
  delDir.bindParam(0, entry.stat.st_ino);
  delDir.execute();

  // And decrement nlink
  Statement nlink(this->getPreparedStatement(STMT_UPDATE_NLINK));
  parent.stat.st_nlink--;
  nlink.bindParam(0, parent.stat.st_nlink);
  nlink.bindParam(1, parent.stat.st_ino);
  nlink.execute();

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
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       oldParent.acl, oldParent.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions on origin " + oldParentPath);
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       newParent.acl, newParent.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions on destination " + newParentPath);

  // If source is a directory, need write permissions there too
  if (S_ISDIR(old.stat.st_mode)) {
    if (checkPermissions(this->user_, this->group_, this->groups_,
                        old.acl, old.stat, S_IWRITE) != 0)
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
      this->user_.uid != oldParent.stat.st_uid &&
      this->user_.uid != old.stat.st_uid &&
      checkPermissions(this->user_, this->group_, this->groups_, old.acl, old.stat, S_IWRITE) != 0)
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
    Statement changeNameStmt(this->getPreparedStatement(STMT_CHANGE_NAME));

    changeNameStmt.bindParam(0, newName);
    changeNameStmt.bindParam(1, old.stat.st_ino);

    if (changeNameStmt.execute() == 0)
      throw DmException(DM_INTERNAL_ERROR, "Could not change the name");
  }

  // Change the parent if needed
  if (newParent.stat.st_ino != oldParent.stat.st_ino) {
    Statement changeParentStmt(this->getPreparedStatement(STMT_CHANGE_PARENT));

    changeParentStmt.bindParam(0, newParent.stat.st_ino);
    changeParentStmt.bindParam(1, old.stat.st_ino);

    if (changeParentStmt.execute() == 0)
      throw DmException(DM_INTERNAL_ERROR, "Could not update the parent ino!");

    // Reduce nlinks from old
    Statement oldNlinkStmt(this->getPreparedStatement(STMT_UPDATE_NLINK));

    oldNlinkStmt.bindParam(0, --oldParent.stat.st_nlink);
    oldNlinkStmt.bindParam(1, oldParent.stat.st_ino);

    if (oldNlinkStmt.execute() == 0)
      throw DmException(DM_INTERNAL_ERROR, "Could not update the old parent nlink!");

    // Increment from new
    Statement newNlinkStmt(this->getPreparedStatement(STMT_UPDATE_NLINK));

    newNlinkStmt.bindParam(0, ++newParent.stat.st_nlink);
    newNlinkStmt.bindParam(1, newParent.stat.st_ino);

    if (newNlinkStmt.execute() == 0)
      throw DmException(DM_INTERNAL_ERROR, "Could not update the new parent nlink!");
  }

  // Done!
  transaction.commit();
}



void NsMySqlCatalog::setUserId(uid_t uid, gid_t gid, const std::string& dn) throw(DmException)
{
  UserInfo  u = this->getUser(uid);
  GroupInfo g = this->getGroup(gid);

  if (u.name != dn && uid != 0) {
    throw DmException(DM_INVALID_VALUE, "The specified dn doesn't match "
                                        "the one associated with the id #%ld: "
                                        "'%s' != '%s'",
                                        uid, dn.c_str(), this->user_.name);
  }

  this->user_  = u;
  this->group_ = g;
  this->groups_.clear();
}



void NsMySqlCatalog::setVomsData(const std::string& vo, const std::vector<std::string>& fqans) throw (DmException)
{
  std::string group;

  // First, set the main group
  if (!vo.empty()) {
    group = voFromRole(vo);
    this->group_ = this->getGroup(group);
  }

  // Set the secondary
  this->groups_.clear();

  std::vector<std::string>::const_iterator i;

  for (i = fqans.begin(); i != fqans.end(); ++i) {
    group = voFromRole(*i);
    this->groups_.push_back(this->getGroup(group));
  }
}



void NsMySqlCatalog::getIdMap(const std::string& userName, const std::vector<std::string>& groupNames,
                              uid_t* uid, std::vector<gid_t>* gids) throw (DmException)
{
  std::string vo;
  UserInfo    user;
  GroupInfo   group;

  // Clear
  gids->clear();

  // User mapping
  try {
    user = this->getUser(userName);
  }
  catch (DmException e) {
    if (e.code() == DM_NO_SUCH_USER)
      user = this->newUser(userName, "");
    else
      throw;
  }
  *uid = user.uid;

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
    gids->push_back(group.gid);
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
      gids->push_back(group.gid);
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

  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_GROUP, "Group " + groupName + " not found");
  
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

  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_GROUP, "Group %ld not found", gid);

  return group;
}



UserInfo NsMySqlCatalog::newUser(const std::string& uname, const std::string& ca) throw (DmException)
{
  Transaction transaction(this->conn_);

  // Get the last uid, increment and update
  Statement uidStmt(this->getPreparedStatement(STMT_GET_UNIQ_UID_FOR_UPDATE));
  uid_t     uid;

  uidStmt.execute();
  uidStmt.bindResult(0, &uid);

  // Update the uid
  if (uidStmt.fetch()) {
    Statement updateUidStmt(this->getPreparedStatement(STMT_UPDATE_UNIQ_UID));
    ++uid;
    updateUidStmt.bindParam(0, uid);
    updateUidStmt.execute();
  }
  // Couldn't get, so insert it instead
  else {
    Statement insertUidStmt(this->getPreparedStatement(STMT_INSERT_UNIQ_UID));
    uid = 1;
    insertUidStmt.bindParam(0, uid);
    insertUidStmt.execute();
  }

  // Insert the user
  Statement userStmt(this->getPreparedStatement(STMT_INSERT_USER));

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
  Statement gidStmt(this->getPreparedStatement(STMT_GET_UNIQ_GID_FOR_UPDATE));
  gid_t     gid;

  gidStmt.execute();
  gidStmt.bindResult(0, &gid);

  // Update the gid
  if (gidStmt.fetch()) {
    Statement updateGidStmt(this->getPreparedStatement(STMT_UPDATE_UNIQ_GID));
    ++gid;
    updateGidStmt.bindParam(0, gid);
    updateGidStmt.execute();
  }
  // Couldn't get, so insert it instead
  else {
    Statement insertGidStmt(this->getPreparedStatement(STMT_INSERT_UNIQ_GID));
    gid = 1;
    insertGidStmt.bindParam(0, gid);
    insertGidStmt.execute();
  }

  // Insert the group
  Statement groupStmt(this->getPreparedStatement(STMT_INSERT_GROUP));

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
