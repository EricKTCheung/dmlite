/// @file    plugins/mysql/NsMySql.cpp
/// @brief   MySQL NS Implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstring>
#include <list>
#include <string>
#include <vector>
#include "NsMySql.h"

#include "../common/Uris.h"

using namespace dmlite;


/// Used to keep prepared statements
enum {
  STMT_GET_FILE_BY_ID = 0,
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
};



MYSQL_STMT* NsMySqlCatalog::getPreparedStatement(unsigned stId)
{
  if (this->preparedStmt_[stId] == 0x00) {

    if (mysql_select_db(this->conn_, this->nsDb_.c_str()) != 0)
      throw DmException(DM_QUERY_FAILED, std::string("Select DB: ") + mysql_error(this->conn_));

    this->preparedStmt_[stId] = mysql_stmt_init(this->conn_);
    if (mysql_stmt_prepare(this->preparedStmt_[stId],
                           statements[stId], strlen(statements[stId])) != 0) {
      throw DmException(DM_QUERY_FAILED, std::string("Prepare: ") + mysql_stmt_error(this->preparedStmt_[stId]));
    }
  }

  return this->preparedStmt_[stId];
}



NsMySqlCatalog::NsMySqlCatalog(PoolContainer<MYSQL*>* connPool, const std::string& db,
                               Catalog* decorates,
                               unsigned int symLinkLimit) throw(DmException):
                DummyCatalog(decorates), nsDb_(db), symLinkLimit_(symLinkLimit),
                preparedStmt_(STMT_SENTINEL, 0x00)
{
  this->connectionPool_ = connPool;
  this->conn_           = connPool->acquire();
  memset(&this->user_,  0x00, sizeof(UserInfo));
  memset(&this->group_, 0x00, sizeof(GroupInfo));
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



std::string NsMySqlCatalog::getImplId()
{
  return std::string("NsMySqlCatalog");
}



static void bindMetadata(MYSQL_STMT* stmt, MYSQL_BIND* bindResult, FileMetadata* meta) throw(DmException)
{
  // Values not used, so it doesn't really matter if they are not
  // thread-safe.
  // Beware!! These memory locations are used outside the scope of this function
  // by MySQL bindings, so keep them static!
  static unsigned long length;
  static my_bool       isNull;
  
  memset(bindResult, 0x00, sizeof(MYSQL_BIND) * 17);
  memset(meta,       0x00, sizeof(FileMetadata));
  
  // fileid
  bindResult[ 0].buffer_type = MYSQL_TYPE_LONGLONG;
  bindResult[ 0].buffer      = &meta->stStat.st_ino;
  // parent_fileid
  bindResult[ 1].buffer_type = MYSQL_TYPE_LONGLONG;
  bindResult[ 1].buffer      = &meta->parentId;
  // guid
  bindResult[ 2].buffer_type   = MYSQL_TYPE_STRING;
  bindResult[ 2].buffer_length = sizeof(meta->guid);
  bindResult[ 2].buffer        = meta->guid;
  bindResult[ 2].length        = &length;
  bindResult[ 2].is_null       = &isNull;
  // name
  bindResult[ 3].buffer_type   = MYSQL_TYPE_STRING;
  bindResult[ 3].buffer_length = sizeof(meta->name);
  bindResult[ 3].buffer        = meta->name;
  bindResult[ 3].length        = &length;
  // filemode
  bindResult[ 4].buffer_type = MYSQL_TYPE_LONG;
  bindResult[ 4].buffer      = &meta->stStat.st_mode;
  // nlink
  bindResult[ 5].buffer_type = MYSQL_TYPE_LONG;
  bindResult[ 5].buffer      = &meta->stStat.st_nlink;
  bindResult[ 5].is_null     = &isNull;
  // owner_uid
  bindResult[ 6].buffer_type = MYSQL_TYPE_LONG;
  bindResult[ 6].buffer      = &meta->stStat.st_uid;
  bindResult[ 6].is_null     = &isNull;
  // gid
  bindResult[ 7].buffer_type = MYSQL_TYPE_LONG;
  bindResult[ 7].buffer      = &meta->stStat.st_gid;
  bindResult[ 7].is_null     = &isNull;
  // filesize
  bindResult[ 8].buffer_type = MYSQL_TYPE_LONGLONG;
  bindResult[ 8].buffer      = &meta->stStat.st_size;
  bindResult[ 8].is_null     = &isNull;
  // atime
  bindResult[ 9].buffer_type = MYSQL_TYPE_LONG;
  bindResult[ 9].buffer      = &meta->stStat.st_atime;
  bindResult[ 9].is_null     = &isNull;
  // mtime
  bindResult[10].buffer_type = MYSQL_TYPE_LONG;
  bindResult[10].buffer      = &meta->stStat.st_mtime;
  bindResult[10].is_null     = &isNull;
  // ctime
  bindResult[11].buffer_type = MYSQL_TYPE_LONG;
  bindResult[11].buffer      = &meta->stStat.st_ctime;
  bindResult[11].is_null     = &isNull;
  // fileclass
  bindResult[12].buffer_type = MYSQL_TYPE_TINY;
  bindResult[12].buffer      = &meta->fileClass;
  bindResult[12].is_null     = &isNull;
  // status
  bindResult[13].buffer_type   = MYSQL_TYPE_STRING;
  bindResult[13].buffer_length = sizeof(meta->status);
  bindResult[13].buffer        = &meta->status;
  bindResult[13].length        = &length;
  bindResult[13].is_null       = &isNull;
  // csumtype
  bindResult[14].buffer_type   = MYSQL_TYPE_STRING;
  bindResult[14].buffer_length = sizeof(meta->csumtype);
  bindResult[14].buffer        = meta->csumtype;
  bindResult[14].length        = &length;
  bindResult[14].is_null       = &isNull;
  // csumvalue
  bindResult[15].buffer_type   = MYSQL_TYPE_STRING;
  bindResult[15].buffer_length = sizeof(meta->csumvalue);
  bindResult[15].buffer        = meta->csumvalue;
  bindResult[15].length        = &length;
  bindResult[15].is_null       = &isNull;
  // acl
  bindResult[16].buffer_type   = MYSQL_TYPE_MEDIUM_BLOB;
  bindResult[16].buffer_length = sizeof(meta->acl);
  bindResult[16].buffer        = meta->acl;
  bindResult[16].length        = &length;
  bindResult[16].is_null       = &isNull;

  if (mysql_stmt_bind_result(stmt, bindResult) != 0)
    throw DmException(DM_INTERNAL_ERROR, "Could not bind the MySQL variables");
}



FileMetadata NsMySqlCatalog::getFile(uint64_t fileId) throw(DmException)
{
  MYSQL_STMT*  stmt = this->getPreparedStatement(STMT_GET_FILE_BY_ID);;
  MYSQL_BIND   bindParam[1], bindResult[17];
  FileMetadata meta;

  // Bind parameters (fileid)
  memset(bindParam, 0, sizeof(bindParam));
  
  bindParam[0].buffer_type = MYSQL_TYPE_LONGLONG;
  bindParam[0].buffer  = &fileId;

  mysql_stmt_bind_param(stmt, bindParam);

  // Execute the query
  if (mysql_stmt_execute(stmt) != 0)
    throw DmException(DM_QUERY_FAILED, mysql_stmt_error(stmt));

  // Bind the result
  bindMetadata(stmt, bindResult, &meta);

  // Get the metadata
  switch (mysql_stmt_fetch(stmt)) {
    case 0:
      break;
    case MYSQL_NO_DATA:
      throw DmException(DM_NO_SUCH_FILE, "No such file or directory: #%lu", fileId);
      break;
    default:
      throw DmException(DM_INTERNAL_ERROR, mysql_stmt_error(stmt));
      break;
  }

  mysql_stmt_free_result(stmt);
  return meta;
}



FileMetadata NsMySqlCatalog::getFile(const std::string& name, uint64_t parent) throw(DmException)
{
  MYSQL_STMT*   stmt = this->getPreparedStatement(STMT_GET_FILE_BY_NAME);;
  MYSQL_BIND    bindParam[2], bindResult[17];
  long unsigned nameLen;
  FileMetadata  meta;

  // Bind parameters (parent_fileid and name)
  memset(bindParam, 0, sizeof(bindParam));
  bindParam[0].buffer_type = MYSQL_TYPE_LONGLONG;
  bindParam[0].buffer      = &parent;
  bindParam[1].buffer_type = MYSQL_TYPE_VARCHAR;
  bindParam[1].buffer      = (void*)name.c_str();
  nameLen = name.size();
  bindParam[1].length      = &nameLen;

  mysql_stmt_bind_param(stmt, bindParam);

  // Execute the query
  if (mysql_stmt_execute(stmt) != 0)
    throw DmException(DM_QUERY_FAILED, mysql_stmt_error(stmt));

  // Bind the result
  bindMetadata(stmt, bindResult, &meta);

  // Get the metadata
  switch (mysql_stmt_fetch(stmt)) {
    case 0:
      break;
    case MYSQL_NO_DATA:
      throw DmException(DM_NO_SUCH_FILE, "No such file or directory: " + name);
    default:
      throw DmException(DM_INTERNAL_ERROR, mysql_stmt_error(stmt));
  }

  mysql_stmt_free_result(stmt);
  return meta;
}



SymLink NsMySqlCatalog::getLink(uint64_t linkId) throw(DmException)
{
  MYSQL_STMT* stmt = this->getPreparedStatement(STMT_GET_SYMLINK);;
  MYSQL_BIND  bindParam[1], bindResult[2];
  SymLink     link;

  memset(&link, 0, sizeof(SymLink));

  // Bind parameters (fileid)
  memset(bindParam, 0x00, sizeof(bindParam));
  bindParam[0].buffer_type = MYSQL_TYPE_LONGLONG;
  bindParam[0].buffer      = &linkId;

  mysql_stmt_bind_param(stmt, bindParam);

  // Execute the query
  if (mysql_stmt_execute(stmt) != 0)
    throw DmException(DM_QUERY_FAILED, mysql_stmt_error(stmt));

  // Bind the result
  memset(bindResult, 0x00, sizeof(bindResult));
  bindResult[0].buffer_type   = MYSQL_TYPE_LONGLONG;
  bindResult[0].buffer        = &link.fileId;
  bindResult[1].buffer_type   = MYSQL_TYPE_BLOB;
  bindResult[1].buffer        = link.link;
  bindResult[1].buffer_length = sizeof(link.link);

  mysql_stmt_bind_result(stmt, bindResult);

  // Fetch
  switch (mysql_stmt_fetch(stmt)) {
    case 0:
      break;
    case MYSQL_NO_DATA:
      throw DmException(DM_NO_SUCH_FILE, "No such symbolic link: #%ul", linkId);
    default:
      throw DmException(DM_INTERNAL_ERROR, mysql_stmt_error(stmt));
  }

  mysql_stmt_free_result(stmt);
  return link;
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
    meta.stStat.st_mode = S_IFDIR | 0555 ;
  }
  // Relative, and cwd set, so start there
  else {
    meta   = this->cwdMeta_;
    parent = meta.stStat.st_ino;
  }
  

  while (!components.empty()) {
    // Check that the parent is a directory first
    if (!S_ISDIR(meta.stStat.st_mode) && !S_ISLNK(meta.stStat.st_mode))
      throw DmException(DM_NOT_DIRECTORY, "%s is not a directory", meta.name);
    // New element traversed! Need to check if it is possible to keep going.
    if (checkPermissions(this->user_, this->group_, this->groups_,
                         meta.acl, meta.stStat, S_IEXEC) != 0)
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
      meta   = this->getFile(parent);
      parent = meta.parentId;
    }
    // Regular entry
    else {
      meta = this->getFile(c, parent);

      // Symbolic link!, follow that instead
      if (S_ISLNK(meta.stStat.st_mode) && followSym) {
        SymLink link = this->getLink(meta.stStat.st_ino);

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
        parent = meta.stStat.st_ino;
      }
    }
    
  }

  return meta;
}



void NsMySqlCatalog::changeDir(const std::string& path) throw (DmException)
{
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
  return meta.stStat;
}



struct stat NsMySqlCatalog::stat(ino_t inode) throw (DmException)
{
  FileMetadata meta = this->getFile(inode);
  return meta.stStat;
}



struct stat NsMySqlCatalog::linkStat(const std::string& path) throw(DmException)
{
  FileMetadata meta = this->parsePath(path, false);
  return meta.stStat;
}



struct xstat NsMySqlCatalog::extendedStat(const std::string& path) throw (DmException)
{
  FileMetadata meta = this->parsePath(path);
  struct xstat xStat;

  xStat.stat = meta.stStat;
  strncpy(xStat.csumtype,  meta.csumtype,  SUMTYPE_MAX);
  strncpy(xStat.csumvalue, meta.csumvalue, SUMVALUE_MAX);
  strncpy(xStat.guid,      meta.guid,      GUID_MAX);

  return xStat;
}



struct xstat NsMySqlCatalog::extendedStat(ino_t inode) throw (DmException)
{
  FileMetadata meta = this->getFile(inode);
  struct xstat xStat;

  xStat.stat = meta.stStat;
  strncpy(xStat.csumtype,  meta.csumtype,  SUMTYPE_MAX);
  strncpy(xStat.csumvalue, meta.csumvalue, SUMVALUE_MAX);
  strncpy(xStat.guid,      meta.guid,      GUID_MAX);

  return xStat;
}



void NsMySqlCatalog::setUserId(uid_t uid, gid_t gid, const std::string& dn) throw(DmException)
{
  if (this->decorated_ != 0x00)
    this->decorated_->setUserId(uid, gid, dn);

  this->user_  = this->getUser(uid);
  this->group_ = this->getGroup(gid);

  if (this->user_.name != dn) {
    throw DmException(DM_INVALID_VALUE, "The specified dn doesn't match "
                                        "the one associated with the id #%ul: "
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
                       meta.acl, meta.stStat, S_IREAD) != 0) {
    throw DmException(DM_FORBIDDEN, "Not enough permissions to read " + path);
  }

  // Create the handle
  dir = new NsMySqlDir();
  memset(dir, 0x00, sizeof(NsMySqlDir));
  dir->dirId = meta.stStat.st_ino;

  try {
    // Pre-fetch the statement
    dir->statement = this->getPreparedStatement(STMT_GET_LIST_FILES);

    // Bind the input variable (parent id)
    memset(&dir->bindParam, 0x00, sizeof(dir->bindParam));
    dir->bindParam[0].buffer_type = MYSQL_TYPE_LONGLONG;
    dir->bindParam[0].buffer      = &dir->dirId;
    mysql_stmt_bind_param(dir->statement, dir->bindParam);

    // Execute the query
    if (mysql_stmt_execute(dir->statement) != 0)
      throw DmException(DM_QUERY_FAILED, mysql_stmt_error(dir->statement));

    // Pre-bind the output variables
    bindMetadata(dir->statement, dir->bindResult, &dir->current);

    // Use buffering
    mysql_stmt_store_result(dir->statement);

    // Done here
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

  mysql_stmt_free_result(dirp->statement);
  delete dirp;
}



struct dirent* NsMySqlCatalog::readDir(Directory* dir) throw(DmException)
{
  if (this->readDirx(dir) == 0)
    return 0x00;
  else
    return &(((NsMySqlDir*)dir)->extended.dirent);
}



struct direntstat* NsMySqlCatalog::readDirx(Directory* dir) throw(DmException)
{
  NsMySqlDir *dirp;

  if (dir == 0x00) {
    throw DmException(DM_NULL_POINTER, "Tried to read a null dir");
    return 0x00;
  }

  dirp = (NsMySqlDir*)dir;

  switch (mysql_stmt_fetch(dirp->statement)) {
    case 0:
      // Set stat structure
      memcpy(&dirp->extended.stat, &dirp->current.stStat, sizeof(struct stat));
      // Set dirent structure
      memset(&dirp->extended.dirent, 0x00, sizeof(struct dirent));      
      dirp->extended.dirent.d_ino  = dirp->extended.stat.st_ino;
      strncpy(dirp->extended.dirent.d_name,
              dirp->current.name,
              sizeof(dirp->extended.dirent.d_name));
      return &dirp->extended;
      break;
    case MYSQL_NO_DATA:
      return 0x00;
    default:
      throw DmException(DM_QUERY_FAILED, mysql_stmt_error(dirp->statement));
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
  return this->getUser(userName, 0);
}



UserInfo NsMySqlCatalog::getUser(uid_t uid) throw(DmException)
{
  return this->getUser(std::string(), uid);
}



UserInfo NsMySqlCatalog::getUser(const std::string& userName, uid_t uid) throw(DmException)
{
  MYSQL_STMT*   uStmt;
  MYSQL_BIND    bindParam[1], bindResult[4];
  long unsigned len;
  my_bool       isNull;
  UserInfo      user;

  // Get user info
  memset(bindParam, 0, sizeof(bindParam));
  memset(&user, 0, sizeof(UserInfo));

  if (!userName.empty()) {
    uStmt = this->getPreparedStatement(STMT_GET_USERINFO_BY_NAME);
    len = userName.size();
    bindParam[0].buffer_type = MYSQL_TYPE_VARCHAR;
    bindParam[0].buffer      = (void*)userName.c_str();
    bindParam[0].length      = &len;
  }
  else {
    uStmt = this->getPreparedStatement(STMT_GET_USERINFO_BY_UID);
    bindParam[0].buffer_type = MYSQL_TYPE_LONG;
    bindParam[0].buffer      = &uid;
  }

  if (mysql_stmt_bind_param(uStmt, bindParam) != 0)
    throw DmException(DM_QUERY_FAILED, mysql_stmt_error(uStmt));

  if (mysql_stmt_execute(uStmt) != 0)
    throw DmException(DM_QUERY_FAILED, mysql_stmt_error(uStmt));

  memset(bindResult, 0, sizeof(bindResult));
  bindResult[0].buffer_type   = MYSQL_TYPE_LONG;
  bindResult[0].buffer        = &user.uid;
  bindResult[0].is_null       = &isNull;

  bindResult[1].buffer_type   = MYSQL_TYPE_STRING;
  bindResult[1].buffer        = user.name;
  bindResult[1].buffer_length = sizeof(user.name);
  bindResult[1].length        = &len;
  bindResult[1].is_null       = &isNull;

  bindResult[2].buffer_type   = MYSQL_TYPE_STRING;
  bindResult[2].buffer        = user.ca;
  bindResult[2].buffer_length = sizeof(user.ca);
  bindResult[2].length        = &len;
  bindResult[2].is_null       = &isNull;

  bindResult[3].buffer_type   = MYSQL_TYPE_LONG;
  bindResult[3].buffer        = &user.banned;
  bindResult[3].is_null       = &isNull;

  if (mysql_stmt_bind_result(uStmt, bindResult) != 0)
    throw DmException(DM_QUERY_FAILED, mysql_stmt_error(uStmt));

  try {
    switch (mysql_stmt_fetch(uStmt)) {
      case 0:
        break;
      case MYSQL_NO_DATA:
        if (!userName.empty())
          throw DmException(DM_NO_SUCH_USER, "Could not find the user " + userName);
        else
          throw DmException(DM_NO_SUCH_USER, "Could not find the user #%lu", uid);
      default:
        throw DmException(DM_QUERY_FAILED, mysql_stmt_error(uStmt));
    }
    mysql_stmt_free_result(uStmt);
  }
  catch (...) {
    mysql_stmt_free_result(uStmt);
    throw;
  }

  return user;
}



GroupInfo NsMySqlCatalog::getGroup(const std::string& groupName) throw(DmException)
{
  return this->getGroup(groupName, 0);
}



GroupInfo NsMySqlCatalog::getGroup(gid_t gid) throw(DmException)
{
  return this->getGroup(std::string(), gid);
}



GroupInfo NsMySqlCatalog::getGroup(const std::string& groupName, gid_t gid) throw(DmException)
{
  MYSQL_STMT*   gStmt;
  MYSQL_BIND    bindParam[1], bindResult[3];
  long unsigned len;
  my_bool       isNull;
  GroupInfo     group;

  // Get user info
  memset(bindParam, 0, sizeof(bindParam));
  memset(&group, 0, sizeof(GroupInfo));

  if (!groupName.empty()) {
    gStmt = this->getPreparedStatement(STMT_GET_GROUPINFO_BY_NAME);
    len = groupName.size();
    bindParam[0].buffer_type = MYSQL_TYPE_VARCHAR;
    bindParam[0].buffer      = (void*)groupName.c_str();
    bindParam[0].length      = &len;
  }
  else {
    gStmt = this->getPreparedStatement(STMT_GET_GROUPINFO_BY_GID);
    bindParam[0].buffer_type   = MYSQL_TYPE_LONG;
    bindParam[0].buffer        = &gid;
  }

  mysql_stmt_bind_param(gStmt, bindParam);

  if (mysql_stmt_execute(gStmt) != 0)
    throw DmException(DM_QUERY_FAILED, mysql_stmt_error(gStmt));

  memset(bindResult, 0, sizeof(bindResult));
  bindResult[0].buffer_type   = MYSQL_TYPE_LONG;
  bindResult[0].buffer        = &group.gid;
  bindResult[0].is_null       = &isNull;

  bindResult[1].buffer_type   = MYSQL_TYPE_STRING;
  bindResult[1].buffer        = group.name;
  bindResult[1].buffer_length = sizeof(group.name);
  bindResult[1].length        = &len;
  bindResult[1].is_null       = &isNull;

  bindResult[2].buffer_type   = MYSQL_TYPE_LONG;
  bindResult[2].buffer        = &group.banned;
  bindResult[2].is_null       = &isNull;

  mysql_stmt_bind_result(gStmt, bindResult);

  try {
    switch (mysql_stmt_fetch(gStmt)) {
      case 0:
        break;
      case MYSQL_NO_DATA:
        if (!groupName.empty())
          throw DmException(DM_NO_SUCH_GROUP, "Could not find the group " + groupName);
        else
          throw DmException(DM_NO_SUCH_GROUP, "Could not find the group #%lu", gid);
      default:
        throw DmException(DM_QUERY_FAILED, mysql_stmt_error(gStmt));
    }
    mysql_stmt_free_result(gStmt);
  }
  catch (...) {
    mysql_stmt_free_result(gStmt);
    throw;
  }

  return group;
}



std::vector<FileReplica> NsMySqlCatalog::getReplicas(const std::string& path) throw(DmException)
{
  FileMetadata  meta;
  FileReplica   replica;
  MYSQL_STMT   *stmt;
  MYSQL_BIND    bindParam[1], bindResult[4];
  int           r = 0, nReplicas;
  unsigned long length;
  my_bool       isNull;

  // Need to grab the file first
  meta = this->parsePath(path, true);

  // The file exists, plus we have permissions to go there. Check we can read
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.stStat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN,
                   "Not enough permissions to read " + path);

  // MySQL statement
  stmt = this->getPreparedStatement(STMT_GET_FILE_REPLICAS);
  
  // Bind parameters
  memset(bindParam, 0, sizeof(bindParam));
  bindParam[0].buffer_type = MYSQL_TYPE_LONGLONG;
  bindParam[0].buffer      = &(meta.stStat.st_ino);

  mysql_stmt_bind_param(stmt, bindParam);

  // Execute query
  if (mysql_stmt_execute(stmt) != 0)
    throw DmException(DM_QUERY_FAILED, mysql_stmt_error(stmt));

  // Bind result
  memset(bindResult, 0, sizeof(bindResult));
  bindResult[0].buffer_type   = MYSQL_TYPE_LONGLONG;
  bindResult[0].buffer        = &replica.replicaid;

  bindResult[1].buffer_type   = MYSQL_TYPE_LONGLONG;
  bindResult[1].buffer        = &replica.fileid;

  bindResult[2].buffer_type   = MYSQL_TYPE_STRING;
  bindResult[2].buffer        = &replica.status;
  bindResult[2].buffer_length = sizeof(replica.status);
  bindResult[2].length        = &length;
  bindResult[2].is_null       = &isNull;

  bindResult[3].buffer_type   = MYSQL_TYPE_STRING;
  bindResult[3].buffer        = replica.unparsed_location;
  bindResult[3].buffer_length = sizeof(replica.unparsed_location);
  bindResult[3].length        = &length;
  bindResult[3].is_null       = &isNull;

  mysql_stmt_bind_result(stmt, bindResult);
  mysql_stmt_store_result(stmt);

  try {
    std::vector<FileReplica> replicas;
    
    nReplicas = mysql_stmt_num_rows(stmt);
    if (nReplicas == 0)
      throw DmException(DM_NO_REPLICAS, "No replicas available for " + path);

    // Fetch
    int i = 0;
    while ((r = mysql_stmt_fetch(stmt)) == 0) {
      replica.location = splitUri(replica.unparsed_location);
      replicas.push_back(replica);
      ++i;
    };

    switch (r) {
      case MYSQL_NO_DATA:
        break;
      case MYSQL_DATA_TRUNCATED:
        throw DmException(DM_QUERY_FAILED, std::string("getReplicas: Data truncated"));
      default:
        throw DmException(DM_QUERY_FAILED, std::string(mysql_stmt_error(stmt)));
    }
    mysql_stmt_free_result(stmt);

    return replicas;
  }
  catch (...) {
    mysql_stmt_free_result(stmt);
    throw;
  }
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



std::vector<ExtendedReplica> NsMySqlCatalog::getExReplicas(const std::string& path) throw(DmException)
{
  FileMetadata    meta;
  ExtendedReplica replica;
  MYSQL_STMT     *stmt;
  MYSQL_BIND      bindParam[1], bindResult[7];
  int             r = 0, nReplicas;
  unsigned long   length;
  my_bool         isNull;

  // Need to grab the file first
  meta = this->parsePath(path, true);

  // The file exists, plus we have permissions to go there. Check we can read
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.stStat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN,
                   "Not enough permissions to read " + path);

  // MySQL statement
  stmt = this->getPreparedStatement(STMT_GET_FILE_REPLICAS_EXTENDED);

  // Bind parameters
  memset(bindParam, 0, sizeof(bindParam));
  bindParam[0].buffer_type = MYSQL_TYPE_LONGLONG;
  bindParam[0].buffer      = &(meta.stStat.st_ino);

  mysql_stmt_bind_param(stmt, bindParam);

  // Execute query
  if (mysql_stmt_execute(stmt) != 0)
    throw DmException(DM_QUERY_FAILED, mysql_stmt_error(stmt));

  // Bind result
  memset(bindResult, 0, sizeof(bindResult));
  bindResult[0].buffer_type   = MYSQL_TYPE_LONGLONG;
  bindResult[0].buffer        = &replica.replica.replicaid;

  bindResult[1].buffer_type   = MYSQL_TYPE_LONGLONG;
  bindResult[1].buffer        = &replica.replica.fileid;

  bindResult[2].buffer_type   = MYSQL_TYPE_STRING;
  bindResult[2].buffer        = &replica.replica.status;
  bindResult[2].buffer_length = sizeof(replica.replica.status);
  bindResult[2].length        = &length;
  bindResult[2].is_null       = &isNull;

  bindResult[3].buffer_type   = MYSQL_TYPE_STRING;
  bindResult[3].buffer        = replica.replica.unparsed_location;
  bindResult[3].buffer_length = sizeof(replica.replica.unparsed_location);
  bindResult[3].length        = &length;
  bindResult[3].is_null       = &isNull;

  bindResult[4].buffer_type   = MYSQL_TYPE_STRING;
  bindResult[4].buffer        = replica.pool;
  bindResult[4].buffer_length = sizeof(replica.pool);
  bindResult[4].length        = &length;
  bindResult[4].is_null       = &isNull;

  bindResult[5].buffer_type   = MYSQL_TYPE_STRING;
  bindResult[5].buffer        = replica.host;
  bindResult[5].buffer_length = sizeof(replica.host);
  bindResult[5].length        = &length;
  bindResult[5].is_null       = &isNull;

  bindResult[6].buffer_type   = MYSQL_TYPE_STRING;
  bindResult[6].buffer        = replica.fs;
  bindResult[6].buffer_length = sizeof(replica.fs);
  bindResult[6].length        = &length;
  bindResult[6].is_null       = &isNull;


  mysql_stmt_bind_result(stmt, bindResult);
  mysql_stmt_store_result(stmt);

  try {
    std::vector<ExtendedReplica> replicas;

    nReplicas = mysql_stmt_num_rows(stmt);
    if (nReplicas == 0)
      throw DmException(DM_NO_REPLICAS, "No replicas available for " + path);

    // Fetch
    int i = 0;
    while ((r = mysql_stmt_fetch(stmt)) == 0) {
      replica.replica.location = splitUri(replica.replica.unparsed_location);
      replicas.push_back(replica);
      ++i;
    };

    switch (r) {
      case MYSQL_NO_DATA:
        break;
      case MYSQL_DATA_TRUNCATED:
        throw DmException(DM_QUERY_FAILED, "getReplicas: Data truncated");
      default:
        throw DmException(DM_QUERY_FAILED, mysql_stmt_error(stmt));
    }
    mysql_stmt_free_result(stmt);

    return replicas;
  }
  catch (...) {
    mysql_stmt_free_result(stmt);
    throw;
  }
}



std::string NsMySqlCatalog::getComment(const std::string& path) throw(DmException)
{
  MYSQL_STMT  *stmt = this->getPreparedStatement(STMT_GET_COMMENT);
  MYSQL_BIND   bindParam[1], bindResult[1];
  FileMetadata meta;
  char         comment[COMMENT_MAX];

  // Get the file
  meta = this->parsePath(path, true);

  // Check we can read
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.stStat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to read " + path);

  // Bind params
  memset(bindParam, 0, sizeof(bindParam));
  bindParam[0].buffer_type = MYSQL_TYPE_LONGLONG;
  bindParam[0].buffer      = &(meta.stStat.st_ino);

  mysql_stmt_bind_param(stmt, bindParam);

  // Execute query
  if (mysql_stmt_execute(stmt) != 0)
    throw DmException(DM_QUERY_FAILED, mysql_stmt_error(stmt));


  // Bind result
  memset(bindResult, 0, sizeof(bindResult));
  bindResult[0].buffer_type   = MYSQL_TYPE_STRING;
  bindResult[0].buffer        = comment;
  bindResult[0].buffer_length = 256;

  mysql_stmt_bind_result(stmt, bindResult);

  // Fetch
  try {
    switch (mysql_stmt_fetch(stmt)) {
      case 0:
        break;
      case MYSQL_NO_DATA:
        throw DmException(DM_NO_COMMENT, "There is no comment for " + path);
      case MYSQL_DATA_TRUNCATED:
        throw DmException(DM_QUERY_FAILED, "getComment: Data truncated");
      default:
        throw DmException(DM_QUERY_FAILED, mysql_stmt_error(stmt));
    }
    mysql_stmt_free_result(stmt);
  }
  catch (...) {
    mysql_stmt_free_result(stmt);
    throw;
  }

  return std::string(comment);
}



void NsMySqlCatalog::setGuid(const std::string& path, const std::string& guid) throw (DmException)
{
  MYSQL_STMT   *stmt = this->getPreparedStatement(STMT_SET_GUID);
  MYSQL_BIND    bindParam[2];
  long unsigned guidLen = guid.length();
  FileMetadata  meta    = this->parsePath(path);

  memset(bindParam, 0, sizeof(bindParam));

  bindParam[0].buffer_type = MYSQL_TYPE_VARCHAR;
  bindParam[0].length      = &guidLen;
  bindParam[0].buffer      = (void*)guid.c_str();
  bindParam[1].buffer_type = MYSQL_TYPE_LONGLONG;
  bindParam[1].buffer      = &meta.stStat.st_ino;

  mysql_stmt_bind_param(stmt, bindParam);

  if (mysql_stmt_execute(stmt) != 0)
    throw DmException(DM_QUERY_FAILED, mysql_stmt_error(stmt));  
}
