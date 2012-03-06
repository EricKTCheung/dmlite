/// @file    plugins/oracle/NsOracle.cpp
/// @brief   Oracle NS Implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include "NsOracle.h"
#include <assert.h>
#include <dmlite/common/Uris.h>
#include <string.h>
#include <stdlib.h>

using namespace dmlite;
using namespace oracle;

#define NOT_IMPLEMENTED(p)\
p {\
  throw DmException(DM_NOT_IMPLEMENTED, #p" not implemented");\
}


/// Just to do rollbacks easier, as they will happen
/// by default on destruction (i.e. Exceptions)
class Transaction {
private:
  occi::Connection* conn_;
  bool              pending_;
public:
  Transaction(occi::Connection* conn): conn_(conn), pending_(true) {
    // Nothing
  }

  ~Transaction() {
    if (this->pending_)
      this->conn_->rollback();
  }

  void rollback() {
    assert(this->pending_ == true);
    this->conn_->rollback();
    this->pending_ = false;
  }

  void commit() {
    assert(this->pending_ == true);
    this->conn_->commit();
    this->pending_ = false;
  }
};


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
  STMT_UTIME,
  STMT_SENTINEL
};

/// Used internally to define prepared statements.
/// Must match with STMT_* constants!
static const char* statements[] = {
  "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
          filesize, atime, mtime, ctime, fileclass, status,\
          csumtype, csumvalue, acl\
        FROM Cns_file_metadata\
        WHERE fileid = :b_fileid",
  "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
          filesize, atime, mtime, ctime, fileclass, status,\
          csumtype, csumvalue, acl\
        FROM Cns_file_metadata\
        WHERE guid = :b_guid",
  "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
          filesize, atime, mtime, ctime, fileclass, status,\
          csumtype, csumvalue, acl\
        FROM Cns_file_metadata \
        WHERE parent_fileid = :b_parent AND name = :b_name",
  "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
          filesize, atime, mtime, ctime, fileclass, status,\
          csumtype, csumvalue, acl\
        FROM Cns_file_metadata \
        WHERE parent_fileid = :b_parent",
  "SELECT fileid, linkname FROM Cns_symlinks WHERE fileid = :b_fileid",
  "SELECT userid, username, user_ca, NVL(banned, 0)\
        FROM Cns_userinfo\
        WHERE username = :b_username",
  "SELECT userid, username, user_ca, NVL(banned, 0)\
        FROM Cns_userinfo\
        WHERE userid = :b_uid",
  "SELECT gid, groupname, NVL(banned, 0)\
        FROM Cns_groupinfo\
        WHERE groupname = :b_groupname",
  "SELECT gid, groupname, NVL(banned, 0)\
        FROM Cns_groupinfo\
        WHERE gid = :b_gid",
  "SELECT DBMS_ROWID.ROWID_BLOCK_NUMBER(rowid), fileid, status, sfn\
        FROM Cns_file_replica\
        WHERE fileid = :b_fileid",
  "SELECT DBMS_ROWID.ROWID_BLOCK_NUMBER(rowid), fileid, status, sfn, poolname, host, fs\
        FROM Cns_file_replica\
        WHERE fileid = :b_fileid",
  "SELECT comments\
        FROM Cns_user_metadata\
        WHERE u_fileid = :b_fileid",
  "UPDATE Cns_file_metadata\
        SET guid = :b_guid\
        WHERE fileid = :b_fileid",
  "UPDATE Cns_user_metadata\
        SET comments = :b_comment\
        WHERE u_fileid = :b_fileid",
  "INSERT INTO Cns_user_metadata\
          (u_fileid, comments)\
        VALUES\
          (:b_fileid, :b_comment)",
  "INSERT INTO Cns_file_metadata\
          (fileid, parent_fileid, name, filemode, nlink, owner_uid, gid,\
           filesize, atime, mtime, ctime, fileclass, status,\
           csumtype, csumvalue, acl)\
        VALUES\
          (Cns_unique_id.nextval, :b_parentid, :b_name, :b_mode, :b_nlink, :b_uid, :b_gid,\
           :b_size, :b_atime, :b_mtime, :b_ctime, :b_fclass, :b_status,\
           :b_sumtype, :b_sumvalue, :b_acl)",
  "INSERT INTO Cns_symlinks\
          (fileid, linkname)\
        VALUES\
          (:b_fileid, :b_linkname)",
  "DELETE FROM Cns_file_metadata WHERE fileid = :b_fileid",
  "DELETE FROM Cns_user_metadata WHERE u_fileid = :b_fileid",
  "DELETE FROM Cns_symlinks WHERE fileid = :b_fileid",
  "UPDATE Cns_file_metadata\
        SET nlink = :b_nlink, mtime = :b_mtime, ctime = :b_ctime\
        WHERE fileid = :b_fileid",
  "UPDATE Cns_file_metadata\
        SET filemode = :b_mode, ctime = :b_ctime\
        WHERE fileid = :b_fileid",
  "DELETE FROM Cns_file_replica\
        WHERE fileid = :b_fileid AND sfn = :b_sfn",
  "INSERT INTO Cns_file_replica\
          (fileid, nbaccesses,\
           ctime, atime, ptime, ltime,\
           r_type, status, f_type,\
           setname, poolname, host, fs, sfn)\
        VALUES\
          (:b_fileid, 0,\
           :b_ctime, :b_atime, :b_ptime, :b_ltime,\
           :b_rtype, :b_status, :b_ftype,\
           :b_setname, :b_pool, :b_host, :b_fs, :b_sfn)",
  "UPDATE Cns_file_metadata\
        SET filesize = 0, ctime = :b_ctime\
        WHERE fileid = :b_fileid",
  "SELECT id FROM Cns_unique_uid FOR UPDATE",
  "SELECT id FROM Cns_unique_gid FOR UPDATE",
  "UPDATE Cns_unique_uid SET id = :b_uid",
  "UPDATE Cns_unique_gid SET id = :b_gid",
  "INSERT INTO Cns_unique_uid (id) VALUES (:b_uid)",
  "INSERT INTO Cns_unique_gid (id) VALUES (:b_gid)",
  "INSERT INTO Cns_userinfo\
          (userid, username, user_ca, banned)\
        VALUES\
          (:b_uid, :b_username, :b_ca, :b_banned)",
  "INSERT INTO Cns_groupinfo\
          (gid, groupname, banned)\
        VALUES\
          (:b_gid, :b_groupname, :b_banned)",
  "UPDATE Cns_file_metadata\
        SET name = :b_name, ctime = :b_ctime\
        WHERE fileid = :b_fileid",
  "UPDATE Cns_file_metadata\
        SET parent_fileid = :b_parent, ctime = :b_ctime\
        WHERE fileid = :b_fileid",
  "UPDATE Cns_file_metadata\
        SET owner_uid = :b_uid, gid = :b_gid, ctime = :b_ctime\
        WHERE fileid = :b_fileid",
  "UPDATE Cns_file_metadata\
        SET atime = :b_atime, mtime = :b_mtime, ctime = :b_ctime\
        WHERE fileid = :b_fileid",
};



occi::Statement* NsOracleCatalog::getPreparedStatement(unsigned stId)
{
  if (this->preparedStmt_[stId] == 0x00) {
    try {
      this->preparedStmt_[stId] = this->conn_->createStatement(statements[stId]);
      // Make sure autocommit is always disabled
      this->preparedStmt_[stId]->setAutoCommit(false);
    }
    catch (occi::SQLException& ex) {
      throw DmException(DM_QUERY_FAILED, std::string("Prepare: ") + ex.getMessage());
    }
  }
  return this->preparedStmt_[stId];
}



NsOracleCatalog::NsOracleCatalog(oracle::occi::ConnectionPool* pool,
                                 oracle::occi::Connection* conn,
                                 unsigned int symLimit) throw (DmException):
  pool_(pool), conn_(conn), cwd_(0), umask_(022), symLinkLimit_(symLimit),
  preparedStmt_(STMT_SENTINEL, 0x00)
{
  memset(&this->user_,  0x00, sizeof(UserInfo));
  memset(&this->group_, 0x00, sizeof(GroupInfo));
}



NsOracleCatalog::~NsOracleCatalog() throw (DmException)
{
  std::vector<occi::Statement*>::iterator i;

  for (i = this->preparedStmt_.begin(); i != this->preparedStmt_.end(); i++) {
    if (*i != 0x00)
      this->conn_->terminateStatement(*i);
  }
  this->pool_->terminateConnection(this->conn_);
}



std::string NsOracleCatalog::getImplId() throw ()
{
  return std::string("NsOracleCatalog");
}



void NsOracleCatalog::set(const std::string& key, va_list varg) throw(DmException)
{
  throw DmException(DM_UNKNOWN_OPTION, "Option " + key + " unknown");
}



static void getMetadata(occi::ResultSet* rs, ExtendedStat* meta) throw(DmException)
{
  meta->stat.st_ino   = rs->getNumber( 1);
  meta->parent        = rs->getNumber( 2);

  strncpy(meta->guid, rs->getString(3).c_str(), sizeof(meta->guid));
  strncpy(meta->name, rs->getString(4).c_str(), sizeof(meta->name));

  meta->stat.st_mode  = rs->getNumber( 5);
  meta->stat.st_nlink = rs->getNumber( 6);
  meta->stat.st_uid   = rs->getNumber( 7);
  meta->stat.st_gid   = rs->getNumber( 8);
  meta->stat.st_size  = rs->getNumber( 9);
  meta->stat.st_atime = rs->getNumber(10);
  meta->stat.st_mtime = rs->getNumber(11);
  meta->stat.st_ctime = rs->getNumber(12);
  meta->type          = rs->getNumber(13);
  meta->status        = rs->getString(14)[0];

  strncpy(meta->csumtype, rs->getString(15).c_str(), sizeof(meta->csumtype));
  strncpy(meta->csumvalue, rs->getString(16).c_str(), sizeof(meta->csumvalue));
  
  strncpy(meta->acl, rs->getString(17).c_str(), sizeof(meta->acl));
}



ExtendedStat NsOracleCatalog::extendedStat(uint64_t fileId) throw(DmException)
{
  ExtendedStat     meta;
  occi::ResultSet* rs   = 0x00;
  occi::Statement* stmt = this->getPreparedStatement(STMT_GET_FILE_BY_ID);


  memset(&meta, 0x00, sizeof(ExtendedStat));

  try {
    stmt->setNumber(1, fileId);
    rs = stmt->executeQuery();
    if (!rs->next()) {
      stmt->closeResultSet(rs);
      throw DmException(DM_NO_SUCH_FILE, "File %ld not found", fileId);
    }
    getMetadata(rs, &meta);
  }
  catch (occi::SQLException& ex) {
    if (rs)
      stmt->closeResultSet(rs);
    throw DmException(DM_QUERY_FAILED, ex.getMessage());
  }

  stmt->closeResultSet(rs);
  return meta;
}



ExtendedStat NsOracleCatalog::guidStat(const std::string& guid) throw (DmException)
{
  ExtendedStat     meta;
  occi::ResultSet* rs   = 0x00;
  occi::Statement* stmt = this->getPreparedStatement(STMT_GET_FILE_BY_GUID);

  memset(&meta, 0x00, sizeof(ExtendedStat));

  try {
    stmt->setString(1, guid);
    rs = stmt->executeQuery();
    if (!rs->next()) {
      stmt->closeResultSet(rs);
      throw DmException(DM_NO_SUCH_FILE, "File with guid " + guid + " not found");
    }
    getMetadata(rs, &meta);
  }
  catch (occi::SQLException& ex) {
    if (rs)
      stmt->closeResultSet(rs);
    throw DmException(DM_QUERY_FAILED, ex.getMessage());
  }

  stmt->closeResultSet(rs);
  return meta;
}



ExtendedStat NsOracleCatalog::extendedStat(uint64_t parent, const std::string& name) throw(DmException)
{
  ExtendedStat meta;
  occi::ResultSet* rs   = 0x00;
  occi::Statement* stmt = this->getPreparedStatement(STMT_GET_FILE_BY_NAME);

  memset(&meta, 0x00, sizeof(ExtendedStat));

  try {
    stmt->setNumber(1, parent);
    stmt->setString(2, name);
    rs = stmt->executeQuery();
    if (!rs->next()) {
      stmt->closeResultSet(rs);
      throw DmException(DM_NO_SUCH_FILE, name + " not found");
    }
    getMetadata(rs, &meta);
  }
  catch (occi::SQLException& ex) {
    if (rs)
      stmt->closeResultSet(rs);
    throw DmException(DM_QUERY_FAILED, ex.getMessage());
  }

  stmt->closeResultSet(rs);
  return meta;
}



SymLink NsOracleCatalog::readLink(uint64_t linkId) throw(DmException)
{
  SymLink link;
  occi::ResultSet* rs   = 0x00;
  occi::Statement* stmt = this->getPreparedStatement(STMT_GET_SYMLINK);

  memset(&link, 0x00, sizeof(SymLink));

  try {
    stmt->setNumber(1, linkId);
    rs = stmt->executeQuery();
    if (!rs->next()) {
      stmt->closeResultSet(rs);
      throw DmException(DM_NO_SUCH_FILE, "Link %ld not found", linkId);
    }
    link.fileId = rs->getNumber(1);
    strncpy(link.link, rs->getString(2).c_str(), sizeof(link.link));
  }
  catch (occi::SQLException& ex) {
    if (rs)
      stmt->closeResultSet(rs);
    throw DmException(DM_QUERY_FAILED, ex.getMessage());
  }

  stmt->closeResultSet(rs);
  return link;
}



ExtendedStat NsOracleCatalog::newFile(ExtendedStat& parent, const std::string& name,
                                      mode_t mode, long nlink, size_t size,
                                      short type, char status,
                                      const std::string& csumtype,
                                      const std::string& csumvalue,
                                      const std::string& acl) throw (DmException)
{
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

  // Check SGID
  if (parent.stat.st_mode & S_ISGID) {
    egid = parent.stat.st_gid;
    mode |= S_ISGID;
  }
  else {
    egid = this->group_.gid;
  }

  // Create the entry
  occi::Statement* fileStmt = this->getPreparedStatement(STMT_INSERT_FILE);

  fileStmt->setNumber( 1, parent.stat.st_ino);
  fileStmt->setString( 2, name);
  fileStmt->setNumber( 3, mode);
  fileStmt->setNumber( 4, nlink);
  fileStmt->setNumber( 5, this->user_.uid);
  fileStmt->setNumber( 6, egid);
  fileStmt->setNumber( 7, size);
  fileStmt->setNumber( 8, time(NULL));
  fileStmt->setNumber( 9, time(NULL));
  fileStmt->setNumber(10, time(NULL));
  fileStmt->setNumber(11, type);
  fileStmt->setString(12, std::string(&status, 1));
  fileStmt->setString(13, csumtype);
  fileStmt->setString(14, csumvalue);
  fileStmt->setString(15, acl);

  fileStmt->executeUpdate();

  // Increment the nlink
  occi::Statement* nlinkStmt = this->getPreparedStatement(STMT_UPDATE_NLINK);

  parent.stat.st_nlink++;
  nlinkStmt->setNumber(1, parent.stat.st_nlink);
  nlinkStmt->setNumber(2, time(NULL));
  nlinkStmt->setNumber(3, time(NULL));
  nlinkStmt->setNumber(4, parent.stat.st_ino);

  nlinkStmt->executeUpdate();

  // Return back
  return this->extendedStat(parent.stat.st_ino, name);
}



ExtendedStat NsOracleCatalog::getParent(const std::string& path,
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



ExtendedStat NsOracleCatalog::extendedStat(const std::string& path, bool followSym) throw(DmException)
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



void NsOracleCatalog::traverseBackwards(const ExtendedStat& meta) throw (DmException)
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



void NsOracleCatalog::changeDir(const std::string& path) throw (DmException)
{
  ExtendedStat cwdMeta = this->extendedStat(path);
  this->cwdPath_ = path;
  this->cwd_     = cwdMeta.stat.st_ino;
}



std::string NsOracleCatalog::getWorkingDir(void) throw (DmException)
{
  return this->cwdPath_;
}



ino_t NsOracleCatalog::getWorkingDirI(void) throw (DmException)
{
  return this->cwd_;
}



Directory* NsOracleCatalog::openDir(const std::string& path) throw(DmException)
{
  NsOracleDir  *dir;
  ExtendedStat meta;

  // Get the directory
  meta = this->extendedStat(path);

  // Can we read it?
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to read " + path);

  // Touch
  struct utimbuf tim;
  tim.actime  = time(NULL);
  tim.modtime = meta.stat.st_mtime;
  this->utime(meta.stat.st_ino, &tim);

  // Create the handle
  dir = new NsOracleDir();
  dir->dirId = meta.stat.st_ino;

  try {
    dir->stmt = this->getPreparedStatement(STMT_GET_LIST_FILES);
    dir->stmt->setNumber(1, dir->dirId);
    dir->rs = dir->stmt->executeQuery();
    return dir;
  }
  catch (...) {
    delete dir;
    throw;
  }
}



void NsOracleCatalog::closeDir(Directory* dir) throw(DmException)
{
  NsOracleDir *dirp;

  if (dir == 0x00)
    throw DmException(DM_NULL_POINTER, std::string("Tried to close a null dir"));

  dirp = (NsOracleDir*)dir;

  dirp->stmt->closeResultSet(dirp->rs);
  delete dirp;
}



struct dirent* NsOracleCatalog::readDir(Directory* dir) throw(DmException)
{
  if (this->readDirx(dir) == 0)
    return 0x00;
  else
    return &(((NsOracleDir*)dir)->ds.dirent);
}



struct direntstat* NsOracleCatalog::readDirx(Directory* dir) throw(DmException)
{
  NsOracleDir *dirp;

  if (dir == 0x00)
    throw DmException(DM_NULL_POINTER, "Tried to read a null dir");

  dirp = (NsOracleDir*)dir;

  if (dirp->rs->next()) {
    getMetadata(dirp->rs, &dirp->current);
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



void NsOracleCatalog::addReplica(const std::string& guid, int64_t id,
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
  try {
    Transaction transaction(this->conn_);
    
    occi::Statement* stmt = this->getPreparedStatement(STMT_ADD_REPLICA);

    stmt->setNumber( 1, meta.stat.st_ino);
    stmt->setNumber( 2, time(NULL));
    stmt->setNumber( 3, time(NULL));
    stmt->setNumber( 4, time(NULL));
    stmt->setNumber( 5, time(NULL));

    stmt->setNull  ( 6, occi::OCCISTRING);
    stmt->setString( 7, std::string(&status, 1));
    stmt->setString( 8, std::string(&fileType, 1));
    stmt->setNull  ( 9, occi::OCCISTRING);
    stmt->setString(10, poolName);
    stmt->setString(11, server);
    stmt->setString(12, fileSystem);
    stmt->setString(13, sfn);

    stmt->executeUpdate();
    
    transaction.commit();
  }
  catch (occi::SQLException& ex) {
    throw DmException(DM_QUERY_FAILED, ex.getMessage());
  }
}



void NsOracleCatalog::deleteReplica(const std::string& guid, int64_t id,
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
  Transaction transaction(this->conn_);

  occi::Statement* stmt = this->getPreparedStatement(STMT_DELETE_REPLICA);

  stmt->setNumber(1, meta.stat.st_ino);
  stmt->setString(2, sfn);
  
  stmt->executeUpdate();

  transaction.commit();
}



std::vector<FileReplica> NsOracleCatalog::getReplicas(const std::string& path) throw(DmException)
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



std::vector<FileReplica> NsOracleCatalog::getReplicas(ino_t ino) throw (DmException)
{
  FileReplica   replica;

  // MySQL statement
  occi::Statement* stmt = this->getPreparedStatement(STMT_GET_FILE_REPLICAS);
  occi::ResultSet* rs;

  // Execute query
  try {
    stmt->setNumber(1, ino);
    rs = stmt->executeQuery();
  }
  catch (occi::SQLException& ex) {
    throw DmException(DM_QUERY_FAILED, ex.getMessage());
  }

  // Bind result
  std::vector<FileReplica> replicas;

  while (rs->next()) {
    replica.replicaid = rs->getNumber(1);
    replica.fileid    = rs->getNumber(2);
    replica.status    = rs->getString(3)[0];
    strncpy(replica.unparsed_location, rs->getString(4).c_str(), sizeof(replica.unparsed_location));
    replica.location = splitUri(replica.unparsed_location);
    replicas.push_back(replica);
  }
  stmt->closeResultSet(rs);

  if (replicas.size() == 0)
    throw DmException(DM_NO_REPLICAS, "No replicas available for %ld", ino);

  return replicas;
}



FileReplica NsOracleCatalog::get(const std::string& path) throw(DmException)
{
  // Get all the replicas
  std::vector<FileReplica> replicas = this->getReplicas(path);

  // Pick a random one
  int i = rand() % replicas.size();

  // Copy
  return replicas[i];
}



NOT_IMPLEMENTED(std::string NsOracleCatalog::put(const std::string&, Uri*) throw (DmException))
NOT_IMPLEMENTED(std::string NsOracleCatalog::put(const std::string&, Uri*, const std::string&) throw (DmException))
NOT_IMPLEMENTED(void NsOracleCatalog::putStatus(const std::string&, const std::string&, Uri*) throw (DmException))
NOT_IMPLEMENTED(void NsOracleCatalog::putDone(const std::string&, const std::string&) throw (DmException))



void NsOracleCatalog::symlink(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
  std::string parentPath, symName;

  // Get the parent of the destination and file
  ExtendedStat parent = this->getParent(newPath, &parentPath, &symName);

  // Check we have write access for the parent
  if (checkPermissions(this->user_, this->group_, this->groups_, parent.acl,
                      parent.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Need write access for " + parentPath);

  // Transaction
  Transaction transaction(this->conn_);

  // Create the file entry
  ExtendedStat linkMeta = this->newFile(parent,
                                        symName, 0777 | S_IFLNK,
                                        1, 0, 0, '-',
                                        "", "", "");
  // Create the symlink entry
  occi::Statement* stmt = this->getPreparedStatement(STMT_INSERT_SYMLINK);

  stmt->setNumber(1, linkMeta.stat.st_ino);
  stmt->setString(2, oldPath);
  
  stmt->executeUpdate();

  // Done
  transaction.commit();
}



void NsOracleCatalog::unlink(const std::string& path) throw (DmException)
{
  std::string  parentPath, name;

  // Get the parent
  ExtendedStat parent = this->getParent(path, &parentPath, &name);

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
  occi::Statement* delSymlink = this->getPreparedStatement(STMT_DELETE_SYMLINK);
  delSymlink->setNumber(1, file.stat.st_ino);
  delSymlink->executeUpdate();

  // Remove associated comments
  occi::Statement* delComment = this->getPreparedStatement(STMT_DELETE_COMMENT);
  delComment->setNumber(1, file.stat.st_ino);
  delComment->executeUpdate();

  // Remove file itself
  occi::Statement* delFile = this->getPreparedStatement(STMT_DELETE_FILE);
  delFile->setNumber(1, file.stat.st_ino);
  delFile->executeUpdate();

  // And decrement nlink
  occi::Statement* nlink = this->getPreparedStatement(STMT_UPDATE_NLINK);
  parent.stat.st_nlink--;
  nlink->setNumber(1, parent.stat.st_nlink);
  nlink->setNumber(2, time(NULL));
  nlink->setNumber(3, time(NULL));
  nlink->setNumber(4, parent.stat.st_ino);
  nlink->executeUpdate();

  // Done!
  transaction.commit();
}



std::vector<ExtendedReplica> NsOracleCatalog::getExReplicas(const std::string& path) throw(DmException)
{
  ExtendedStat    meta;
  ExtendedReplica replica;

  // Need to grab the file first
  meta = this->extendedStat(path, true);

  // The file exists, plus we have permissions to go there. Check we can read
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN,
                      "Not enough permissions to read " + path);

  // Statement
  occi::Statement* stmt = this->getPreparedStatement(STMT_GET_FILE_REPLICAS_EXTENDED);

  stmt->setNumber(1, meta.stat.st_ino);
  occi::ResultSet* rs = stmt->executeQuery();

  // Fetch
  std::vector<ExtendedReplica> replicas;

  while (rs->next()) {
    replica.replica.replicaid = rs->getNumber(1);
    replica.replica.fileid    = rs->getNumber(2);
    replica.replica.status    = rs->getString(3)[0];
    strncpy(replica.replica.unparsed_location, rs->getString(4).c_str(), sizeof(replica.replica.unparsed_location));
    strncpy(replica.pool, rs->getString(5).c_str(), sizeof(replica.pool));
    strncpy(replica.host, rs->getString(6).c_str(), sizeof(replica.host));
    strncpy(replica.fs, rs->getString(7).c_str(), sizeof(replica.fs));

    replica.replica.location = splitUri(replica.replica.unparsed_location);

    replicas.push_back(replica);
  }

  stmt->closeResultSet(rs);

  if (replicas.size() == 0)
    throw DmException(DM_NO_REPLICAS, "No replicas available for " + path);

  return replicas;
}



void NsOracleCatalog::create(const std::string& path, mode_t mode) throw (DmException)
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
    occi::Statement* stmt = this->getPreparedStatement(STMT_TRUNCATE_FILE);
    stmt->setNumber(1, time(NULL));
    stmt->setNumber(2, file.stat.st_ino);
    stmt->executeUpdate();
  }
  
  transaction.commit();
}



mode_t NsOracleCatalog::umask(mode_t mask) throw ()
{
  mode_t prev = this->umask_;
  this->umask_ = mask & 0777;
  return prev;
}



void NsOracleCatalog::changeMode(const std::string& path, mode_t mode) throw (DmException)
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

  Transaction transaction(this->conn_);

  occi::Statement* stmt = this->getPreparedStatement(STMT_UPDATE_MODE);
  stmt->setNumber(1, mode);
  stmt->setNumber(2, time(NULL));
  stmt->setNumber(3, meta.stat.st_ino);
  stmt->executeUpdate();

  transaction.commit();
}



void NsOracleCatalog::changeOwner(ExtendedStat& meta, uid_t newUid, gid_t newGid)
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
  Transaction transaction(this->conn_);

  occi::Statement* chownStmt = this->getPreparedStatement(STMT_CHANGE_OWNER);

  chownStmt->setNumber(1, newUid);
  chownStmt->setNumber(2, newGid);
  chownStmt->setNumber(3, time(NULL));
  chownStmt->setNumber(4, meta.stat.st_ino);

  chownStmt->executeUpdate();

  transaction.commit();
}



void NsOracleCatalog::changeOwner(const std::string& path, uid_t newUid, gid_t newGid)
  throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path);
  this->changeOwner(meta, newUid, newGid);
}



void NsOracleCatalog::linkChangeOwner(const std::string& path, uid_t newUid, gid_t newGid)
  throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path, false);
  this->changeOwner(meta, newUid, newGid);
}



void NsOracleCatalog::utime(const std::string& path, const struct utimbuf* buf) throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path);

  // The user is the owner OR buf is NULL and has write permissions
  if (this->user_.uid != meta.stat.st_uid &&
      checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to modify the time of " + path);

  // Touch
  this->utime(meta.stat.st_ino, buf);
}



void NsOracleCatalog::utime(ino_t inode, const struct utimbuf* buf) throw (DmException)
{
  // If NULL, point to ours!
  struct utimbuf internal;
  if (buf == 0x00) {
    buf = &internal;
    internal.actime  = time(NULL);
    internal.modtime = time(NULL);
  }

  // Change
  occi::Statement* stmt = this->getPreparedStatement(STMT_UTIME);
  stmt->setNumber(1, buf->actime);
  stmt->setNumber(2, buf->modtime);
  stmt->setNumber(3, time(NULL));
  stmt->setNumber(4, inode);

  stmt->executeUpdate();
}



std::string NsOracleCatalog::getComment(const std::string& path) throw(DmException)
{
  // Get the file and check we can read
  ExtendedStat meta = this->extendedStat(path);

  if (checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to read " + path);

  // Query
  occi::Statement* stmt = this->getPreparedStatement(STMT_GET_COMMENT);

  stmt->setNumber(1, meta.stat.st_ino);
  occi::ResultSet* rs = stmt->executeQuery();

  if (!rs->next())
    throw DmException(DM_NO_COMMENT, "There is no comment for " + path);

  std::string comment = rs->getString(1);
  
  stmt->closeResultSet(rs);

  // Done here!
  return comment;
}



void NsOracleCatalog::setComment(const std::string& path, const std::string& comment) throw (DmException)
{
  // Get the file and check we can write
  ExtendedStat meta = this->extendedStat(path);

  if (checkPermissions(this->user_, this->group_, this->groups_, meta.acl,
                       meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to write " + path);

  // Query
  Transaction transaction(this->conn_);

  occi::Statement* stmt = this->getPreparedStatement(STMT_SET_COMMENT);

  stmt->setString(1, comment);
  stmt->setNumber(2, meta.stat.st_ino);

  if (stmt->executeUpdate() == 0) {
    // No update! Try inserting
    occi::Statement* stmti = this->getPreparedStatement(STMT_INSERT_COMMENT);

    stmti->setNumber(1, meta.stat.st_ino);
    stmti->setString(2, comment);

    stmti->executeUpdate();
  }

  transaction.commit();
}



void NsOracleCatalog::setGuid(const std::string& path, const std::string& guid) throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path);

  if (checkPermissions(this->user_, this->group_, this->groups_, meta.acl,
                       meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to write " + path);

  // Query
  Transaction transaction(this->conn_);

  occi::Statement* stmt = this->getPreparedStatement(STMT_SET_GUID);

  stmt->setString(1, guid);
  stmt->setNumber(2, meta.stat.st_ino);

  stmt->executeUpdate();

  transaction.commit();
}



void NsOracleCatalog::makeDir(const std::string& path, mode_t mode) throw (DmException)
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



void NsOracleCatalog::removeDir(const std::string& path) throw (DmException)
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

  // Remove directory itself
  occi::Statement* delDir = this->getPreparedStatement(STMT_DELETE_FILE);
  delDir->setNumber(1, entry.stat.st_ino);
  delDir->executeUpdate();

  // And decrement nlink
  occi::Statement* nlink = this->getPreparedStatement(STMT_UPDATE_NLINK);
  parent.stat.st_nlink--;
  nlink->setNumber(1, parent.stat.st_nlink);
  nlink->setNumber(2, time(NULL));
  nlink->setNumber(3, time(NULL));
  nlink->setNumber(4, parent.stat.st_ino);
  nlink->executeUpdate();

  // Done!
  transaction.commit();
}



void NsOracleCatalog::rename(const std::string& oldPath, const std::string& newPath) throw (DmException)
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
    occi::Statement* changeNameStmt = this->getPreparedStatement(STMT_CHANGE_NAME);

    changeNameStmt->setString(1, newName);
    changeNameStmt->setNumber(2, time(NULL));
    changeNameStmt->setNumber(3, old.stat.st_ino);

    changeNameStmt->executeUpdate();
  }

  // Change the parent if needed
  if (newParent.stat.st_ino != oldParent.stat.st_ino) {
    occi::Statement* changeParentStmt = this->getPreparedStatement(STMT_CHANGE_PARENT);

    changeParentStmt->setNumber(1, newParent.stat.st_ino);
    changeParentStmt->setNumber(2, time(NULL));
    changeParentStmt->setNumber(3, old.stat.st_ino);

    changeParentStmt->executeUpdate();

    // Reduce nlinks from old
    occi::Statement* oldNlinkStmt = this->getPreparedStatement(STMT_UPDATE_NLINK);

    oldNlinkStmt->setNumber(1, --oldParent.stat.st_nlink);
    oldNlinkStmt->setNumber(2, time(NULL));
    oldNlinkStmt->setNumber(3, time(NULL));
    oldNlinkStmt->setNumber(4, oldParent.stat.st_ino);

    oldNlinkStmt->executeUpdate();

    // Increment from new
    occi::Statement* newNlinkStmt = this->getPreparedStatement(STMT_UPDATE_NLINK);

    newNlinkStmt->setNumber(1, ++newParent.stat.st_nlink);
    newNlinkStmt->setNumber(2, time(NULL));
    newNlinkStmt->setNumber(3, time(NULL));
    newNlinkStmt->setNumber(4, newParent.stat.st_ino);

    newNlinkStmt->executeUpdate();
  }

  // Done!
  transaction.commit();
}



void NsOracleCatalog::setUserId(uid_t uid, gid_t gid, const std::string& dn) throw(DmException)
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



void NsOracleCatalog::setVomsData(const std::string& vo, const std::vector<std::string>& fqans) throw (DmException)
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



void NsOracleCatalog::getIdMap(const std::string& userName, const std::vector<std::string>& groupNames,
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



UserInfo NsOracleCatalog::getUser(const std::string& userName) throw(DmException)
{
  UserInfo  user;
  occi::Statement* stmt = this->getPreparedStatement(STMT_GET_USERINFO_BY_NAME);

  stmt->setString(1, userName);
  occi::ResultSet* rs = stmt->executeQuery();

  if (!rs->next())
    throw DmException(DM_NO_SUCH_USER, "User " + userName + " not found");

  user.uid = rs->getNumber(1);
  strncpy(user.name, rs->getString(2).c_str(), sizeof(user.name));
  strncpy(user.ca, rs->getString(3).c_str(), sizeof(user.ca));
  user.banned = rs->getNumber(4);

  stmt->closeResultSet(rs);

  return user;
}



UserInfo NsOracleCatalog::getUser(uid_t uid) throw(DmException)
{
  UserInfo  user;
  occi::Statement* stmt(this->getPreparedStatement(STMT_GET_USERINFO_BY_UID));

  stmt->setNumber(1, uid);
  occi::ResultSet* rs = stmt->executeQuery();

  if (!rs->next())
    throw DmException(DM_NO_SUCH_USER, "User %ld not found", uid);

  user.uid = rs->getNumber(1);
  strncpy(user.name, rs->getString(2).c_str(), sizeof(user.name));
  strncpy(user.ca, rs->getString(3).c_str(), sizeof(user.ca));
  user.banned = rs->getNumber(4);

  stmt->closeResultSet(rs);

  return user;
}



GroupInfo NsOracleCatalog::getGroup(const std::string& groupName) throw(DmException)
{
  GroupInfo group;
  occi::Statement* stmt = this->getPreparedStatement(STMT_GET_GROUPINFO_BY_NAME);

  stmt->setString(1, groupName);
  occi::ResultSet* rs = stmt->executeQuery();

  if (!rs->next())
    throw DmException(DM_NO_SUCH_GROUP, "Group " + groupName + " not found");

  group.gid = rs->getNumber(1);
  strncpy(group.name, rs->getString(2).c_str(), sizeof(group.name));
  group.banned = rs->getNumber(3);

  stmt->closeResultSet(rs);

  return group;
}



GroupInfo NsOracleCatalog::getGroup(gid_t gid) throw(DmException)
{
  GroupInfo group;
  occi::Statement* stmt = this->getPreparedStatement(STMT_GET_GROUPINFO_BY_GID);

  stmt->setNumber(1, gid);
  occi::ResultSet* rs = stmt->executeQuery();

  if (!rs->next())
    throw DmException(DM_NO_SUCH_GROUP, "Group %ld not found", gid);

  group.gid = rs->getNumber(1);
  strncpy(group.name, rs->getString(2).c_str(), sizeof(group.name));
  group.banned = rs->getNumber(3);

  stmt->closeResultSet(rs);

  return group;
}



UserInfo NsOracleCatalog::newUser(const std::string& uname, const std::string& ca) throw (DmException)
{
  Transaction transaction(this->conn_);

  // Get the last uid, increment and update
  occi::Statement* uidStmt = this->getPreparedStatement(STMT_GET_UNIQ_UID_FOR_UPDATE);
  uid_t     uid;

  occi::ResultSet* rs = uidStmt->executeQuery();

  // Update the uid
  if (rs->next()) {
    uid = rs->getNumber(1);
    ++uid;

    occi::Statement* updateUidStmt = this->getPreparedStatement(STMT_UPDATE_UNIQ_UID);
    updateUidStmt->setNumber(1, uid);
    updateUidStmt->executeUpdate();
  }
  // Couldn't get, so insert it instead
  else {
    uid = 1;

    occi::Statement* insertUidStmt = this->getPreparedStatement(STMT_INSERT_UNIQ_UID);
    insertUidStmt->setNumber(1, uid);
    insertUidStmt->executeUpdate();
  }

  uidStmt->closeResultSet(rs);

  // Insert the user
  occi::Statement* userStmt = this->getPreparedStatement(STMT_INSERT_USER);

  userStmt->setNumber(1, uid);
  userStmt->setString(2, uname);
  userStmt->setString(3, ca);
  userStmt->setNumber(4, 0);

  userStmt->executeUpdate();

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



GroupInfo NsOracleCatalog::newGroup(const std::string& gname) throw (DmException)
{
  Transaction transaction(this->conn_);

  // Get the last gid, increment and update
  occi::Statement* gidStmt = this->getPreparedStatement(STMT_GET_UNIQ_GID_FOR_UPDATE);
  gid_t     gid;

  occi::ResultSet* rs = gidStmt->executeQuery();

  // Update the gid
  if (rs->next()) {
    gid = rs->getNumber(1);
    ++gid;

    occi::Statement* updateGidStmt = this->getPreparedStatement(STMT_UPDATE_UNIQ_GID);
    updateGidStmt->setNumber(1, gid);
    updateGidStmt->executeUpdate();
  }
  // Couldn't get, so insert it instead
  else {
    gid = 1;
    occi::Statement* insertGidStmt = this->getPreparedStatement(STMT_INSERT_UNIQ_GID);
    insertGidStmt->setNumber(1, gid);
    insertGidStmt->executeUpdate();
  }

  gidStmt->closeResultSet(rs);

  // Insert the group
  occi::Statement* groupStmt = this->getPreparedStatement(STMT_INSERT_GROUP);

  groupStmt->setNumber(1, gid);
  groupStmt->setString(2, gname);
  groupStmt->setNumber(3, 0);

  groupStmt->executeUpdate();

  // Commit
  transaction.commit();

  // Build and return the GroupInfo
  GroupInfo g;
  g.gid = gid;
  strncpy(g.name, gname.c_str(), sizeof(g.name));
  g.banned = 0;

  return g;
}
