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
  STMT_GET_REPLICA_BY_URL,
  STMT_GET_COMMENT,
  STMT_SET_GUID,
  STMT_SET_COMMENT,
  STMT_INSERT_COMMENT,
  STMT_INSERT_FILE,
  STMT_INSERT_SYMLINK,
  STMT_DELETE_FILE,
  STMT_DELETE_COMMENT,
  STMT_DELETE_SYMLINK,
  STMT_SELECT_NLINK_FOR_UPDATE,
  STMT_UPDATE_NLINK,
  STMT_UPDATE_PERMS,
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
  STMT_UTIME,
  STMT_UPDATE_REPLICA,
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
  "SELECT DBMS_ROWID.ROWID_BLOCK_NUMBER(rowid), fileid, nbaccesses, atime, ptime, ltime, status, f_type, poolname, host, fs, sfn\
        FROM Cns_file_replica\
        WHERE fileid = :b_fileid",
  "SELECT DBMS_ROWID.ROWID_BLOCK_NUMBER(rowid), fileid, nbaccesses, atime, ptime, ltime, status, f_type, poolname, host, fs, sfn\
        FROM Cns_file_replica\
        WHERE sfn = :b_sfn",
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
  "SELECT nlink FROM Cns_file_metadata WHERE fileid = :b_fileid FOR UPDATE",
  "UPDATE Cns_file_metadata\
        SET nlink = :b_nlink, mtime = :b_mtime, ctime = :b_ctime\
        WHERE fileid = :b_fileid",
  "UPDATE Cns_file_metadata\
        SET owner_uid = :b_uid, gid = :b_gid, filemode = :b_mode, acl = :b_acl, ctime = :b_ctime\
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
        SET atime = :b_atime, mtime = :b_mtime, ctime = :b_ctime\
        WHERE fileid = :b_fileid",
  "UPDATE Cns_file_replica\
        SET atime = :b_atime, ltime = :b_ltime, nbaccesses = :b_nacc, status = :b_status, f_type = :b_type\
        WHERE sfn = :b_sfn"
};



occi::Statement* NsOracleCatalog::getPreparedStatement(unsigned stId)
{
  occi::Statement* stmt;

  try {
    stmt = this->conn_->createStatement(statements[stId]);
    // Make sure autocommit is always disabled
    stmt->setAutoCommit(false);
  }
  catch (occi::SQLException& ex) {
    throw DmException(DM_QUERY_FAILED, std::string("Prepare: ") + ex.getMessage());
  }
  return stmt;
}



NsOracleCatalog::NsOracleCatalog(oracle::occi::ConnectionPool* pool,
                                 oracle::occi::Connection* conn,
                                 unsigned int symLimit) throw (DmException):
  pool_(pool), conn_(conn), cwd_(0), umask_(022), symLinkLimit_(symLimit)
{
  // Nothing
}



NsOracleCatalog::~NsOracleCatalog() throw (DmException)
{
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



void NsOracleCatalog::setSecurityCredentials(const SecurityCredentials& cred) throw (DmException)
{
  UserInfo user;
  std::vector<GroupInfo> groups;

  this->getIdMap(cred.getClientName(), cred.getFqans(), &user, &groups);
  this->secCtx_ = SecurityContext(cred, user, groups);
}



const SecurityContext& NsOracleCatalog::getSecurityContext() throw (DmException)
{
  return this->secCtx_;
}



void NsOracleCatalog::setSecurityContext(const SecurityContext& ctx)
{
  this->secCtx_ = ctx;
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
      this->conn_->terminateStatement(stmt);
      throw DmException(DM_NO_SUCH_FILE, "File %ld not found", fileId);
    }
    getMetadata(rs, &meta);
  }
  catch (occi::SQLException& ex) {
    if (rs)
      stmt->closeResultSet(rs);
    this->conn_->terminateStatement(stmt);
    throw DmException(DM_QUERY_FAILED, ex.getMessage());
  }

  stmt->closeResultSet(rs);
  this->conn_->terminateStatement(stmt);
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
      this->conn_->terminateStatement(stmt);
      throw DmException(DM_NO_SUCH_FILE, "File with guid " + guid + " not found");
    }
    getMetadata(rs, &meta);
  }
  catch (occi::SQLException& ex) {
    if (rs)
      stmt->closeResultSet(rs);
    this->conn_->terminateStatement(stmt);
    throw DmException(DM_QUERY_FAILED, ex.getMessage());
  }

  stmt->closeResultSet(rs);
  this->conn_->terminateStatement(stmt);
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
      this->conn_->terminateStatement(stmt);
      throw DmException(DM_NO_SUCH_FILE, name + " not found");
    }
    getMetadata(rs, &meta);
  }
  catch (occi::SQLException& ex) {
    if (rs)
      stmt->closeResultSet(rs);
    this->conn_->terminateStatement(stmt);
    throw DmException(DM_QUERY_FAILED, ex.getMessage());
  }

  stmt->closeResultSet(rs);
  this->conn_->terminateStatement(stmt);
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
      this->conn_->terminateStatement(stmt);
      throw DmException(DM_NO_SUCH_FILE, "Link %ld not found", linkId);
    }
    link.fileId = rs->getNumber(1);
    strncpy(link.link, rs->getString(2).c_str(), sizeof(link.link));
  }
  catch (occi::SQLException& ex) {
    if (rs)
      stmt->closeResultSet(rs);
    this->conn_->terminateStatement(stmt);
    throw DmException(DM_QUERY_FAILED, ex.getMessage());
  }

  stmt->closeResultSet(rs);
  this->conn_->terminateStatement(stmt);
  return link;
}



void NsOracleCatalog::updateNlink(ino_t fileid, int diff) throw (DmException)
{
  occi::Statement* nlinkStmt = this->getPreparedStatement(STMT_SELECT_NLINK_FOR_UPDATE);
  nlinkStmt->setNumber(1, fileid);
  occi::ResultSet* nlinkRs = nlinkStmt->executeQuery();
  nlinkRs->next();
  long nlink = nlinkRs->getNumber(1);

  nlinkStmt->closeResultSet(nlinkRs);
  this->conn_->terminateStatement(nlinkStmt);

  occi::Statement* nlinkUpdateStmt = this->getPreparedStatement(STMT_UPDATE_NLINK);

  nlink += diff;
  nlinkUpdateStmt->setNumber(1, nlink);
  nlinkUpdateStmt->setNumber(2, time(NULL));
  nlinkUpdateStmt->setNumber(3, time(NULL));
  nlinkUpdateStmt->setNumber(4, fileid);

  nlinkUpdateStmt->executeUpdate();
  this->conn_->terminateStatement(nlinkUpdateStmt);
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
  if (checkPermissions(this->secCtx_, parent.acl, parent.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Need write access on the parent");

  // Check SGID
  if (parent.stat.st_mode & S_ISGID) {
    egid = parent.stat.st_gid;
    mode |= S_ISGID;
  }
  else {
    egid = this->secCtx_.getGroup().gid;
  }

  // Create the entry
  occi::Statement* fileStmt = this->getPreparedStatement(STMT_INSERT_FILE);

  fileStmt->setNumber( 1, parent.stat.st_ino);
  fileStmt->setString( 2, name);
  fileStmt->setNumber( 3, mode);
  fileStmt->setNumber( 4, nlink);
  fileStmt->setNumber( 5, this->secCtx_.getUser().uid);
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

  this->conn_->terminateStatement(fileStmt);

  // Increment the nlink
  this->updateNlink(parent.stat.st_ino, +1);

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



void NsOracleCatalog::traverseBackwards(const ExtendedStat& meta) throw (DmException)
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
  if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to read " + path);

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
  this->conn_->terminateStatement(dirp->stmt);
  delete dirp;
}



struct dirent* NsOracleCatalog::readDir(Directory* dir) throw(DmException)
{
  if (this->readDirx(dir) == 0)
    return 0x00;
  else
    return &(((NsOracleDir*)dir)->ds);
}



ExtendedStat* NsOracleCatalog::readDirx(Directory* dir) throw(DmException)
{
  NsOracleDir *dirp;

  if (dir == 0x00)
    throw DmException(DM_NULL_POINTER, "Tried to read a null dir");

  dirp = (NsOracleDir*)dir;

  if (dirp->rs->next()) {
    getMetadata(dirp->rs, &dirp->current);
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



void NsOracleCatalog::addReplica(const std::string& guid, int64_t id,
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
    stmt->setString(11, host);
    stmt->setString(12, fileSystem);
    stmt->setString(13, sfn);

    stmt->executeUpdate();

    this->conn_->terminateStatement(stmt);
    
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
  if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to remove the replica");

  // Remove
  Transaction transaction(this->conn_);

  occi::Statement* stmt = this->getPreparedStatement(STMT_DELETE_REPLICA);

  stmt->setNumber(1, meta.stat.st_ino);
  stmt->setString(2, sfn);
  
  stmt->executeUpdate();
  this->conn_->terminateStatement(stmt);

  transaction.commit();
}



std::vector<FileReplica> NsOracleCatalog::getReplicas(const std::string& path) throw(DmException)
{
  ExtendedStat  meta;

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
    this->conn_->terminateStatement(stmt);
    throw DmException(DM_QUERY_FAILED, ex.getMessage());
  }

  // Bind result
  std::vector<FileReplica> replicas;

  while (rs->next()) {
    memset(&replica, 0x00, sizeof(replica));
    replica.replicaid  = rs->getNumber(1);
    replica.fileid     = rs->getNumber(2);
    replica.nbaccesses = rs->getNumber(3);
    replica.atime      = rs->getNumber(4);
    replica.ptime      = rs->getNumber(5);
    replica.ltime      = rs->getNumber(6);
    replica.status     = rs->getString(7)[0];
    replica.type       = rs->getString(8)[0];
    strncpy(replica.pool,       rs->getString( 9).c_str(), sizeof(replica.pool));
    strncpy(replica.server,     rs->getString(10).c_str(), sizeof(replica.server));
    strncpy(replica.filesystem, rs->getString(11).c_str(), sizeof(replica.filesystem));
    strncpy(replica.url,        rs->getString(12).c_str(), sizeof(replica.url));

    replicas.push_back(replica);
  }
  stmt->closeResultSet(rs);
  this->conn_->terminateStatement(stmt);

  if (replicas.size() == 0)
    throw DmException(DM_NO_REPLICAS, "No replicas available for %ld", ino);

  return replicas;
}



Uri NsOracleCatalog::get(const std::string& path) throw(DmException)
{
  // Get all the replicas
  std::vector<FileReplica> replicas = this->getReplicas(path);

  // Pick a random one
  int i = rand() % replicas.size();

  // Copy
  return dmlite::splitUri(replicas[i].url);
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
  if (checkPermissions(this->secCtx_, parent.acl, parent.stat, S_IWRITE) != 0)
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
  this->conn_->terminateStatement(stmt);
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
    if (this->secCtx_.getUser().uid != file.stat.st_uid &&
        this->secCtx_.getUser().uid != parent.stat.st_uid &&
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
  occi::Statement* delSymlink = this->getPreparedStatement(STMT_DELETE_SYMLINK);
  delSymlink->setNumber(1, file.stat.st_ino);
  delSymlink->executeUpdate();
  this->conn_->terminateStatement(delSymlink);

  // Remove associated comments
  occi::Statement* delComment = this->getPreparedStatement(STMT_DELETE_COMMENT);
  delComment->setNumber(1, file.stat.st_ino);
  delComment->executeUpdate();
  this->conn_->terminateStatement(delComment);

  // Remove file itself
  occi::Statement* delFile = this->getPreparedStatement(STMT_DELETE_FILE);
  delFile->setNumber(1, file.stat.st_ino);
  delFile->executeUpdate();
  this->conn_->terminateStatement(delFile);

  // And decrement nlink
  this->updateNlink(parent.stat.st_ino, -1);

  // Done!
  transaction.commit();
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
    mode_t newMode = ((mode & ~S_IFMT) & ~this->umask_);

    // Generate inherited ACL's if there are defaults
    std::string aclStr;
    if (strchr (parent.acl, ACL_DEFAULT | ACL_USER_OBJ  | '@') != NULL) {
      aclStr = serializeAcl(inheritAcl(deserializeAcl(parent.acl),
                                       this->secCtx_.getUser().uid,
                                       this->secCtx_.getGroup(0).gid,
                                       &newMode, mode));
    }
    
    this->newFile(parent, name, (mode & ~S_IFMT) & ~this->umask_,
                  1, 0, 0, '-',
                  std::string(), std::string(),
                  aclStr);
  }
  // Truncate
  else if (code == DM_NO_REPLICAS) {
    occi::Statement* stmt = this->getPreparedStatement(STMT_TRUNCATE_FILE);
    stmt->setNumber(1, time(NULL));
    stmt->setNumber(2, file.stat.st_ino);
    stmt->executeUpdate();
    this->conn_->terminateStatement(stmt);
  }
  
  transaction.commit();
}



mode_t NsOracleCatalog::umask(mode_t mask) throw ()
{
  mode_t prev  = this->umask_;
  this->umask_ = mask & 0777;
  return prev;
}



void NsOracleCatalog::changeMode(const std::string& path, mode_t mode) throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path);

  // User has to be the owner, or root
  if (this->secCtx_.getUser().uid != meta.stat.st_uid &&
      this->secCtx_.getUser().uid != 0)
    throw DmException(DM_FORBIDDEN, "Only the owner can change the mode of " + path);

  // Clean up unwanted bits
  mode &= ~S_IFMT;
  if (!S_ISDIR(meta.stat.st_mode) && this->secCtx_.getUser().uid != 0)
    mode &= ~S_ISVTX;
  if (this->secCtx_.getUser().uid != 0 &&
      !this->secCtx_.hasGroup(meta.stat.st_gid))
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

  // Update DB.
  Transaction transaction(this->conn_);

  occi::Statement* stmt = this->getPreparedStatement(STMT_UPDATE_PERMS);
  stmt->setNumber(1, meta.stat.st_uid);
  stmt->setNumber(2, meta.stat.st_gid);
  stmt->setNumber(3, mode);
  stmt->setString(4, aclStr);
  stmt->setNumber(5, time(NULL));
  stmt->setNumber(6, meta.stat.st_ino);
  stmt->executeUpdate();
  this->conn_->terminateStatement(stmt);
  
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
  if (this->secCtx_.getUser().uid != 0) {
    // Only root can change the owner
    if (meta.stat.st_uid != newUid)
      throw DmException(DM_BAD_OPERATION, "Only root can change the owner");
    // If the group is changing...
    if (meta.stat.st_gid != newGid) {
      // The user has to be the owner
      if (meta.stat.st_uid != this->secCtx_.getUser().uid)
        throw DmException(DM_BAD_OPERATION, "Only root or the owner can change the group");
      // AND it has to belong to that group
      if (!this->secCtx_.hasGroup(newGid))
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
  Transaction transaction(this->conn_);

  occi::Statement* chownStmt = this->getPreparedStatement(STMT_UPDATE_PERMS);

  chownStmt->setNumber(1, newUid);
  chownStmt->setNumber(2, newGid);
  chownStmt->setNumber(3, meta.stat.st_mode);
  chownStmt->setString(4, aclStr);
  chownStmt->setNumber(5, time(NULL));
  chownStmt->setNumber(6, meta.stat.st_ino);

  chownStmt->executeUpdate();
  this->conn_->terminateStatement(chownStmt);

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



void NsOracleCatalog::setAcl(const std::string& path, const std::vector<Acl>& acls) throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path);

  // Check we can change it
  if (this->secCtx_.getUser().uid != meta.stat.st_uid &&
      this->secCtx_.getUser().uid != 0)
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
        meta.stat.st_mode = meta.stat.st_mode & 0177077 | (aclsCopy[i].perm << 6);
        break;
      case ACL_GROUP_OBJ:
        meta.stat.st_mode = meta.stat.st_mode & 0177707 | (aclsCopy[i].perm << 3);
        break;
      case ACL_MASK:
        meta.stat.st_mode = (meta.stat.st_mode & ~070) | (meta.stat.st_mode & aclsCopy[i].perm << 3);
        break;
      case ACL_OTHER:
        meta.stat.st_mode = meta.stat.st_mode & 0177770 | (aclsCopy[i].perm);
        break;
    }
  }

  // If only 3 entries, no extended ACL
  std::string aclStr;
  if (aclsCopy.size() > 3)
    aclStr = dmlite::serializeAcl(aclsCopy);

  // Update the DB
  Transaction transaction(this->conn_);

  occi::Statement* stmt = this->getPreparedStatement(STMT_UPDATE_PERMS);

  stmt->setNumber(1, meta.stat.st_uid);
  stmt->setNumber(2, meta.stat.st_gid);
  stmt->setNumber(3, meta.stat.st_mode);
  stmt->setString(4, aclStr);
  stmt->setNumber(5, time(NULL));
  stmt->setNumber(6, meta.stat.st_ino);

  stmt->executeUpdate();

  this->conn_->terminateStatement(stmt);
  transaction.commit();
}



void NsOracleCatalog::utime(const std::string& path, const struct utimbuf* buf) throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path);

  // The user is the owner OR buf is NULL and has write permissions
  if (this->secCtx_.getUser().uid != meta.stat.st_uid &&
      checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IWRITE) != 0)
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
  this->conn_->terminateStatement(stmt);
}



std::string NsOracleCatalog::getComment(const std::string& path) throw(DmException)
{
  // Get the file and check we can read
  ExtendedStat meta = this->extendedStat(path);

  if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to read " + path);

  // Query
  occi::Statement* stmt = this->getPreparedStatement(STMT_GET_COMMENT);

  stmt->setNumber(1, meta.stat.st_ino);
  occi::ResultSet* rs = stmt->executeQuery();

  if (!rs->next())
    throw DmException(DM_NO_COMMENT, "There is no comment for " + path);

  std::string comment = rs->getString(1);
  
  stmt->closeResultSet(rs);
  this->conn_->terminateStatement(stmt);

  // Done here!
  return comment;
}



void NsOracleCatalog::setComment(const std::string& path, const std::string& comment) throw (DmException)
{
  // Get the file and check we can write
  ExtendedStat meta = this->extendedStat(path);

  if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IWRITE) != 0)
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

  this->conn_->terminateStatement(stmt);
  transaction.commit();
}



void NsOracleCatalog::setGuid(const std::string& path, const std::string& guid) throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path);

  if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to write " + path);

  // Query
  Transaction transaction(this->conn_);

  occi::Statement* stmt = this->getPreparedStatement(STMT_SET_GUID);

  stmt->setString(1, guid);
  stmt->setNumber(2, meta.stat.st_ino);

  stmt->executeUpdate();

  this->conn_->terminateStatement(stmt);
  transaction.commit();
}



void NsOracleCatalog::makeDir(const std::string& path, mode_t mode) throw (DmException)
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
                                     this->secCtx_.getUser().uid,
                                     this->secCtx_.getGroup(0).gid,
                                     &newMode, mode));
  }

  // Create the file entry
  Transaction transaction(this->conn_);

  this->newFile(parent, name, newMode,
                0, 0, parent.type, '-', "", "", aclStr);
  
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
    if (this->secCtx_.getUser().uid != entry.stat.st_uid &&
        this->secCtx_.getUser().uid != parent.stat.st_uid &&
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

  // Remove directory itself
  occi::Statement* delDir = this->getPreparedStatement(STMT_DELETE_FILE);
  delDir->setNumber(1, entry.stat.st_ino);
  delDir->executeUpdate();
  this->conn_->terminateStatement(delDir);

  // And decrement nlink
  this->updateNlink(parent.stat.st_ino, -1);

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
      this->secCtx_.getUser().uid != oldParent.stat.st_uid &&
      this->secCtx_.getUser().uid != old.stat.st_uid &&
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
    occi::Statement* changeNameStmt = this->getPreparedStatement(STMT_CHANGE_NAME);

    changeNameStmt->setString(1, newName);
    changeNameStmt->setNumber(2, time(NULL));
    changeNameStmt->setNumber(3, old.stat.st_ino);

    changeNameStmt->executeUpdate();
    this->conn_->terminateStatement(changeNameStmt);
  }

  // Change the parent if needed
  if (newParent.stat.st_ino != oldParent.stat.st_ino) {
    occi::Statement* changeParentStmt = this->getPreparedStatement(STMT_CHANGE_PARENT);

    changeParentStmt->setNumber(1, newParent.stat.st_ino);
    changeParentStmt->setNumber(2, time(NULL));
    changeParentStmt->setNumber(3, old.stat.st_ino);

    changeParentStmt->executeUpdate();

    this->conn_->terminateStatement(changeParentStmt);

    // Reduce nlinks from old
    this->updateNlink(oldParent.stat.st_ino, -1);

    // Increment from new
    this->updateNlink(newParent.stat.st_ino, +1);
  }

  // Done!
  transaction.commit();
}



FileReplica NsOracleCatalog::replicaGet(const std::string& replica) throw (DmException)
{
  occi::Statement* stmt = this->getPreparedStatement(STMT_GET_REPLICA_BY_URL);
  stmt->setString(1, replica);

  occi::ResultSet* rs = stmt->executeQuery();

  if (!rs->next()) {
    this->conn_->terminateStatement(stmt);
    throw DmException(DM_NO_REPLICAS, "Replica " + replica + " not found");
  }

  FileReplica r;

  r.replicaid  = rs->getNumber(1);
  r.fileid     = rs->getNumber(2);
  r.nbaccesses = rs->getNumber(3);
  r.atime      = rs->getNumber(4);
  r.ptime      = rs->getNumber(5);
  r.ltime      = rs->getNumber(6);
  r.status     = rs->getString(7)[0];
  r.type       = rs->getString(8)[0];
  strncpy(r.pool,       rs->getString( 9).c_str(), sizeof(r.pool));
  strncpy(r.server,     rs->getString(10).c_str(), sizeof(r.server));
  strncpy(r.filesystem, rs->getString(11).c_str(), sizeof(r.filesystem));
  strncpy(r.url,        rs->getString(12).c_str(), sizeof(r.url));

  stmt->closeResultSet(rs);
  this->conn_->terminateStatement(stmt);
  return r;
}



void NsOracleCatalog::replicaSet(const FileReplica& rdata) throw (DmException)
{
  /* Get associated file */
  ExtendedStat meta = this->extendedStat(rdata.fileid);

  /* Check we can actually go here */
  this->traverseBackwards(meta);

  /* Check the user can modify */
  if (this->secCtx_.getUser().uid != meta.stat.st_uid &&
      checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to modify the replica");

  /* Update */
  occi::Statement* stmt = this->getPreparedStatement(STMT_UPDATE_REPLICA);
  stmt->setNumber(1, rdata.atime);
  stmt->setNumber(2, rdata.ltime);
  stmt->setNumber(3, rdata.nbaccesses);
  stmt->setString(4, std::string(&rdata.status, 1));
  stmt->setString(5, std::string(&rdata.type, 1));
  stmt->setString(6, rdata.url);

  stmt->executeUpdate();
  this->conn_->terminateStatement(stmt);
}



void NsOracleCatalog::replicaSetAccessTime(const std::string& replica) throw (DmException)
{
  FileReplica rdata = this->replicaGet(replica);
  rdata.atime = time(NULL);
  this->replicaSet(rdata);
}



void NsOracleCatalog::replicaSetLifeTime(const std::string& replica, time_t ltime) throw (DmException)
{
  FileReplica rdata = this->replicaGet(replica);
  rdata.ltime = ltime;
  this->replicaSet(rdata);
}



void NsOracleCatalog::replicaSetStatus(const std::string& replica, char status) throw (DmException)
{
  FileReplica rdata = this->replicaGet(replica);
  rdata.status = status;
  this->replicaSet(rdata);
}



void NsOracleCatalog::replicaSetType(const std::string& replica, char type) throw (DmException)
{
  FileReplica rdata = this->replicaGet(replica);
  rdata.type = type;
  this->replicaSet(rdata);
}



void NsOracleCatalog::getIdMap(const std::string& userName, const std::vector<std::string>& groupNames,
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
  this->conn_->terminateStatement(stmt);

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
  this->conn_->terminateStatement(stmt);

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
  this->conn_->terminateStatement(stmt);

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
  this->conn_->terminateStatement(stmt);

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
    this->conn_->terminateStatement(updateUidStmt);
  }
  // Couldn't get, so insert it instead
  else {
    uid = 1;

    occi::Statement* insertUidStmt = this->getPreparedStatement(STMT_INSERT_UNIQ_UID);
    insertUidStmt->setNumber(1, uid);
    insertUidStmt->executeUpdate();
    this->conn_->terminateStatement(insertUidStmt);
  }

  uidStmt->closeResultSet(rs);
  this->conn_->terminateStatement(uidStmt);

  // Insert the user
  occi::Statement* userStmt = this->getPreparedStatement(STMT_INSERT_USER);

  userStmt->setNumber(1, uid);
  userStmt->setString(2, uname);
  userStmt->setString(3, ca);
  userStmt->setNumber(4, 0);

  userStmt->executeUpdate();

  this->conn_->terminateStatement(userStmt);

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

    this->conn_->terminateStatement(updateGidStmt);
  }
  // Couldn't get, so insert it instead
  else {
    gid = 1;
    occi::Statement* insertGidStmt = this->getPreparedStatement(STMT_INSERT_UNIQ_GID);
    insertGidStmt->setNumber(1, gid);
    insertGidStmt->executeUpdate();

    this->conn_->terminateStatement(insertGidStmt);
  }

  gidStmt->closeResultSet(rs);
  this->conn_->terminateStatement(gidStmt);

  // Insert the group
  occi::Statement* groupStmt = this->getPreparedStatement(STMT_INSERT_GROUP);

  groupStmt->setNumber(1, gid);
  groupStmt->setString(2, gname);
  groupStmt->setNumber(3, 0);

  groupStmt->executeUpdate();

  this->conn_->terminateStatement(groupStmt);

  // Commit
  transaction.commit();

  // Build and return the GroupInfo
  GroupInfo g;
  g.gid = gid;
  strncpy(g.name, gname.c_str(), sizeof(g.name));
  g.banned = 0;

  return g;
}
