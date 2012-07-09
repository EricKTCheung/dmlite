/// @file    plugins/oracle/NsOracle.cpp
/// @brief   Oracle NS Implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include "NsOracle.h"
#include <assert.h>
#include <cstring>
#include <cstdlib>
#include <dmlite/cpp/utils/dm_urls.h>
#include "Queries.h"

using namespace dmlite;
using namespace oracle;


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



INodeOracle::INodeOracle(oracle::occi::ConnectionPool* pool,
                         oracle::occi::Connection* conn) throw (DmException):
  pool_(pool), conn_(conn), transactionLevel_(0)
{
  // Nothing
}



INodeOracle::~INodeOracle() throw (DmException)
{
  this->pool_->terminateConnection(this->conn_);
}



std::string INodeOracle::getImplId() throw ()
{
  return std::string("INodeOracle");
}



void INodeOracle::setStackInstance(StackInstance* si) throw (DmException)
{
  // Nothing
}



void INodeOracle::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  // Nothing
}



void INodeOracle::begin() throw (DmException)
{
  this->transactionLevel_++;
}



void INodeOracle::commit() throw (DmException)
{
  if (this->transactionLevel_ == 0)
    throw DmException(DM_INTERNAL_ERROR,
                      "INodeOracle::commit Inconsistent state (Maybe there is a\
 commit without a begin, or a badly handled error sequence.)");
  
  if (--this->transactionLevel_ == 0) {
    try {
      this->conn_->commit();
    }
    catch (occi::SQLException& ex) {
      throw DmException(DM_QUERY_FAILED, ex.getMessage());
    }
  }
}



void INodeOracle::rollback() throw (DmException)
{
  this->transactionLevel_ = 0;
  try {
    this->conn_->rollback();
  }
  catch (occi::SQLException& ex) {
    throw DmException(DM_QUERY_FAILED, ex.getMessage());
  }
}



ExtendedStat INodeOracle::create(ino_t parent, const std::string& name,
                                 uid_t uid, gid_t gid, mode_t mode, size_t size,
                                 short type, char status,
                                 const std::string& csumtype,
                                 const std::string& csumvalue,
                                 const std::string& acl) throw (DmException)
{
  // Create the entry
  occi::Statement* fileStmt = getPreparedStatement(this->conn_, STMT_INSERT_FILE);
  
  this->begin();
  
  try {
    unsigned nlink = S_ISDIR(mode) ? 0 : 1;

    fileStmt->setNumber( 1, parent);
    fileStmt->setString( 2, name);
    fileStmt->setNumber( 3, mode);
    fileStmt->setNumber( 4, nlink);
    fileStmt->setNumber( 5, uid);
    fileStmt->setNumber( 6, gid);
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
    this->updateNlink(parent, +1);

    // Return back
    return this->extendedStat(parent, name);
  }
  catch (occi::SQLException& ex) {
    this->rollback();
    throw DmException(DM_INTERNAL_ERROR, ex.getMessage());
  }
  
  this->commit();
}



void INodeOracle::symlink(ino_t inode, const std::string& link) throw (DmException)
{
  occi::Statement* stmt = getPreparedStatement(this->conn_, STMT_INSERT_SYMLINK);

  this->begin();
  
  try {
    stmt->setNumber(1, inode);
    stmt->setString(2, link);

    stmt->executeUpdate();

    this->conn_->terminateStatement(stmt);
  }
  catch (occi::SQLException& ex) {
    this->rollback();
    throw DmException(DM_INTERNAL_ERROR, ex.getMessage());
  }
}



void INodeOracle::unlink(ino_t inode) throw (DmException)
{
  ExtendedStat file = this->extendedStat(inode);
  
  if (S_ISDIR(file.stat.st_mode) && file.stat.st_nlink > 0)
    throw DmException(DM_IS_DIRECTORY, "Inode %l is a directory and it is not empty", inode);

  
  this->begin();
  try {
    // Remove associated symlink
    occi::Statement* delSymlink = getPreparedStatement(this->conn_, STMT_DELETE_SYMLINK);
    delSymlink->setNumber(1, inode);
    delSymlink->executeUpdate();
    this->conn_->terminateStatement(delSymlink);

    // Remove associated comments
    occi::Statement* delComment = getPreparedStatement(this->conn_, STMT_DELETE_COMMENT);
    delComment->setNumber(1, inode);
    delComment->executeUpdate();
    this->conn_->terminateStatement(delComment);

    // Remove file itself
    occi::Statement* delFile = getPreparedStatement(this->conn_, STMT_DELETE_FILE);
    delFile->setNumber(1, inode);
    delFile->executeUpdate();
    this->conn_->terminateStatement(delFile);

    // And decrement nlink
    this->updateNlink(file.parent, -1);
  }
  catch (DmException& e) {
    this->rollback();
    throw;
  }
  catch (occi::SQLException& e) {
    this->rollback();
    throw DmException(DM_INTERNAL_ERROR, e.getMessage());
  }

  // Done!
  this->commit();
}


  
void INodeOracle::move(ino_t inode, ino_t dest) throw (DmException)
{
  occi::Statement* changeParentStmt = getPreparedStatement(this->conn_, STMT_CHANGE_PARENT);

  ExtendedStat file = this->extendedStat(inode);
  
  this->begin();
  try {
    changeParentStmt->setNumber(1, dest);
    changeParentStmt->setNumber(2, time(NULL));
    changeParentStmt->setNumber(3, inode);

    changeParentStmt->executeUpdate();

    this->conn_->terminateStatement(changeParentStmt);

    // Reduce nlinks from old
    this->updateNlink(file.parent, -1);

    // Increment from new
    this->updateNlink(dest, +1);
  }
  catch (occi::SQLException& e) {
    this->rollback();
    throw DmException(DM_INTERNAL_ERROR, e.getMessage());
  }
  
  this->commit();
}



void INodeOracle::rename(ino_t inode, const std::string& name) throw (DmException)
{
  occi::Statement* changeNameStmt = getPreparedStatement(this->conn_, STMT_CHANGE_NAME);

  this->begin();
  try {
    changeNameStmt->setString(1, name);
    changeNameStmt->setNumber(2, time(NULL));
    changeNameStmt->setNumber(3, inode);

    changeNameStmt->executeUpdate();
    this->conn_->terminateStatement(changeNameStmt);
  }
  catch (occi::SQLException& e) {
    this->rollback();
    throw DmException(DM_INTERNAL_ERROR, e.getMessage());
  }
  
  this->commit();
}



ExtendedStat INodeOracle::extendedStat(ino_t inode) throw (DmException)
{
  ExtendedStat     meta;
  occi::ResultSet* rs   = 0x00;
  occi::Statement* stmt = getPreparedStatement(this->conn_, STMT_GET_FILE_BY_ID);


  memset(&meta, 0x00, sizeof(ExtendedStat));

  try {
    stmt->setNumber(1, inode);
    rs = stmt->executeQuery();
    if (!rs->next()) {
      stmt->closeResultSet(rs);
      this->conn_->terminateStatement(stmt);
      throw DmException(DM_NO_SUCH_FILE, "File %ld not found", inode);
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



ExtendedStat INodeOracle::extendedStat(ino_t parent, const std::string& name) throw (DmException)
{
  ExtendedStat meta;
  occi::ResultSet* rs   = 0x00;
  occi::Statement* stmt = getPreparedStatement(this->conn_, STMT_GET_FILE_BY_NAME);

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



ExtendedStat INodeOracle::extendedStat(const std::string& guid) throw (DmException)
{
  ExtendedStat     meta;
  occi::ResultSet* rs   = 0x00;
  occi::Statement* stmt = getPreparedStatement(this->conn_, STMT_GET_FILE_BY_GUID);

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



SymLink INodeOracle::readLink(ino_t inode) throw (DmException)
{
  SymLink link;
  occi::ResultSet* rs   = 0x00;
  occi::Statement* stmt = getPreparedStatement(this->conn_, STMT_GET_SYMLINK);

  memset(&link, 0x00, sizeof(SymLink));

  try {
    stmt->setNumber(1, inode);
    rs = stmt->executeQuery();
    if (!rs->next()) {
      stmt->closeResultSet(rs);
      this->conn_->terminateStatement(stmt);
      throw DmException(DM_NO_SUCH_FILE, "Link %ld not found", inode);
    }
    link.inode = rs->getNumber(1);
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



void INodeOracle::addReplica(ino_t inode, const std::string& server,
                  const std::string& sfn, char status, char fileType,
                  const std::string& poolName, const std::string& fileSystem) throw (DmException)
{
  std::string  host;

  // If server is empty, parse the surl
  if (server.empty()) {
    Url u = splitUrl(sfn);
    host = u.host;
    if (host.empty())
      throw DmException(DM_INVALID_VALUE, "Empty server specified, and SFN does not include it: " + sfn);
  }
  else {
    host = server;
  }

  // Add it
  occi::Statement* stmt = getPreparedStatement(this->conn_, STMT_ADD_REPLICA);
  
  this->begin();
  try {

    stmt->setNumber( 1, inode);
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
    
    this->commit();
  }
  catch (occi::SQLException& ex) {
    this->rollback();
    throw DmException(DM_QUERY_FAILED, ex.getMessage());
  }
}



void INodeOracle::deleteReplica(ino_t inode, const std::string& sfn) throw (DmException)
{
  occi::Statement* stmt = getPreparedStatement(this->conn_, STMT_DELETE_REPLICA);
  
  this->begin();
  try {
    stmt->setNumber(1, inode);
    stmt->setString(2, sfn);
  
    stmt->executeUpdate();
    this->conn_->terminateStatement(stmt);

    this->commit();
  }
  catch (occi::SQLException& e) {
    this->rollback();
    throw DmException(DM_INTERNAL_ERROR, e.getMessage());
  }
}



std::vector<FileReplica> INodeOracle::getReplicas(ino_t inode) throw (DmException)
{
  FileReplica replica;
  
  // Set to 0
  memset(&replica, 0x00, sizeof(FileReplica));

  // MySQL statement
  occi::Statement* stmt = getPreparedStatement(this->conn_, STMT_GET_FILE_REPLICAS);
  occi::ResultSet* rs;

  // Execute query
  try {
    stmt->setNumber(1, inode);
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
    strncpy(replica.rfn,        rs->getString(12).c_str(), sizeof(replica.rfn));

    replicas.push_back(replica);
  }
  stmt->closeResultSet(rs);
  this->conn_->terminateStatement(stmt);

  return replicas;
}



FileReplica INodeOracle::getReplica(const std::string& sfn) throw (DmException)
{
  occi::Statement* stmt = getPreparedStatement(this->conn_, STMT_GET_REPLICA_BY_URL);
  stmt->setString(1, sfn);

  occi::ResultSet* rs = stmt->executeQuery();

  if (!rs->next()) {
    this->conn_->terminateStatement(stmt);
    throw DmException(DM_NO_SUCH_REPLICA, "Replica " + sfn + " not found");
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
  strncpy(r.rfn,        rs->getString(12).c_str(), sizeof(r.rfn));

  stmt->closeResultSet(rs);
  this->conn_->terminateStatement(stmt);
  return r;
}



void INodeOracle::setReplica(const FileReplica& replica) throw (DmException)
{
  occi::Statement* stmt = getPreparedStatement(this->conn_, STMT_UPDATE_REPLICA);
  stmt->setNumber(1, replica.atime);
  stmt->setNumber(2, replica.ltime);
  stmt->setNumber(3, replica.nbaccesses);
  stmt->setString(4, std::string(&replica.status, 1));
  stmt->setString(5, std::string(&replica.type, 1));
  stmt->setString(6, replica.rfn);

  stmt->executeUpdate();
  this->conn_->terminateStatement(stmt);
}



void INodeOracle::utime(ino_t inode, const struct utimbuf* buf) throw (DmException)
{
  // If NULL, current time
  struct utimbuf internal;
  if (buf == 0x00) {
    buf = &internal;
    internal.actime  = time(NULL);
    internal.modtime = time(NULL);
  }

  occi::Statement* stmt = getPreparedStatement(this->conn_, STMT_UTIME);
  this->begin();
  try {
    stmt->setNumber(1, buf->actime);
    stmt->setNumber(2, buf->modtime);
    stmt->setNumber(3, time(NULL));
    stmt->setNumber(4, inode);

    stmt->executeUpdate();
    this->conn_->terminateStatement(stmt);
  }
  catch (occi::SQLException& e) {
    this->rollback();
    throw DmException(DM_INTERNAL_ERROR, e.getMessage());
  }
  
  this->commit();
}



void INodeOracle::changeMode(ino_t inode, uid_t uid, gid_t gid, mode_t mode, const std::string& acl) throw (DmException)
{
  occi::Statement* stmt = getPreparedStatement(this->conn_, STMT_UPDATE_PERMS);
  
  this->begin();
  try {
    stmt->setNumber(1, uid);
    stmt->setNumber(2, gid);
    stmt->setNumber(3, mode);
    stmt->setString(4, acl);
    stmt->setNumber(5, time(NULL));
    stmt->setNumber(6, inode);
    stmt->executeUpdate();
    this->conn_->terminateStatement(stmt);
  }
  catch (occi::SQLException& e) {
    this->rollback();
    throw DmException(DM_INTERNAL_ERROR, e.getMessage());
  }
  
  this->commit();
}



void INodeOracle::changeSize(ino_t inode, size_t size) throw (DmException)
{
  throw DmException(DM_NOT_IMPLEMENTED, "changeSize not implemented in Oracle");
}



void INodeOracle::changeChecksum(ino_t inode, const std::string& csumtype,
                                 const std::string& csumvalue) throw (DmException)
{
  throw DmException(DM_NOT_IMPLEMENTED, "changeChecksum not implemented in Oracle");
}



std::string INodeOracle::getComment(ino_t inode) throw (DmException)
{
  occi::Statement* stmt = getPreparedStatement(this->conn_, STMT_GET_COMMENT);

  stmt->setNumber(1, inode);
  occi::ResultSet* rs = stmt->executeQuery();

  if (!rs->next())
    throw DmException(DM_NO_COMMENT, "There is no comment for " + inode);

  std::string comment = rs->getString(1);
  
  stmt->closeResultSet(rs);
  this->conn_->terminateStatement(stmt);

  // Done here!
  return comment;
}



void INodeOracle::setComment(ino_t inode, const std::string& comment) throw (DmException)
{
  occi::Statement* stmt = getPreparedStatement(this->conn_, STMT_SET_COMMENT);  
  
  this->begin();

  try {
    stmt->setString(1, comment);
    stmt->setNumber(2, inode);

    if (stmt->executeUpdate() == 0) {
      // No update! Try inserting
      occi::Statement* stmti = getPreparedStatement(this->conn_, STMT_INSERT_COMMENT);

      stmti->setNumber(1, inode);
      stmti->setString(2, comment);

      stmti->executeUpdate();
    }

    this->conn_->terminateStatement(stmt);
  }
  catch (occi::SQLException& e) {
    this->rollback();
    throw DmException(DM_INTERNAL_ERROR, e.getMessage());
  }
  
  this->commit();
}



void INodeOracle::deleteComment(ino_t inode) throw (DmException)
{
  
}



void INodeOracle::setGuid(ino_t inode, const std::string& guid) throw (DmException)
{
  occi::Statement* stmt = getPreparedStatement(this->conn_, STMT_SET_GUID);
  
  this->begin();

  try {
    stmt->setString(1, guid);
    stmt->setNumber(2, inode);

    stmt->executeUpdate();

    this->conn_->terminateStatement(stmt);
  }
  catch (occi::SQLException& e) {
    this->rollback();
    throw DmException(DM_INTERNAL_ERROR, e.getMessage());
  }
  
  this->commit();
}



IDirectory* INodeOracle::openDir(ino_t inode) throw (DmException)
{
  NsOracleDir  *dir;

  dir = new NsOracleDir();
  dir->dirId = inode;
  dir->stmt = getPreparedStatement(this->conn_, STMT_GET_LIST_FILES);

  try {
    dir->stmt->setNumber(1, dir->dirId);
    dir->rs = dir->stmt->executeQuery();
    return dir;
  }
  catch (occi::SQLException& e) {
    delete dir;
    throw DmException(DM_INTERNAL_ERROR, e.getMessage());
  }
}



void INodeOracle::closeDir(IDirectory* dir) throw (DmException)
{
  NsOracleDir *dirp;

  if (dir == 0x00)
    throw DmException(DM_NULL_POINTER, std::string("Tried to close a null dir"));

  dirp = (NsOracleDir*)dir;

  dirp->stmt->closeResultSet(dirp->rs);
  this->conn_->terminateStatement(dirp->stmt);
  delete dirp;
}



ExtendedStat* INodeOracle::readDirx(IDirectory* dir) throw (DmException)
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


    return &dirp->current;
  }
  else {
    return 0x00;
  }
}



struct dirent* INodeOracle::readDir(IDirectory* dir) throw (DmException)
{
  if (this->readDirx(dir) == 0)
    return 0x00;
  else
    return &(((NsOracleDir*)dir)->ds);
}



void INodeOracle::updateNlink(ino_t inode, int diff) throw (DmException)
{
  occi::Statement* nlinkStmt = getPreparedStatement(this->conn_, STMT_SELECT_NLINK_FOR_UPDATE);
  nlinkStmt->setNumber(1, inode);
  occi::ResultSet* nlinkRs = nlinkStmt->executeQuery();
  nlinkRs->next();
  long nlink = nlinkRs->getNumber(1);

  nlinkStmt->closeResultSet(nlinkRs);
  this->conn_->terminateStatement(nlinkStmt);

  occi::Statement* nlinkUpdateStmt = getPreparedStatement(this->conn_, STMT_UPDATE_NLINK);

  nlink += diff;
  nlinkUpdateStmt->setNumber(1, nlink);
  nlinkUpdateStmt->setNumber(2, time(NULL));
  nlinkUpdateStmt->setNumber(3, time(NULL));
  nlinkUpdateStmt->setNumber(4, inode);

  nlinkUpdateStmt->executeUpdate();
  this->conn_->terminateStatement(nlinkUpdateStmt);
}
