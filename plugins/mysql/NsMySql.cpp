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
#include <dmlite/common/Urls.h>

#include "MySqlWrapper.h"
#include "NsMySql.h"
#include "Queries.h"

#define NOT_IMPLEMENTED(p)\
p {\
  throw DmException(DM_NOT_IMPLEMENTED, #p" not implemented");\
}


using namespace dmlite;



INodeMySql::INodeMySql(PoolContainer<MYSQL*>* connPool,
                       const std::string& db) throw(DmException):
  nsDb_(db), transactionLevel_(0)
{
  this->connectionPool_ = connPool;
  this->conn_           = connPool->acquire();
}



INodeMySql::~INodeMySql() throw(DmException)
{
  this->connectionPool_->release(this->conn_);
}



std::string INodeMySql::getImplId() throw ()
{
  return std::string("NsMySqlINode");
}



void INodeMySql::setStackInstance(StackInstance* si) throw (DmException)
{
  // Nothing
}



void INodeMySql::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  // Nothing
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


void INodeMySql::begin(void) throw (DmException)
{
  if (this->transactionLevel_ == 0 && mysql_query(this->conn_, "BEGIN") != 0)
    throw DmException(DM_QUERY_FAILED, mysql_error(this->conn_));
  
  this->transactionLevel_++;
}



void INodeMySql::commit(void) throw (DmException)
{
  if (this->transactionLevel_ == 0)
    throw DmException(DM_INTERNAL_ERROR,
                      "INodeMySql::commit Inconsistent state (Maybe there is a\
 commit without a begin, or a badly handled error sequence.)");
  
  this->transactionLevel_--;
  
  if (this->transactionLevel_ == 0 && mysql_query(this->conn_, "COMMIT") != 0)
    throw DmException(DM_QUERY_FAILED, mysql_error(this->conn_));
}



void INodeMySql::rollback(void) throw (DmException)
{
  this->transactionLevel_ = 0;
  if (mysql_query(this->conn_, "ROLLBACK") != 0)
    throw DmException(DM_QUERY_FAILED, mysql_error(this->conn_));
}



ExtendedStat INodeMySql::create(ino_t parent, const std::string& name,
                                uid_t uid, gid_t gid, mode_t mode,
                                size_t size, short type, char status,
                                const std::string& csumtype, const std::string& csumvalue,
                                const std::string& acl) throw (DmException)
{
  // Get parent metadata
  ExtendedStat parentMeta = this->extendedStat(parent);
 
  // Destination must not exist!
  try {
    ExtendedStat f = this->extendedStat(parent, name);
    throw DmException(DM_EXISTS, name + " already exists");
  }
  catch (DmException e) {
    if (e.code() != DM_NO_SUCH_FILE)
      throw;
  }

  // Start transaction
  this->begin();
  
  // Fetch the new file ID
  ino_t newFileId;
  
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

  // Regular files start with 1 link. Directories 0.
  unsigned nlink = S_ISDIR(mode) ? 0 : 1;

  // Create the entry
  Statement fileStmt(this->conn_, this->nsDb_, STMT_INSERT_FILE);

  fileStmt.bindParam( 0, newFileId);
  fileStmt.bindParam( 1, parent);
  fileStmt.bindParam( 2, name);
  fileStmt.bindParam( 3, mode);
  fileStmt.bindParam( 4, nlink);
  fileStmt.bindParam( 5, uid);
  fileStmt.bindParam( 6, gid);
  fileStmt.bindParam( 7, size);
  fileStmt.bindParam( 8, type);
  fileStmt.bindParam( 9, std::string(&status, 1));
  fileStmt.bindParam(10, csumtype);
  fileStmt.bindParam(11, csumvalue);
  fileStmt.bindParam(12, acl);

  fileStmt.execute();
  
  // Increment the parent nlink
  Statement nlinkStmt(this->conn_, this->nsDb_, STMT_NLINK_FOR_UPDATE);
  nlinkStmt.bindParam(0, parent);
  nlinkStmt.execute();
  nlinkStmt.bindResult(0, &parentMeta.stat.st_nlink);
  nlinkStmt.fetch();

  Statement nlinkUpdateStmt(this->conn_, this->nsDb_, STMT_UPDATE_NLINK);

  parentMeta.stat.st_nlink++;
  nlinkUpdateStmt.bindParam(0, parentMeta.stat.st_nlink);
  nlinkUpdateStmt.bindParam(1, parentMeta.stat.st_ino);

  nlinkUpdateStmt.execute();

  // Commit and return back
  this->commit();
  
  return this->extendedStat(newFileId);
}



void INodeMySql::symlink(ino_t inode, const std::string &link) throw (DmException)
{
  Statement stmt(this->conn_, this->nsDb_, STMT_INSERT_SYMLINK);

  stmt.bindParam(0, inode);
  stmt.bindParam(1, link);

  stmt.execute();
}



void INodeMySql::unlink(ino_t inode) throw (DmException)
{
  // Get file metadata
  ExtendedStat file = this->extendedStat(inode);
  
  // Non empty directories can not be removed with this method
  if (S_ISDIR(file.stat.st_mode) && file.stat.st_nlink > 0)
    throw DmException(DM_IS_DIRECTORY, "Inode %l is a directory and it is not empty", inode);
  
  
  
  // Get the parent
  ExtendedStat parent = this->extendedStat(file.parent);  

  // All preconditions are good! Start transaction.
  this->begin();

  // Remove associated symlink
  Statement delSymlink(this->conn_, this->nsDb_, STMT_DELETE_SYMLINK);
  delSymlink.bindParam(0, inode);
  delSymlink.execute();

  // Remove associated comments
  Statement delComment(this->conn_, this->nsDb_, STMT_DELETE_COMMENT);
  delComment.bindParam(0, inode);
  delComment.execute();
  
  // Remove replicas
  Statement delReplicas(this->conn_, this->nsDb_, STMT_DELETE_ALL_REPLICAS);
  delReplicas.bindParam(0, inode);
  delReplicas.execute();
  
  // Remove file itself
  Statement delFile(this->conn_, this->nsDb_, STMT_DELETE_FILE);
  delFile.bindParam(0, inode);
  delFile.execute();

  // Decrement parent nlink
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
  this->commit();
}



void INodeMySql::move(ino_t inode, ino_t dest) throw (DmException)
{
  this->begin();
  
  // Metadata
  ExtendedStat file = this->extendedStat(inode);
  
  // Change parent
  Statement changeParentStmt(this->conn_, this->nsDb_, STMT_CHANGE_PARENT);

  changeParentStmt.bindParam(0, dest);
  changeParentStmt.bindParam(1, inode);

  if (changeParentStmt.execute() == 0)
    throw DmException(DM_INTERNAL_ERROR, "Could not update the parent ino!");

  // Reduce nlinks from old parent
  ExtendedStat oldParent = this->extendedStat(file.parent);
          
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
  ExtendedStat newParent = this->extendedStat(dest);
  
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
  
  // Done
  this->commit();
}



void INodeMySql::rename(ino_t inode, const std::string& name) throw (DmException)
{
  Statement changeNameStmt(this->conn_, this->nsDb_, STMT_CHANGE_NAME);

  changeNameStmt.bindParam(0, name);
  changeNameStmt.bindParam(1, inode);

  if (changeNameStmt.execute() == 0)
    throw DmException(DM_INTERNAL_ERROR, "Could not change the name");
}



ExtendedStat INodeMySql::extendedStat(ino_t inode) throw (DmException)
{
  Statement    stmt(this->conn_, this->nsDb_, STMT_GET_FILE_BY_ID);
  ExtendedStat meta;

  memset(&meta, 0x00, sizeof(ExtendedStat));

  stmt.bindParam(0, inode);
  stmt.execute();
  bindMetadata(stmt, &meta);

  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_FILE, "File %l not found", inode);

  return meta;
}



ExtendedStat INodeMySql::extendedStat(ino_t parent, const std::string& name) throw (DmException)
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



ExtendedStat INodeMySql::extendedStat(const std::string& guid) throw (DmException)
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



SymLink INodeMySql::readLink(ino_t inode) throw (DmException)
{
  Statement stmt(this->conn_, this->nsDb_, STMT_GET_SYMLINK);
  SymLink   link;

  memset(&link, 0x00, sizeof(SymLink));

  stmt.bindParam(0, inode);
  stmt.execute();

  stmt.bindResult(0, &link.inode);
  stmt.bindResult(1, link.link, sizeof(link), 0);

  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_FILE, "Link %ld not found", inode);


  return link;
}



void INodeMySql::addReplica(ino_t inode, const std::string& server,
                            const std::string& sfn, char status, char fileType,
                            const std::string& poolName, const std::string& fileSystem) throw (DmException)
{
  std::string  host;

  // If server is empty, parse the surl
  if (server.empty()) {
    Url u = splitUrl(sfn);
    host = u.host;
  }
  else {
    host = server;
  }

  // Add it
  Statement statement(this->conn_, this->nsDb_, STMT_ADD_REPLICA);

  statement.bindParam(0, inode);
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



void INodeMySql::deleteReplica(ino_t inode, const std::string& sfn) throw (DmException)
{
  // Remove
  Statement statement(this->conn_, this->nsDb_, STMT_DELETE_REPLICA);
  statement.bindParam(0, inode);
  statement.bindParam(1, sfn);
  statement.execute();
}



std::vector<FileReplica> INodeMySql::getReplicas(ino_t inode) throw (DmException)
{
  FileReplica   replica;
  
  // Set to 0
  memset(&replica, 0x00, sizeof(FileReplica));

  // MySQL statement
  Statement stmt(this->conn_, this->nsDb_, STMT_GET_FILE_REPLICAS);

  // Execute query
  stmt.bindParam(0, inode);
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
  stmt.bindResult(11, replica.rfn,        sizeof(replica.rfn));

  std::vector<FileReplica> replicas;

  // Fetch
  int i = 0;
  while (stmt.fetch()) {
    replicas.push_back(replica);
    ++i;
  };

  return replicas;
}



FileReplica INodeMySql::getReplica(const std::string& rfn) throw (DmException)
{
  Statement stmt(this->conn_, this->nsDb_, STMT_GET_REPLICA_BY_URL);
  stmt.bindParam(0, rfn);
  
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
  stmt.bindResult(11, r.rfn,        sizeof(r.rfn));

  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_REPLICA, "Replica " + rfn + " not found");

  return r;
}



void INodeMySql::setReplica(const FileReplica& rdata) throw (DmException)
{
  // Update
  Statement stmt(this->conn_, this->nsDb_, STMT_UPDATE_REPLICA);
  stmt.bindParam(0, rdata.atime);
  stmt.bindParam(1, rdata.ltime);
  stmt.bindParam(2, rdata.nbaccesses);
  stmt.bindParam(3, std::string(&rdata.status, 1));
  stmt.bindParam(4, std::string(&rdata.type, 1));
  stmt.bindParam(5, rdata.replicaid);

  stmt.execute();
}

 

void INodeMySql::utime(ino_t inode, const struct utimbuf* buf) throw (DmException)
{
  // If NULL, current time.
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



void INodeMySql::changeMode(ino_t inode, uid_t uid, gid_t gid, mode_t mode, const std::string& acl) throw (DmException)
{
  // Update DB
  Statement stmt(this->conn_, this->nsDb_, STMT_UPDATE_PERMS);
  stmt.bindParam(0, uid);
  stmt.bindParam(1, gid);
  stmt.bindParam(2, mode);
  stmt.bindParam(3, acl);
  stmt.bindParam(4, inode);
  stmt.execute();
}



void INodeMySql::changeSize(ino_t inode, size_t size) throw (DmException)
{
  Statement stmt(this->conn_, this->nsDb_, STMT_CHANGE_SIZE);
  stmt.bindParam(0, size);
  stmt.bindParam(1, inode);
  stmt.execute();
}



std::string INodeMySql::getComment(ino_t inode) throw (DmException)
{
  char comment[COMMENT_MAX];

  Statement stmt(this->conn_, this->nsDb_, STMT_GET_COMMENT);

  stmt.bindParam(0, inode);
  stmt.execute();
  
  stmt.bindResult(0, comment, COMMENT_MAX);
  if (!stmt.fetch())
    throw DmException(DM_NO_COMMENT, "There is no comment for %l", inode);

  return std::string(comment);
}



void INodeMySql::setComment(ino_t inode, const std::string& comment) throw (DmException)
{
  // Try to set first
  Statement stmt(this->conn_, this->nsDb_, STMT_SET_COMMENT);

  stmt.bindParam(0, comment);
  stmt.bindParam(1, inode);

  if (stmt.execute() == 0) {
    // No update! Try inserting
    Statement stmti(this->conn_, this->nsDb_, STMT_INSERT_COMMENT);

    stmti.bindParam(0, inode);
    stmti.bindParam(1, comment);
    
    stmti.execute();
  }
}



void INodeMySql::deleteComment(ino_t inode) throw (DmException)
{
  Statement stmt(this->conn_, this->nsDb_, STMT_DELETE_COMMENT);
  stmt.bindParam(0, inode);
  stmt.execute();
}



void INodeMySql::setGuid(ino_t inode, const std::string& guid) throw (DmException)
{
  Statement stmt(this->conn_, this->nsDb_, STMT_SET_GUID);

  stmt.bindParam(0, guid);
  stmt.bindParam(1, inode);

  stmt.execute();
}



IDirectory* INodeMySql::openDir(ino_t inode) throw (DmException)
{
  NsMySqlDir  *dir;
  ExtendedStat meta;

  // Get the directory
  meta = this->extendedStat(inode);

  // Create the handle
  dir = new NsMySqlDir();
  dir->dir = meta;
   
  try {
    dir->stmt = new Statement(this->conn_, this->nsDb_, STMT_GET_LIST_FILES);
    dir->stmt->bindParam(0, inode);
    dir->stmt->execute();
    bindMetadata(*dir->stmt, &dir->current);
    return dir;
  }
  catch (...) {
    delete dir;
    throw;
  }
}



void INodeMySql::closeDir(IDirectory* dir) throw (DmException)
{
  NsMySqlDir *dirp;

  if (dir == 0x00)
    throw DmException(DM_NULL_POINTER, std::string("Tried to close a null dir"));

  dirp = (NsMySqlDir*)dir;

  delete dirp->stmt;
  delete dirp;
}



ExtendedStat* INodeMySql::readDirx(IDirectory* dir) throw (DmException)
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
    
    return &dirp->current;
  }
  else {
    return 0x00;
  }
}



struct dirent* INodeMySql::readDir (IDirectory* dir) throw (DmException)
{
  if (this->readDirx(dir) == 0)
    return 0x00;
  else
    return &(((NsMySqlDir*)dir)->ds);
}
