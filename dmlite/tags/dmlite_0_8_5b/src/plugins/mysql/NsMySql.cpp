/// @file   NsMySql.cpp
/// @brief  MySQL NS Implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <mysql/mysql.h>
#include <time.h>
#include <dmlite/cpp/utils/checksums.h>
#include <dmlite/cpp/utils/urls.h>
#include <list>
#include <string>
#include <vector>
#include <sys/param.h>

#include "utils/MySqlWrapper.h"
#include "NsMySql.h"
#include "Queries.h"
#include "MySqlFactories.h"

#define NOT_IMPLEMENTED(p)\
p {\
  throw DmException(DMLITE_SYSERR(ENOSYS), #p" not implemented");\
}


using namespace dmlite;



INodeMySql::INodeMySql(NsMySqlFactory* factory,
                       const std::string& db) throw(DmException):
  factory_(factory), transactionLevel_(0), nsDb_(db),
  conn_(0)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");
  // Nothing
}



INodeMySql::~INodeMySql()
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");
  // Nothing
}



std::string INodeMySql::getImplId() const throw ()
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


/// Takes the content of a CStat structure, as it comes from the queries
/// and use it to fill the final ExtendedStat object
/// We also take some corrective action on checksums, to make sure that
/// the legacy chksum fields and the xattrs are somehow coherent, or at least
/// not half empty
static inline void dumpCStat(const CStat& cstat, ExtendedStat* xstat)
{

  xstat->clear();
  Log(Logger::Lvl4, mysqllogmask, mysqllogname,
      " name: " << cstat.name <<
      " parent: " << cstat.parent <<
      " csumtype: " << cstat.csumtype <<
      " csumvalue: " << cstat.csumvalue <<
      " acl: " << cstat.acl);

  xstat->stat      = cstat.stat;
  xstat->csumtype  = cstat.csumtype;
  xstat->csumvalue = cstat.csumvalue;
  xstat->guid      = cstat.guid;
  xstat->name      = cstat.name;
  xstat->parent    = cstat.parent;
  xstat->status    = static_cast<ExtendedStat::FileStatus>(cstat.status);
  xstat->acl       = Acl(cstat.acl);
  xstat->clear();
  xstat->deserialize(cstat.xattr);

  // From LCGDM-1742
  xstat->fixchecksums();

  (*xstat)["type"] = cstat.type;
}



static inline void bindMetadata(Statement& stmt, CStat* meta) throw(DmException)
{
  memset(meta, 0, sizeof(CStat));
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
  stmt.bindResult(17, meta->xattr, sizeof(meta->xattr));
}


void INodeMySql::begin(void) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "Starting transaction");

  if (!conn_) {
    conn_ = MySqlHolder::getMySqlPool().acquire();
  }

  if (!conn_) {
    throw DmException(DMLITE_DBERR(DMLITE_INTERNAL_ERROR),
      "No MySQL connection handle");
  }

  if (this->transactionLevel_ == 0 && mysql_query(this->conn_, "BEGIN") != 0) {
    unsigned int merrno = mysql_errno(this->conn_);
    std::string merror = mysql_error(this->conn_);
    MySqlHolder::getMySqlPool().release(conn_);
    conn_ = 0;
    throw DmException(DMLITE_DBERR(merrno), merror);
  }

  this->transactionLevel_++;
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting.");
}



void INodeMySql::commit(void) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");

  if (this->transactionLevel_ == 0)
    throw DmException(DMLITE_SYSERR(DMLITE_INTERNAL_ERROR),
                      "INodeMySql::commit Inconsistent state (Maybe there is a\
 commit without a begin, or a badly handled error sequence.)");

  if (!conn_) {
    throw DmException(DMLITE_DBERR(DMLITE_INTERNAL_ERROR),
      "No MySQL connection handle");
  }

  this->transactionLevel_--;

  if (this->transactionLevel_ == 0) {
    int qret;
    unsigned int merrno = 0;
    std::string merror;

    Log(Logger::Lvl4, mysqllogmask, mysqllogname, "Releasing transaction.");
    qret = mysql_query(conn_, "COMMIT");
    if (qret != 0) {
      merrno = mysql_errno(this->conn_);
      merror = mysql_error(this->conn_);
    }
    MySqlHolder::getMySqlPool().release(conn_);
    conn_ = 0;
    if  (qret != 0) {
      throw DmException(DMLITE_DBERR(merrno), merror);
    }
  }

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting.");
}



void INodeMySql::rollback(void) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");

  this->transactionLevel_ = 0;

  if (conn_) {
    int qret;
    unsigned int merrno = 0;
    std::string merror;

    qret = mysql_query(this->conn_, "ROLLBACK");

    if (qret != 0) {
      merrno = mysql_errno(this->conn_);
      merror = mysql_error(this->conn_);
    }

    MySqlHolder::getMySqlPool().release(conn_);
    conn_ = 0;

    if (qret != 0) {
      throw DmException(DMLITE_DBERR(merrno), merror);
    }
  }

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting.");
}



ExtendedStat INodeMySql::create(const ExtendedStat& nf) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");

  ExtendedStat parentMeta;


  // Destination must not exist!
  // For the future... I think that this overhead can be avoided by more carefully
  // checking for the execute() return values and exceptions
  //   try {
  //     this->extendedStat(nf.parent, nf.name);
  //     throw DmException(EEXIST, "%s already exists", nf.name.c_str());
  //   }
  //   catch (DmException& e) {
  //     if (e.code() != ENOENT) throw;
  //   }



  // Fetch the new file ID
  ino_t newFileId = 0;

  // Start transaction
  InodeMySqlTrans trans(this);

  try {


    {
      // Scope to make sure that the local objects that involve mysql
      // are destroyed before the transaction is closed


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


      // Closing the scope here makes sure that no local mysql-involving objects
      // are still around when we close the transaction
    }



    // Get parent metadata, if it is not root
    if (nf.parent > 0) {
      parentMeta = this->extendedStat(nf.parent);
    }

    // Regular files start with 1 link. Directories 0.
    unsigned    nlink   = S_ISDIR(nf.stat.st_mode) ? 0 : 1;
    std::string aclStr  = nf.acl.serialize();
    char        cstatus = static_cast<char>(nf.status);

    // Create the entry
    Statement fileStmt(this->conn_, this->nsDb_, STMT_INSERT_FILE);

    fileStmt.bindParam( 0, newFileId);
    fileStmt.bindParam( 1, nf.parent);
    fileStmt.bindParam( 2, nf.name);
    fileStmt.bindParam( 3, nf.stat.st_mode);
    fileStmt.bindParam( 4, nlink);
    fileStmt.bindParam( 5, nf.stat.st_uid);
    fileStmt.bindParam( 6, nf.stat.st_gid);
    fileStmt.bindParam( 7, nf.stat.st_size);
    fileStmt.bindParam( 8, 0);
    fileStmt.bindParam( 9, std::string(&cstatus, 1));
    fileStmt.bindParam(10, nf.csumtype);
    fileStmt.bindParam(11, nf.csumvalue);
    fileStmt.bindParam(12, aclStr);
    fileStmt.bindParam(13, nf.serialize());

    fileStmt.execute();

    // Increment the parent nlink
    if (nf.parent > 0) {
      Statement nlinkStmt(this->conn_, this->nsDb_, STMT_NLINK_FOR_UPDATE);
      nlinkStmt.bindParam(0, nf.parent);
      nlinkStmt.execute();
      nlinkStmt.bindResult(0, &parentMeta.stat.st_nlink);
      nlinkStmt.fetch();

      Statement nlinkUpdateStmt(this->conn_, this->nsDb_, STMT_UPDATE_NLINK);

      parentMeta.stat.st_nlink++;
      nlinkUpdateStmt.bindParam(0, parentMeta.stat.st_nlink);
      nlinkUpdateStmt.bindParam(1, parentMeta.stat.st_ino);

      nlinkUpdateStmt.execute();
    }

    // Closing the scope here makes sure that no local mysql-involving objects
    // are still around when we close the transaction
    // Commit the local trans object
    // This also releases the connection back to the pool
    trans.Commit();
  }



  catch (DmException e) {
    if (e.code() | DMLITE_DATABASE_ERROR) {
      int c = e.code() ^ DMLITE_DATABASE_ERROR;
      if (c == 1062) // mysql duplicate key
        throw DmException(EEXIST, "File may already exist - mysql duplicate key");
    }
    throw;
  }

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting.");


  // Note: Maybe also this additional overhead can be avoided in the future
  return this->extendedStat(newFileId);
}



void INodeMySql::symlink(ino_t inode, const std::string &link) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " lnk:" << link);

  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());

  Statement stmt(conn, this->nsDb_, STMT_INSERT_SYMLINK);

  stmt.bindParam(0, inode);
  stmt.bindParam(1, link);

  stmt.execute();
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting.  lnk:" << link);
}



void INodeMySql::unlink(ino_t inode) throw (DmException)
{
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, " inode:" << inode);

  // Get file metadata
  ExtendedStat file = this->extendedStat(inode);

  // Non empty directories can not be removed with this method
  if (S_ISDIR(file.stat.st_mode) && file.stat.st_nlink > 0)
    throw DmException(EISDIR,
                      "Inode %ld is a directory and it is not empty", inode);

  // Get the parent
  ExtendedStat parent = this->extendedStat(file.parent);

  {
    // All preconditions are good! Start transaction.
    // Start transaction
    InodeMySqlTrans trans(this);

    {
      // Scope to make sure that the local objects that involve mysql
      // are destroyed before the transaction is closed

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

    }
    // Done!

    // Commit the local trans object
    // This also releases the connection back to the pool
    trans.Commit();
  }

  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "Deleting symlinks, comments, replicas.  inode:" << inode);

  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  {
    // Scope to make sure that the local objects that involve mysql
    // are destroyed before the transaction is closed

    // Remove associated symlink
    Statement delSymlink(conn, this->nsDb_, STMT_DELETE_SYMLINK);
    delSymlink.bindParam(0, inode);
    delSymlink.execute();

    // Remove associated comments
    Statement delComment(conn, this->nsDb_, STMT_DELETE_COMMENT);
    delComment.bindParam(0, inode);
    delComment.execute();

    // Remove replicas
    Statement delReplicas(conn, this->nsDb_, STMT_DELETE_ALL_REPLICAS);
    delReplicas.bindParam(0, inode);
    delReplicas.execute();


  }

  Log(Logger::Lvl2, mysqllogmask, mysqllogname, "Exiting.  inode:" << inode);
}



void INodeMySql::move(ino_t inode, ino_t dest) throw (DmException)
{
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, " inode:" << inode << " dest:" << dest);



  // All preconditions are good! Start transaction.
  // Start transaction
  InodeMySqlTrans trans(this);

  {
    // Scope to make sure that the local objects that involve mysql
    // are destroyed before the transaction is closed

    // Metadata
    ExtendedStat file = this->extendedStat(inode);

    // Make sure the destiny is a dir!
    ExtendedStat newParent = this->extendedStat(dest);
    if (!S_ISDIR(newParent.stat.st_mode))
      throw DmException(ENOTDIR, "Inode %ld is not a directory", dest);

    // Change parent
    Statement changeParentStmt(this->conn_, this->nsDb_, STMT_CHANGE_PARENT);

    changeParentStmt.bindParam(0, dest);
    changeParentStmt.bindParam(1, inode);

    if (changeParentStmt.execute() == 0)
      throw DmException(DMLITE_SYSERR(DMLITE_INTERNAL_ERROR),
			"Could not update the parent ino!");

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
      throw DmException(DMLITE_SYSERR(DMLITE_INTERNAL_ERROR),
			"Could not update the old parent nlink!");

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
      throw DmException(DMLITE_SYSERR(DMLITE_INTERNAL_ERROR),
			"Could not update the new parent nlink!");



      // Closing the scope here makes sure that no local mysql-involving objects
      // are still around when we close the transaction
  }

  // Commit the local trans object
  // This also releases the connection back to the pool
  trans.Commit();


  Log(Logger::Lvl2, mysqllogmask, mysqllogname, "Exiting.  inode:" << inode << " dest:" << dest);
}



void INodeMySql::rename(ino_t inode, const std::string& name) throw (DmException)
{
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, " inode:" << inode << " name:" << name);

  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement changeNameStmt(conn, this->nsDb_, STMT_CHANGE_NAME);

  changeNameStmt.bindParam(0, name);
  changeNameStmt.bindParam(1, inode);

  if (changeNameStmt.execute() == 0)
    throw DmException(DMLITE_SYSERR(DMLITE_INTERNAL_ERROR),
                      "Could not change the name");

  Log(Logger::Lvl2, mysqllogmask, mysqllogname, "Exiting.  inode:" << inode << " name:" << name);
}

ExtendedStat INodeMySql::extendedStat(ino_t inode) throw (DmException)
{
  ExtendedStat meta;
  DmStatus st = this->extendedStat(meta, inode);
  if(!st.ok()) throw st.exception();
  return meta;
}

DmStatus INodeMySql::extendedStat(ExtendedStat &xstat, ino_t inode) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " inode:" << inode);

  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement    stmt(conn, this->nsDb_, STMT_GET_FILE_BY_ID);
  CStat        cstat;
  xstat = ExtendedStat();

  stmt.bindParam(0, inode);
  stmt.execute();
  bindMetadata(stmt, &cstat);

  if (!stmt.fetch())
    return DmStatus(ENOENT, "Inode %ld not found", inode);

  dumpCStat(cstat, &xstat);

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting.  inode:" << inode << " sz:" << xstat.size());
  return DmStatus();
}

ExtendedStat INodeMySql::extendedStat(ino_t parent, const std::string& name) throw (DmException)
{
  ExtendedStat meta;
  DmStatus st = this->extendedStat(meta, parent, name);
  if(!st.ok()) throw st.exception();
  return meta;
}

DmStatus INodeMySql::extendedStat(ExtendedStat &xstat, ino_t parent, const std::string& name) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " parent:" << parent << " name:" << name);

  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement    stmt(conn, this->nsDb_, STMT_GET_FILE_BY_NAME);
  CStat        cstat;
  xstat = ExtendedStat();

  stmt.bindParam(0, parent);
  stmt.bindParam(1, name);
  stmt.execute();

  bindMetadata(stmt, &cstat);

  if (!stmt.fetch())
    return DmStatus(ENOENT, SSTR("'" << name << "' not found in parent directory id: " << parent));

  dumpCStat(cstat, &xstat);
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. parent:" << parent << " name:" << name << " sz:" << xstat.size());
  return DmStatus();
}

ExtendedStat INodeMySql::extendedStat(const std::string& guid) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " guid:" << guid);

  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement    stmt(conn, this->nsDb_, STMT_GET_FILE_BY_GUID);
  ExtendedStat meta;
  CStat        cstat;

  stmt.bindParam(0, guid);
  stmt.execute();

  bindMetadata(stmt, &cstat);

  if (!stmt.fetch())
    throw DmException(ENOENT, "File with guid " + guid + " not found");

  dumpCStat(cstat, &meta);

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting.  guid:" << guid << " sz:" << meta.size());
  return meta;
}



SymLink INodeMySql::readLink(ino_t inode) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " inode:" << inode);

  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->nsDb_, STMT_GET_SYMLINK);
  SymLink   link;
  char      clink[4096];

  memset(clink, 0, sizeof(clink));
  stmt.bindParam(0, inode);
  stmt.execute();

  stmt.bindResult(0, &link.inode);
  stmt.bindResult(1, clink, sizeof(clink), 0);

  if (!stmt.fetch())
    throw DmException(ENOENT, "Link %ld not found", inode);

  link.link = clink;

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. inode:" << inode);

  return link;
}



void INodeMySql::addReplica(const Replica& replica) throw (DmException)
{
  std::string  host;
  char         cstatus, ctype;

  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " replica:" << replica.rfn);

  // Make sure fileid exists and is a regular file
  ExtendedStat s = this->extendedStat(replica.fileid);
  if (!S_ISREG(s.stat.st_mode))
    throw DmException(EINVAL,
                      "Inode %ld is not a regular file", replica.fileid);

  // The replica should not exist already
  Replica tmp;
  DmStatus st = this->getReplica(tmp, replica.rfn);
  if(st.ok()) {
    throw DmException(EEXIST,
                      "Replica %s already registered", replica.rfn.c_str());
  }
  else if(st.code() != DMLITE_NO_SUCH_REPLICA) {
    throw st.exception();
  }

  // If server is empty, parse the surl
  if (replica.server.empty()) {
    Url u(replica.rfn);
    host = u.domain;
  }
  else {
    host = replica.server;
  }

  cstatus = static_cast<char>(replica.status);
  ctype   = static_cast<char>(replica.type);

  // Add it
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement statement(conn, this->nsDb_, STMT_ADD_REPLICA);

  statement.bindParam(0, replica.fileid);
  statement.bindParam(1, NULL, 0);
  statement.bindParam(2, std::string(&cstatus, 1));
  statement.bindParam(3, std::string(&ctype, 1));

  if (replica.setname.size() == 0)
    statement.bindParam(4, NULL, 0);
  else
    statement.bindParam(4, replica.setname);

  statement.bindParam(5, replica.getString("pool"));
  statement.bindParam(6, host);
  statement.bindParam(7, replica.getString("filesystem"));
  statement.bindParam(8, replica.rfn);
  statement.bindParam(9, replica.serialize());

  statement.execute();

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. replica:" << replica.rfn);
}



void INodeMySql::deleteReplica(const Replica& replica) throw (DmException)
{

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, " replica:" << replica.rfn);
  // Remove
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement statement(conn, this->nsDb_, STMT_DELETE_REPLICA);
  statement.bindParam(0, replica.fileid);
  statement.bindParam(1, replica.rfn);
  statement.execute();


  ExtendedStat st;


  // Stat the file to have the size of the file that was just touched
  try {
    Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Looking up size of fileid " << replica.fileid << " : " << replica.rfn);
    st = this->extendedStat(replica.fileid);
    Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Ok. Size of " << replica.rfn << " is " << st.stat.st_size);
  }
  catch (DmException& e) {
    Err( "MysqlIOPassthroughDriver::deleteReplica" , " Cannot retrieve filesize for replica:" << replica.rfn);
    return;
  }

  // Subtract this filesize to the size of its parent dirs, only the first N levels
  if (replica.status == Replica::kAvailable) {


    // Start transaction
    InodeMySqlTrans trans(this);

    off_t sz = st.stat.st_size;


    ino_t hierarchy[128];
    size_t hierarchysz[128];
    int idx = 0;
    while (st.parent) {

      Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Going to stat " << st.parent << " parent of " << st.stat.st_ino << " with idx " << idx);

      try {
        st = extendedStat(st.parent);
      }
      catch (DmException& e) {
        Err( "INodeMySql::deleteReplica" , " Cannot stat inode " << st.parent << " parent of " << st.stat.st_ino);
        return;
      }

      hierarchy[idx] = st.stat.st_ino;
      hierarchysz[idx] = st.stat.st_size;

      Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Size of inode " << st.stat.st_ino <<
      " is " << st.stat.st_size << " with idx " << idx);

      idx++;

      if (idx >= (int)sizeof(hierarchy)) {
        Err( "INodeMySql::deleteReplica" , " Too many parent directories for replica " << replica.rfn);
        return;
      }
    }

    // Update the filesize in the first levels
    // Avoid the contention on /dpm/voname/home
    if (idx > 0) {
      Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Going to set sizes. Max depth found: " << idx);
      for (int i = MAX(0, idx-3); i >= MAX(0, idx-1-factory_->dirspacereportdepth); i--) {
        Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Inode: " << hierarchy[i] << " Size: " << hierarchysz[i] << "-->" <<  hierarchysz[i] - sz);
        try {
          setSize( hierarchy[i], ((hierarchysz[i] < (size_t)sz) ? 0 : (hierarchysz[i] - sz)) );
        }
        catch (DmException& e) {
          Err( "FilesystemPoolHandler::removeReplica" , " Cannot setSize inode " << hierarchy[i] << " to " << MAX(0, hierarchysz[i] - sz));
          return;
        }
      }
    }
    else {
      Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Cannot set any size. Max depth found: " << idx);
    }

    // Commit the local trans object
    // This also releases the connection back to the pool
    trans.Commit();
  }


  Log(Logger::Lvl2, mysqllogmask, mysqllogname, "Exiting. replica:" << replica.rfn);
}



std::vector<Replica> INodeMySql::getReplicas(ino_t inode) throw (DmException)
{
  Replica   replica;
  char      cpool[512];
  char      setnm[512];
  char      cserver[512];
  char      cfilesystem[512];
  char      crfn[4096];
  char      cmeta[4096];
  char      ctype, cstatus;

  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " inode:" << inode);

  // MySQL statement
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->nsDb_, STMT_GET_FILE_REPLICAS);

  // Execute query
  stmt.bindParam(0, inode);
  stmt.execute();

  // Bind results
  stmt.bindResult( 0, &replica.replicaid);
  stmt.bindResult( 1, &replica.fileid);
  stmt.bindResult( 2, &replica.nbaccesses);
  stmt.bindResult( 3, &replica.atime);
  stmt.bindResult( 4, &replica.ptime);
  stmt.bindResult( 5, &replica.ltime);
  stmt.bindResult( 6, &cstatus, 1);
  stmt.bindResult( 7, &ctype, 1);
  stmt.bindResult( 8, setnm,       sizeof(setnm));
  stmt.bindResult( 9, cpool,       sizeof(cpool));
  stmt.bindResult( 10, cserver,     sizeof(cserver));
  stmt.bindResult(11, cfilesystem, sizeof(cfilesystem));
  stmt.bindResult(12, crfn,        sizeof(crfn));
  stmt.bindResult(13, cmeta,       sizeof(cmeta));

  std::vector<Replica> replicas;

  // Fetch
  int i = 0;
  while (stmt.fetch()) {
    replica.clear();

    replica.rfn    = crfn;
    replica.server = cserver;
    replica.status = static_cast<Replica::ReplicaStatus>(cstatus);
    replica.type   = static_cast<Replica::ReplicaType>(ctype);
    replica.setname       = std::string(setnm);
    replica.deserialize(cmeta);

    replica["pool"]       = std::string(cpool);
    replica["filesystem"] = std::string(cfilesystem);

    replicas.push_back(replica);
    ++i;
  };

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. inode:" << inode << " nrepls:" << i);
  return replicas;
}

Replica INodeMySql::getReplica(int64_t rid) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " rid:" << rid);

  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->nsDb_, STMT_GET_REPLICA_BY_ID);
  stmt.bindParam(0, rid);

  stmt.execute();

  Replica r;

  char setnm[512];
  char cpool[512];
  char cserver[512];
  char cfilesystem[512];
  char crfn[4096];
  char cmeta[4096];
  char ctype, cstatus;

  stmt.bindResult( 0, &r.replicaid);
  stmt.bindResult( 1, &r.fileid);
  stmt.bindResult( 2, &r.nbaccesses);
  stmt.bindResult( 3, &r.atime);
  stmt.bindResult( 4, &r.ptime);
  stmt.bindResult( 5, &r.ltime);
  stmt.bindResult( 6, &cstatus, 1);
  stmt.bindResult( 7, &ctype, 1);
  stmt.bindResult( 8, setnm,       sizeof(setnm));
  stmt.bindResult( 9, cpool,       sizeof(cpool));
  stmt.bindResult(10, cserver,     sizeof(cserver));
  stmt.bindResult(11, cfilesystem, sizeof(cfilesystem));
  stmt.bindResult(12, crfn,        sizeof(crfn));
  stmt.bindResult(13, cmeta,       sizeof(cmeta));

  if (!stmt.fetch())
    throw DmException(DMLITE_NO_SUCH_REPLICA, "Replica %d not found", rid);

  r.rfn           = crfn;
  r.server        = cserver;
  r.setname       = std::string(setnm);
  r.status        = static_cast<Replica::ReplicaStatus>(cstatus);
  r.type          = static_cast<Replica::ReplicaType>(ctype);
  r.deserialize(cmeta);

  r["pool"]       = std::string(cpool);
  r["filesystem"] = std::string(cfilesystem);

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. rid:" << rid << " repl:" << r.rfn);

  return r;
}

DmStatus INodeMySql::getReplica(Replica &r, const std::string& rfn) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " rfn:" << rfn);

  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->nsDb_, STMT_GET_REPLICA_BY_URL);
  stmt.bindParam(0, rfn);

  stmt.execute();

  r = Replica();

  char setnm[512];
  char cpool[512];
  char cserver[512];
  char cfilesystem[512];
  char crfn[4096];
  char cmeta[4096];
  char ctype, cstatus;

  stmt.bindResult( 0, &r.replicaid);
  stmt.bindResult( 1, &r.fileid);
  stmt.bindResult( 2, &r.nbaccesses);
  stmt.bindResult( 3, &r.atime);
  stmt.bindResult( 4, &r.ptime);
  stmt.bindResult( 5, &r.ltime);
  stmt.bindResult( 6, &cstatus, 1);
  stmt.bindResult( 7, &ctype, 1);
  stmt.bindResult( 8, setnm,       sizeof(setnm));
  stmt.bindResult( 9, cpool,       sizeof(cpool));
  stmt.bindResult(10, cserver,     sizeof(cserver));
  stmt.bindResult(11, cfilesystem, sizeof(cfilesystem));
  stmt.bindResult(12, crfn,        sizeof(crfn));
  stmt.bindResult(13, cmeta,       sizeof(cmeta));

  if (!stmt.fetch())
    return DmStatus(DMLITE_NO_SUCH_REPLICA, "Replica %s not found", rfn.c_str());

  r.rfn           = crfn;
  r.server        = cserver;
  r.setname       = std::string(setnm);
  r.status        = static_cast<Replica::ReplicaStatus>(cstatus);
  r.type          = static_cast<Replica::ReplicaType>(ctype);
  r.deserialize(cmeta);

  r["pool"]       = std::string(cpool);
  r["filesystem"] = std::string(cfilesystem);

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. repl:" << r.rfn);
  return DmStatus();
}

Replica INodeMySql::getReplica(const std::string& rfn) throw (DmException)
{
  Replica rep;
  DmStatus st = this->getReplica(rep, rfn);
  if(!st.ok()) throw st.exception();
  return rep;
}

void INodeMySql::updateReplica(const Replica& rdata) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " rdata:" << rdata.rfn);

  // Update
  char status = static_cast<char>(rdata.status);
  char type   = static_cast<char>(rdata.type);

  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->nsDb_, STMT_UPDATE_REPLICA);

  stmt.bindParam(0, rdata.nbaccesses);
  stmt.bindParam(1, rdata.atime);
  stmt.bindParam(2, rdata.ptime);
  stmt.bindParam(3, rdata.ltime);
  stmt.bindParam(4, std::string(&type, 1));
  stmt.bindParam(5, std::string(&status, 1));
  stmt.bindParam(6, rdata.getString("pool"));
  stmt.bindParam(7, rdata.server);
  stmt.bindParam(8, rdata.getString("filesystem"));
  stmt.bindParam(9, rdata.rfn);
  stmt.bindParam(10, rdata.serialize());

  if (rdata.setname.size() == 0) {
    stmt.bindParam(11, NULL, 0);
  }
  else {
    stmt.bindParam(11, rdata.setname);

  }

  stmt.bindParam(12, rdata.replicaid);

  stmt.execute();

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. rdata:" << rdata.rfn);
}



void INodeMySql::utime(ino_t inode, const struct utimbuf* buf) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " inode:" << inode);

  // If NULL, current time.
  struct utimbuf internal;
  if (buf == NULL) {
    buf = &internal;
    internal.actime  = time(NULL);
    internal.modtime = time(NULL);
  }

  // Change
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->nsDb_, STMT_UTIME);
  stmt.bindParam(0, buf->actime);
  stmt.bindParam(1, buf->modtime);
  stmt.bindParam(2, inode);

  stmt.execute();

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. inode:" << inode);
}



void INodeMySql::setMode(ino_t inode, uid_t uid, gid_t gid,
                         mode_t mode, const Acl& acl) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " inode:" << inode << " mode:" << mode);

  // Clean type bits
  mode &= ~S_IFMT;

  // Update DB
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->nsDb_, STMT_UPDATE_PERMS);
  stmt.bindParam(0, uid);
  stmt.bindParam(1, uid);
  stmt.bindParam(2, gid);
  stmt.bindParam(3, gid);
  stmt.bindParam(4, mode);
  stmt.bindParam(5, acl.serialize());
  stmt.bindParam(6, acl.serialize());
  stmt.bindParam(7, inode);
  stmt.execute();

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. inode:" << inode << " mode:" << mode);
}



void INodeMySql::setSize(ino_t inode, size_t size) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " inode:" << inode << " size:" << size);

  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->nsDb_, STMT_CHANGE_SIZE);
  stmt.bindParam(0, size);
  stmt.bindParam(1, inode);
  stmt.execute();

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. inode:" << inode << " size:" << size);
}



void INodeMySql::setChecksum(ino_t inode, const std::string& csumtype,
                             const std::string& csumvalue) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " inode:" << inode << " csumtype:" << csumtype << " csumvalue:" << csumvalue);

  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->nsDb_, STMT_CHANGE_CHECKSUM);
  stmt.bindParam(0, csumtype);
  stmt.bindParam(1, csumvalue);
  stmt.bindParam(2, inode);
  stmt.execute();
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. inode:" << inode << " csumtype:" << csumtype << " csumvalue:" << csumvalue);
}



std::string INodeMySql::getComment(ino_t inode) throw (DmException)
{
  char comment[1024];

  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " inode:" << inode);

  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->nsDb_, STMT_GET_COMMENT);

  stmt.bindParam(0, inode);
  stmt.execute();

  stmt.bindResult(0, comment, sizeof(comment));
  if (!stmt.fetch())
    comment[0] = '\0';

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. inode:" << inode << " comment:'" << comment << "'");
  return std::string(comment);
}



void INodeMySql::setComment(ino_t inode, const std::string& comment) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " inode:" << inode << " comment:'" << comment << "'");

  // Try to set first
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->nsDb_, STMT_SET_COMMENT);

  stmt.bindParam(0, comment);
  stmt.bindParam(1, inode);

  if (stmt.execute() == 0) {
    // No update! Try inserting
    Statement stmti(conn, this->nsDb_, STMT_INSERT_COMMENT);

    stmti.bindParam(0, inode);
    stmti.bindParam(1, comment);

    stmti.execute();
  }

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. inode:" << inode << " comment:'" << comment << "'");
}



void INodeMySql::deleteComment(ino_t inode) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " inode:" << inode );

  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->nsDb_, STMT_DELETE_COMMENT);
  stmt.bindParam(0, inode);
  stmt.execute();

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. inode:" << inode);
}



void INodeMySql::setGuid(ino_t inode, const std::string& guid) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " inode:" << inode << " guid:" << guid );

  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->nsDb_, STMT_SET_GUID);

  stmt.bindParam(0, guid);
  stmt.bindParam(1, inode);

  stmt.execute();

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. inode:" << inode << " guid:" << guid );
}



void INodeMySql::updateExtendedAttributes(ino_t inode,
                                          const Extensible& attr) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " inode:" << inode << " nattrs:" << attr.size() );
  {
    PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
    Statement stmt(conn, this->nsDb_, STMT_SET_XATTR);

    stmt.bindParam(0, attr.serialize());
    stmt.bindParam(1, inode);

    stmt.execute();
  }

  // If there were any checksums in list of attributes which have a legacy short
  // type name set the first of them in the legacy csumtype, csumvalue columns
  std::vector<std::string> keys = attr.getKeys();
  std::string shortCsumType;
  std::string csumValue;

  for (unsigned i = 0; i < keys.size(); ++i) {
    if (checksums::isChecksumFullName(keys[i])) {
      std::string csumXattr = keys[i];
      shortCsumType = checksums::shortChecksumName(csumXattr);
      if (!shortCsumType.empty() && shortCsumType.length() <= 2) {
        csumValue     = attr.getString(csumXattr);
        break;
      }
    }
  }

  if (!csumValue.empty()) {
    this->setChecksum(inode, shortCsumType, csumValue);
  }

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. inode:" << inode << " nattrs:" << attr.size() );
}



IDirectory* INodeMySql::openDir(ino_t inode) throw (DmException)
{
  NsMySqlDir  *dir;
  ExtendedStat meta;

  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " inode:" << inode);

  // Get the directory
  meta = this->extendedStat(inode);
  if (!S_ISDIR(meta.stat.st_mode))
    throw DmException(ENOTDIR, "Inode %ld is not a directory", inode);

  // Create the handle
  dir = new NsMySqlDir();
  dir->dir = meta;

  try {
    dir->conn_ = 0; // in case an exception is thrown on the following line
    dir->conn_ = MySqlHolder::getMySqlPool().acquire();
    dir->stmt = new Statement(dir->conn_, this->nsDb_, STMT_GET_LIST_FILES);
    dir->stmt->bindParam(0, inode);
    dir->stmt->execute();
    bindMetadata(*dir->stmt, &dir->cstat);

    dir->eod = !dir->stmt->fetch();
  }
  catch (...) {
    if (dir->conn_) MySqlHolder::getMySqlPool().release(dir->conn_);
    dir->conn_ = 0;
    delete dir;
    throw;
  }

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. inode:" << inode);
  return dir;
}



void INodeMySql::closeDir(IDirectory* dir) throw (DmException)
{
  NsMySqlDir *dirp;

  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");

  if (dir == NULL)
    throw DmException(DMLITE_SYSERR(EFAULT),
                      std::string("Tried to close a null dir"));

  dirp = dynamic_cast<NsMySqlDir*>(dir);

  if (dirp->conn_) MySqlHolder::getMySqlPool().release(dirp->conn_);
  dirp->conn_ = 0;

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. dir:" << dirp->dir.name);

  delete dirp->stmt;
  delete dirp;

}



ExtendedStat* INodeMySql::readDirx(IDirectory* dir) throw (DmException)
{
  NsMySqlDir *dirp;

  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");

  if (dir == NULL)
    throw DmException(DMLITE_SYSERR(EFAULT),
                      "Tried to read a null dir");

  dirp = dynamic_cast<NsMySqlDir*>(dir);

  if (!dirp->eod) {
    dumpCStat(dirp->cstat, &dirp->current);
    dirp->ds.d_ino  = dirp->current.stat.st_ino;
    strncpy(dirp->ds.d_name,
            dirp->current.name.c_str(),
            sizeof(dirp->ds.d_name));

    dirp->eod = !dirp->stmt->fetch();

    Log(Logger::Lvl4, mysqllogmask, mysqllogname, "Exiting. item:" << dirp->current.name);
    return &dirp->current;
  }
  else {
    Log(Logger::Lvl4, mysqllogmask, mysqllogname, "Exiting. with NULL");
    return NULL;
  }
}



struct dirent* INodeMySql::readDir (IDirectory* dir) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");
  if (this->readDirx(dir) == 0)
    return NULL;
  else
    return &(((NsMySqlDir*)dir)->ds);
}
