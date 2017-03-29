/*
 * Copyright 2015 CERN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */



/** @file   DomeMySQL.cpp
 * @brief  A helper class that wraps DomeMySQL stuff
 * @author Fabrizio Furano
 * @date   Jan 2016
 */

#include "DomeMysql.h"
#include "utils/logger.h"
#include "utils/mysqlpools.h"
#include "utils/MySqlWrapper.h"
#include "utils/DomeUtils.h"
#include "DomeStatus.h"
#include "DomeLog.h"
#include "inode.h"
#include "utils/urls.h"

#include <boost/thread.hpp>
#include <time.h>

#include "DomeMetadataCache.hh"

using namespace dmlite;

#define CNS_DB "cns_db"


/// Struct used internally to bind when reading
struct CStat {
  ino_t       parent;
  struct stat stat;
  char        status;
  short       type;
  char        name[256];
  char        guid[37];
  char        csumtype[4];
  char        csumvalue[34];
  char        acl[300 * 13]; // Maximum 300 entries of 13 bytes each
  char        xattr[1024];
};


/// Class used internally to read drectories.
class DomeMySqlDir {
public:
  DomeMySqlDir() {
    stmt = NULL;
    entry = 0;
  };
  virtual ~DomeMySqlDir() {
    if (stmt) delete stmt;
  };
private:
  ExtendedStat  dir;           ///< Directory being read.
  std::string   path;          ///< Path being read
  CStat         cstat;         ///< Used for the binding
  ExtendedStat  current;       ///< Current entry metadata.
  Statement    *stmt;          ///< The statement.
  bool          eod;           ///< True when end of dir is reached.
  int32_t       entry;         ///< Counts the entries being read

  friend class DomeMySql;
};





/// Takes the content of a CStat structure, as it comes from the queries
/// and use it to fill the final ExtendedStat object
/// We also take some corrective action on checksums, to make sure that
/// the legacy chksum fields and the xattrs are somehow coherent, or at least
/// not half empty
static inline void dumpCStat(const CStat& cstat, ExtendedStat* xstat)
{

  xstat->clear();
  Log(Logger::Lvl4, domelogmask, domelogname,
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




/// Utility to check perms for a directory tree. Tells if a certain user can reach a certain file
dmlite::DmStatus DomeMySql::traverseBackwards(const SecurityContext &secctx, dmlite::ExtendedStat& meta) {
  ExtendedStat current = meta;
  dmlite::DmStatus res;
  // We want to check if we can arrive here...
  while (current.parent != 0) {

    res = getStatbyFileid(current, current.parent);
    if (checkPermissions(&secctx, current.acl, current.stat, S_IEXEC))
      return DmStatus(EACCES, SSTR("Can not access fileid " << current.stat.st_ino <<
        " user: '" << secctx.user.name << "'"));
  }

  return DmStatus();
}

DmStatus DomeMySql::createfile(const dmlite::ExtendedStat &parent, std::string fname, mode_t mode, int uid, int gid) {
  DmStatus ret;

  Log(Logger::Lvl4, domelogmask, domelogname, "Processing: '" << fname << "' mode: " << mode);

  // Create the folder
  ExtendedStat newFile;
  // zero stat structure
  memset(&newFile.stat, 0, sizeof(newFile.stat));
  newFile.parent      = parent.stat.st_ino;
  newFile.name        = fname;
  newFile.stat.st_uid = uid;
  newFile.status      = ExtendedStat::kOnline;
  // Mode
  newFile.stat.st_mode = (mode & ~S_IFMT) | S_IFREG;

  // Effective gid
  gid_t egid;
  if (parent.stat.st_mode & S_ISGID) {
    egid = parent.stat.st_gid;
    newFile.stat.st_mode |= S_ISGID;
  }
  else {
    // We take the gid of the first group of the user
    // Note by FF 06/02/2017: this makes little sense, I ported it from Catalog.cpp
    // and I don't really know what to do
    egid = gid;
  }
  newFile.stat.st_gid = egid;



  // Generate inherited ACL's if there are defaults
  if (parent.acl.has(AclEntry::kDefault | AclEntry::kUserObj) > -1)
    newFile.acl = Acl(parent.acl,
                        uid,
                        egid,
                        mode,
                      &newFile.stat.st_mode);

    // Register
    ret = this->create(newFile);
  if (!ret.ok())
    return DmStatus(EINVAL, SSTR("Can't create file '" << fname << "'"));

  Log(Logger::Lvl3, domelogmask, domelogname, "Created: '" << fname << "' mode: " << mode);

  return DmStatus();

}

DmStatus DomeMySql::create(ExtendedStat& nf)
{
  Log(Logger::Lvl4, domelogmask, domelogname, "Creating new namespace entity. name: '" << nf.name <<
    "' parent: " << nf.parent << " flags: " << nf.stat.st_mode);

  ExtendedStat parentMeta;
  DmStatus r;

  // Get parent metadata, if it is not root
  if (nf.parent > 0) {
    r = this->getStatbyFileid(parentMeta, nf.parent);
    if (!r.ok()) return r;
  }

  // Fetch the new file ID
  ino_t newFileId = 0;

  // Start transaction
  DomeMySqlTrans trans(this);

  try {


    {
      // Scope to make sure that the local objects that involve mysql
      // are destroyed before the transaction is closed


      Statement uniqueId(conn_, CNS_DB, "SELECT id FROM Cns_unique_id FOR UPDATE");

      uniqueId.execute();
      uniqueId.bindResult(0, &newFileId);

      // Update the unique ID
      if (uniqueId.fetch()) {
        Statement updateUnique(conn_, CNS_DB, "UPDATE Cns_unique_id SET id = ?");
        ++newFileId;
        updateUnique.bindParam(0, newFileId);
        updateUnique.execute();
      }
      // Couldn't get, so insert
      else {
        Statement insertUnique(conn_, CNS_DB, "INSERT INTO Cns_unique_id (id) VALUES (?)");
        newFileId = 1;
        insertUnique.bindParam(0, newFileId);
        insertUnique.execute();
      }


      // Closing the scope here makes sure that no local mysql-involving objects
      // are still around when we close the transaction
    }


    // Regular files start with 1 link. Directories 0.
    unsigned    nlink   = S_ISDIR(nf.stat.st_mode) ? 0 : 1;
    std::string aclStr  = nf.acl.serialize();
    char        cstatus = static_cast<char>(nf.status);

    // Create the entry
    Statement fileStmt(this->conn_, CNS_DB, "INSERT INTO Cns_file_metadata\
    (fileid, parent_fileid, name, filemode, nlink, owner_uid, gid,\
    filesize, atime, mtime, ctime, fileclass, status,\
    csumtype, csumvalue, acl, xattr)\
    VALUES\
    (?, ?, ?, ?, ?, ?, ?,\
    ?, UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), ?, ?,\
    ?, ?, ?, ?)");

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
      Statement nlinkStmt(this->conn_, CNS_DB, "SELECT nlink FROM Cns_file_metadata WHERE fileid = ? FOR UPDATE");
      nlinkStmt.bindParam(0, nf.parent);
      nlinkStmt.execute();
      nlinkStmt.bindResult(0, &parentMeta.stat.st_nlink);
      nlinkStmt.fetch();

      Statement nlinkUpdateStmt(this->conn_, CNS_DB, "UPDATE Cns_file_metadata\
      SET nlink = ?, mtime = UNIX_TIMESTAMP(), ctime = UNIX_TIMESTAMP()\
      WHERE fileid = ?");

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
        return DmStatus(EEXIST, SSTR("File '" << nf.name << "' parent: " << nf.parent << " already exists - mysql duplicate key. err: 1062-" << e.what()));
    }
    return DmStatus(e);
  }

  nf.stat.st_ino = newFileId;

  DOMECACHE->pushXstatInfo(nf, DomeFileInfo::Ok);
  
  if (S_ISDIR(nf.stat.st_mode))
    Log(Logger::Lvl1, domelogmask, domelogname, "Created new directory. name: '" << nf.name <<
      "' parent: " << nf.parent << " flags: " << nf.stat.st_mode << " fileid: " << newFileId);
  else
    Log(Logger::Lvl1, domelogmask, domelogname, "Created new file. name: '" << nf.name <<
      "' parent: " << nf.parent << " flags: " << nf.stat.st_mode << " fileid: " << newFileId);

  return DmStatus();
}


DmStatus DomeMySql::makedir(const ExtendedStat &parent, std::string dname, mode_t mode, int uid, int gid) {
  DmStatus ret;

  Log(Logger::Lvl4, domelogmask, domelogname, "Processing: '" << dname << "' mode: " << mode);

  // Create the folder
  ExtendedStat newFolder;
  // zero stat structure
  memset(&newFolder.stat, 0, sizeof(newFolder.stat));
  newFolder.parent      = parent.stat.st_ino;
  newFolder.name        = dname;
  newFolder.stat.st_uid = uid;
  newFolder.status      = ExtendedStat::kOnline;
  // Mode
  newFolder.stat.st_mode = (mode & ~S_IFMT) | S_IFDIR;

  // Effective gid
  gid_t egid;
  if (parent.stat.st_mode & S_ISGID) {
    egid = parent.stat.st_gid;
    newFolder.stat.st_mode |= S_ISGID;
  }
  else {
    // We take the gid of the first group of the user
    // Note by FF 06/02/2017: this makes little sense, I ported it from Catalog.cpp
    // and I don't really know what else to do
    egid = gid;
  }
  newFolder.stat.st_gid = egid;



  // Generate inherited ACL's if there are defaults
  if (parent.acl.has(AclEntry::kDefault | AclEntry::kUserObj) > -1)
    newFolder.acl = Acl(parent.acl,
                        uid,
                        egid,
                        mode,
                        &newFolder.stat.st_mode);

    // Register
    ret = this->create(newFolder);
  if (!ret.ok())
    return DmStatus(EINVAL, SSTR("Can't create folder '" << dname << "'"));

  Log(Logger::Lvl3, domelogmask, domelogname, "Created: '" << dname << "' mode: " << mode);

  return DmStatus();
}



DmStatus DomeMySql::getComment(std::string &comment, ino_t inode)
{
  char c[1024];

  Log(Logger::Lvl4, domelogmask, domelogname, " inode:" << inode);

  try {
    Statement stmt(conn_, CNS_DB, "SELECT comments\
    FROM Cns_user_metadata\
    WHERE u_fileid = ?");

    stmt.bindParam(0, inode);
    stmt.execute();

    stmt.bindResult(0, c, sizeof(c));
    if (!stmt.fetch())
      c[0] = '\0';

    comment = c;

  }
  catch ( DmException e ) {
    return DmStatus(e);
  }


  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. inode:" << inode << " comment:'" << comment << "'");
  return DmStatus();
}



DmStatus DomeMySql::setComment(ino_t inode, const std::string& comment)
{
  Log(Logger::Lvl4, domelogmask, domelogname, " inode:" << inode << " comment:'" << comment << "'");

  // Try to set first
  try {
    Statement stmt(conn_, CNS_DB, "UPDATE Cns_user_metadata\
    SET comments = ?\
    WHERE u_fileid = ?");

    stmt.bindParam(0, comment);
    stmt.bindParam(1, inode);

    if (stmt.execute() == 0) {
      // No update! Try inserting
      Statement stmti(conn_, CNS_DB, "INSERT INTO Cns_user_metadata\
      (u_fileid, comments)\
      VALUES\
      (?, ?)");

      stmti.bindParam(0, inode);
      stmti.bindParam(1, comment);

      stmti.execute();
    }

  }
  catch ( DmException e ) {
    return DmStatus(e);
  }
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. inode:" << inode << " comment:'" << comment << "'");
  return DmStatus();
}



DmStatus DomeMySql::setMode(ino_t inode, uid_t uid, gid_t gid, mode_t mode, const Acl& acl) {
  // uhm
  Log(Logger::Lvl4, domelogmask, domelogname, " inode:" << inode << " mode:" << mode);

  // Clean type bits
  mode &= ~S_IFMT;
  try {
    // Update DB
    Statement stmt(conn_, CNS_DB, "UPDATE Cns_file_metadata\
    SET owner_uid = if(? = -1, owner_uid, ?),\
    gid = if(? = -1, gid, ?),\
    filemode = ((filemode & 61440) | ?),\
    acl = if(length(?) = 0, acl, ?),\
    ctime = UNIX_TIMESTAMP()\
    WHERE fileid = ?"); // 61440 is ~S_IFMT in decimal
    stmt.bindParam(0, uid);
    stmt.bindParam(1, uid);
    stmt.bindParam(2, gid);
    stmt.bindParam(3, gid);
    stmt.bindParam(4, mode);
    stmt.bindParam(5, acl.serialize());
    stmt.bindParam(6, acl.serialize());
    stmt.bindParam(7, inode);
    stmt.execute();
  }
  catch ( DmException e ) {
    return DmStatus(e);
  }

  DOMECACHE->wipeEntry(inode);
  
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. inode:" << inode << " mode:" << mode);
  return DmStatus();
}



DmStatus DomeMySql::move(ino_t inode, ino_t dest)
{
  Log(Logger::Lvl3, domelogmask, domelogname, " inode:" << inode << " dest:" << dest);
  DmStatus r;


  // All preconditions are good! Start transaction.
  // Start transaction
  DomeMySqlTrans trans(this);
  try {
    // Scope to make sure that the local objects that involve mysql
    // are destroyed before the transaction is closed

    // Metadata
    ExtendedStat file;
    r = this->getStatbyFileid(file, inode);
    if (!r.ok()) return r;

    // Make sure the destiny is a dir!
    ExtendedStat newParent;
    r = this->getStatbyFileid(newParent, dest);
    if (!r.ok()) {
      Err("move", "Trouble looking for fileid " << dest);
      return r;
    }

    if (!S_ISDIR(newParent.stat.st_mode))
      throw DmException(ENOTDIR, "Inode %ld is not a directory", dest);

    // Change parent
    Statement changeParentStmt(this->conn_, CNS_DB, "UPDATE Cns_file_metadata\
    SET parent_fileid = ?, ctime = UNIX_TIMESTAMP()\
    WHERE fileid = ?");

    changeParentStmt.bindParam(0, dest);
    changeParentStmt.bindParam(1, inode);

    if (changeParentStmt.execute() == 0)
      throw DmException(DMLITE_SYSERR(DMLITE_INTERNAL_ERROR),
                        "Could not update the parent ino!");

    // Reduce nlinks from old parent
    ExtendedStat oldParent;
    r= this->getStatbyFileid(oldParent, file.parent);
    if (!r.ok()) {
      Err("move", "trouble looking for fileid " << file.parent << "  parent of fileid " << file.stat.st_ino);
      return r;
    }

      Statement oldNlinkStmt(this->conn_, CNS_DB, "SELECT nlink FROM Cns_file_metadata WHERE fileid = ? FOR UPDATE");
    oldNlinkStmt.bindParam(0, oldParent.stat.st_ino);
    oldNlinkStmt.execute();
    oldNlinkStmt.bindResult(0, &oldParent.stat.st_nlink);
    oldNlinkStmt.fetch();

    Statement oldNlinkUpdateStmt(this->conn_, CNS_DB, "UPDATE Cns_file_metadata\
    SET nlink = ?, mtime = UNIX_TIMESTAMP(), ctime = UNIX_TIMESTAMP()\
    WHERE fileid = ?");

    oldParent.stat.st_nlink--;
    oldNlinkUpdateStmt.bindParam(0, oldParent.stat.st_nlink);
    oldNlinkUpdateStmt.bindParam(1, oldParent.stat.st_ino);

    if (oldNlinkUpdateStmt.execute() == 0)
      throw DmException(DMLITE_SYSERR(DMLITE_INTERNAL_ERROR),
                        "Could not update the old parent nlink!");

      // Increment from new
      Statement newNlinkStmt(this->conn_, CNS_DB, "SELECT nlink FROM Cns_file_metadata WHERE fileid = ? FOR UPDATE");
    newNlinkStmt.bindParam(0, newParent.stat.st_ino);
    newNlinkStmt.execute();
    newNlinkStmt.bindResult(0, &newParent.stat.st_nlink);
    newNlinkStmt.fetch();

    Statement newNlinkUpdateStmt(this->conn_, CNS_DB, "UPDATE Cns_file_metadata\
    SET nlink = ?, mtime = UNIX_TIMESTAMP(), ctime = UNIX_TIMESTAMP()\
    WHERE fileid = ?");

    newParent.stat.st_nlink++;
    newNlinkUpdateStmt.bindParam(0, newParent.stat.st_nlink);
    newNlinkUpdateStmt.bindParam(1, newParent.stat.st_ino);

    if (newNlinkUpdateStmt.execute() == 0)
      throw DmException(DMLITE_SYSERR(DMLITE_INTERNAL_ERROR),
                        "Could not update the new parent nlink!");


    trans.Commit();
  }
  catch ( DmException e ) {
    return DmStatus(e);
  }

  DOMECACHE->wipeEntry(inode);
  
  Log(Logger::Lvl1, domelogmask, domelogname, "Exiting.  inode:" << inode << " dest:" << dest);
  return DmStatus();
}



DmStatus DomeMySql::addReplica(const Replica& replica)
{
  std::string  host;
  char         cstatus, ctype;

  Log(Logger::Lvl4, domelogmask, domelogname, " replica:" << replica.rfn);

  // Make sure fileid exists and is a regular file
  ExtendedStat s;
  DmStatus res = this->getStatbyFileid(s, replica.fileid);
  if (!res.ok())
    return res;

  if (!S_ISREG(s.stat.st_mode))
    return DmStatus(EINVAL,
                    SSTR("Inode " << replica.fileid << " is not a regular file"));

  // The replica should not exist already
  Replica tmp;
  res = this->getReplicabyRFN(tmp, replica.rfn);
  if(res.ok()) {
    throw DmException(EEXIST,
                      "Replica %s already registered", replica.rfn.c_str());
  }
  else if(res.code() != DMLITE_NO_SUCH_REPLICA) {
    return res;
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
  try {
    Statement statement(conn_, CNS_DB, "INSERT INTO Cns_file_replica\
    (fileid, nbaccesses,\
    ctime, atime, ptime, ltime,\
    r_type, status, f_type,\
    setname, poolname, host, fs, sfn, xattr)\
    VALUES\
    (?, 0,\
    UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), UNIX_TIMESTAMP(),\
    ?, ?, ?,\
    ?, ?, ?, ?, ?, ?)");

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
  }
  catch ( DmException e ) {
    return DmStatus(e);
  }

  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. replica:" << replica.rfn);
  return DmStatus();
}




DmStatus DomeMySql::updateReplica(const Replica& rdata)
{
  Log(Logger::Lvl4, domelogmask, domelogname, " rdata:" << rdata.rfn);

  // Update
  char status = static_cast<char>(rdata.status);
  char type   = static_cast<char>(rdata.type);

  try {
    Statement stmt(conn_, CNS_DB, "UPDATE Cns_file_replica\
    SET nbaccesses = ?, ctime = UNIX_TIMESTAMP(), atime = ?, ptime = ?, ltime = ?, \
    f_type = ?, status = ?, poolname = ?, \
    host = ?, fs = ?, sfn = ?, xattr = ?, setname = ?\
    WHERE rowid = ?");

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
  }
  catch ( DmException e ) {
    return DmStatus(e);
  }

  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. rdata:" << rdata.rfn);
  return DmStatus();
}






/// Delete a replica
int DomeMySql::delReplica(int64_t fileid, const std::string &rfn) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. fileid: '" << fileid << "' rfn: " << rfn);
  bool ok = true;
  long unsigned int nrows = 0;

  try {
    Statement stmt(conn_, CNS_DB,
                   "DELETE FROM Cns_file_replica "
                   "WHERE fileid = ? AND sfn = ?");

    stmt.bindParam(0, fileid);
    stmt.bindParam(1, rfn);

    if ( (nrows = stmt.execute() == 0) ) ok = false;
  }
  catch ( ... ) { ok = false; }

  if (!ok) {
    Err( domelogname, "Could not delete replica from DB. fileid: '" << fileid << "' rfn: " << rfn << " nrows: " << nrows);
    return 1;
  }

  Log(Logger::Lvl3, domelogmask, domelogname, "Replica deleted. fileid: '" << fileid << "' rfn: " << rfn
      << " nrows: " << nrows; );

  return 0;
}

int DomeMySql::addtoDirectorySize(int64_t fileid, int64_t increment) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. fileid: '" << fileid << "' increment: " << increment );
  bool ok = true;
  long unsigned int nrows;

  try {
    Statement stmt(conn_, CNS_DB,
                   "UPDATE Cns_file_metadata\
                    SET filesize = filesize + ( ? )\
                    WHERE fileid = ?");

    stmt.bindParam(0, increment);
    stmt.bindParam(1, fileid);

    if( (nrows = stmt.execute() == 0) )
      ok = false;
  }
  catch ( ... ) { ok = false; }

  if (!ok) {
    Err( domelogname, "Could not update directory size from DB. s_token: '" << fileid <<
      "' increment: " << increment << " nrows: " << nrows );
    return 1;
  }

  dmlite::ExtendedStat xstat;
  DomeMySql sql;
  dmlite::DmStatus ret;
  
  ret = sql.getStatbyFileid(xstat, fileid);
  if (ret.ok()) {
    xstat.stat.st_size += increment; 
    DOMECACHE->pushXstatInfo(xstat, DomeFileInfo::Ok);
  }
  
  Log(Logger::Lvl3, domelogmask, domelogname, "Directory size updated. fileid: '" << fileid <<
  "' increment: " << increment << " nrows: " << nrows );

  return 0;
}

















DmStatus DomeMySql::getStatbyLFN(dmlite::ExtendedStat &meta, const std::string path, bool followSym) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. lfn: '" << path << "'" );


  // Split the path always assuming absolute
  std::vector<std::string> components = Url::splitPath(path);

  // Iterate starting from absolute root (parent of /) (0)
  uint64_t     parent       = 0;
  unsigned     symLinkLevel = 0;
  std::string  c;

  meta = ExtendedStat(); // ensure it's clean

  // We process only absolute paths here
  if (path[0] != '/' ) {

    // return an error!
    return DmStatus(ENOTDIR, SSTR("'" << meta.name << "' is not an absolute path."));
  }

  DmStatus st = getStatbyParentFileid(meta, 0, "/");
  if(!st.ok()) return st;

  parent = meta.stat.st_ino;
  components.erase(components.begin());

  for (unsigned i = 0; i < components.size(); ) {
    // Check that the parent is a directory first
    if (!S_ISDIR(meta.stat.st_mode) && !S_ISLNK(meta.stat.st_mode))
      return DmStatus(ENOTDIR, SSTR("'" << meta.name << "' is not a directory, and is referenced by '" << path << "'. Internal DB error."));

    // Pop next component
    c = components[i];

    // Stay here
    if (c == ".") {
      // Nothing
    }
    // Up one level
    else if (c == "..") {
      parent = meta.parent;
      DmStatus st = getStatbyFileid(meta, parent);
      if(!st.ok()) return st;
    }
    // Regular entry
    else {
      // Stat, but capture ENOENT to improve error code
      DmStatus st = getStatbyParentFileid(meta, parent, c);
      if(!st.ok()) {
        if(st.code() != ENOENT) return st;

        while (i < components.size()) {
          components.pop_back();
        }

        //re-add / at the beginning
        components.insert( components.begin(),std::string(1,'/'));
        return DmStatus(ENOENT, "Entry '%s' not found under '%s'",
                        c.c_str(), Url::joinPath(components).c_str());
      }

      // Symbolic link!, follow that instead
      if (S_ISLNK(meta.stat.st_mode) && followSym) {
        SymLink link;
        DmStatus res = readLink(link, meta.stat.st_ino);
        if (res.code() != DMLITE_SUCCESS)
          return res;

        ++symLinkLevel;
        if (symLinkLevel > 16) {
          return DmStatus(DMLITE_SYSERR(ELOOP),
                          "Symbolic links limit exceeded: > %d",
                          16);
        }

        // We have the symbolic link now. Split it and push
        // into the component
        std::vector<std::string> symPath = Url::splitPath(link.link);

        for (unsigned j = i + 1; j < components.size(); ++j)
          symPath.push_back(components[j]);

        components.swap(symPath);
        i = 0;

        // If absolute, need to reset parent
        if (link.link[0] == '/') {
          parent = 0;
          meta.stat.st_mode = S_IFDIR | 0555;
        }
        // If not, meta has the symlink data, which isn't nice.
        // Stat the parent again!
        else {
          DmStatus st = getStatbyFileid(meta, meta.parent);
          if(!st.ok()) return st;
        }

        continue; // Jump directly to the beginning of the loop
      }
      // Next one!
      else {
        parent = meta.stat.st_ino;
      }
    }
    ++i; // Next in array
  }

  checksums::fillChecksumInXattr(meta);

  Log(Logger::Lvl3, domelogmask, domelogname, "Stat-ed '" << path << "' sz: " << meta.stat.st_size);
  return DmStatus();

}




dmlite::DmStatus DomeMySql::readLink(SymLink &link, int64_t fileid)
{
  Log(Logger::Lvl4, domelogmask, domelogname, " fileid:" << fileid);

  try {
    Statement stmt(conn_, CNS_DB,
                   "SELECT fileid, linkname FROM Cns_symlinks WHERE fileid = ?");

    char      clink[4096];

    memset(clink, 0, sizeof(clink));
    stmt.bindParam(0, fileid);
    stmt.execute();

    stmt.bindResult(0, &link.inode);
    stmt.bindResult(1, clink, sizeof(clink), 0);

    if (!stmt.fetch())
      return DmStatus(ENOENT, "Link %ld not found", fileid);

    link.link = clink;

    }
    catch ( ... ) {
      Err(domelogname, " Exception while reading symlink " << fileid);
      return DmStatus(ENOENT, "Cannot fetch link %ld", fileid);
    }
    Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. fileid:" << fileid << " --> " << link.link);
  return DmStatus();
}





/// Extended stat for inodes
DmStatus DomeMySql::getStatbyFileid(dmlite::ExtendedStat& xstat, int64_t fileid) {
  Log(Logger::Lvl4, domelogmask, domelogname, " fileid:" << fileid);
  
  // Get the correponding item from the cache, it can be empty or pending. It's unlocked
  boost::shared_ptr <DomeFileInfo > dfi = DOMECACHE->getFileInfoOrCreateNewOne(fileid);
  int done = 0;
  {
    boost::unique_lock<boost::mutex> l(*dfi);
    
    
    if (dfi->status_statinfo == DomeFileInfo::NotFound)
      return DmStatus(ENOENT, SSTR("fileid " << fileid << "' not found (cached)"));
    
    done = dfi->waitStat(l);
    if(done) {
      xstat = dfi->statinfo;
    }
    
  }
  
  if (!done) {
    // The cache entry was empty and not pending. Now it's pending and we have to fill it with the query
    
    try {
      Statement    stmt(conn_, CNS_DB,
                        "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
                        filesize, atime, mtime, ctime, fileclass, status,\
                        csumtype, csumvalue, acl, xattr\
                        FROM Cns_file_metadata\
                        WHERE fileid = ?");
      CStat        cstat;
      xstat = ExtendedStat();
      
      stmt.bindParam(0, fileid);
      stmt.execute();
      
      bindMetadata(stmt, &cstat);
      
      if (!stmt.fetch()) {
        
        boost::unique_lock<boost::mutex> l(*dfi);
        dfi->status_statinfo = DomeFileInfo::NotFound;
        dfi->signalSomeUpdate();
        return DmStatus(ENOENT, SSTR("fileid "<< fileid << " not found"));
      }
      
      dumpCStat(cstat, &xstat);
    }
    catch ( ... ) {
      Err(domelogname, " Exception while reading stat of fileid " << fileid);
      
      boost::unique_lock<boost::mutex> l(*dfi);
      dfi->status_statinfo = DomeFileInfo::NotFound;
      dfi->signalSomeUpdate();
      
      return DmStatus(EINVAL, SSTR(" Exception while reading stat of fileid " << fileid));
    }
    
    // Now insert the new stat info into the cache and signal it.
    {
      boost::unique_lock<boost::mutex> l(*dfi);
      dfi->statinfo = xstat;
      dfi->status_statinfo = DomeFileInfo::Ok;
      dfi->signalSomeUpdate();
    }
  }

  
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. fileid:" << fileid << " name:" << xstat.name << " sz:" << xstat.size());
  return DmStatus();
}




// Extended stat for replica file names in rfio syntax
DmStatus DomeMySql::getStatbyRFN(dmlite::ExtendedStat &xstat, std::string rfn) {
  Log(Logger::Lvl4, domelogmask, domelogname, " rfn:" << rfn);
  try {
    Statement    stmt(conn_, CNS_DB,
                      "SELECT m.fileid, m.parent_fileid, m.guid, m.name, m.filemode, m.nlink, m.owner_uid, m.gid,\
                      m.filesize, m.atime, m.mtime, m.ctime, m.fileclass, m.status,\
                      m.csumtype, m.csumvalue, m.acl, m.xattr\
                      FROM Cns_file_metadata m, Cns_file_replica r\
                      WHERE r.sfn = ? AND r.fileid = m.fileid");
    CStat        cstat;
    xstat = ExtendedStat();

    stmt.bindParam(0, rfn);
    stmt.execute();

    bindMetadata(stmt, &cstat);

    if (!stmt.fetch())
      return DmStatus(ENOENT, SSTR("replica '" << rfn << "' not found"));

    dumpCStat(cstat, &xstat);
  }
  catch ( ... ) {
    Err(domelogname, " Exception while reading stat of rfn " << rfn);
    return DmStatus(EINVAL, SSTR(" Exception while reading stat of rfn " << rfn));
  }
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. rfn:" << rfn << " name:" << xstat.name << " sz:" << xstat.stat.st_size);
  return DmStatus();
}


// Extended stat for replica file names in rfio syntax
DmStatus DomeMySql::getReplicabyRFN(dmlite::Replica &r, std::string rfn) {
  Log(Logger::Lvl4, domelogmask, domelogname, " rfn:" << rfn);
  try {
    Statement    stmt(conn_, CNS_DB,
                      "SELECT rowid, fileid, nbaccesses,\
                      atime, ptime, ltime,\
                      status, f_type, setname, poolname, host, fs, sfn, COALESCE(xattr, '')\
                      FROM Cns_file_replica\
                      WHERE sfn = ?");


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
      return DmStatus(DMLITE_NO_SUCH_REPLICA, "Replica '%s' not found", rfn.c_str());

    r.rfn           = crfn;
    r.server        = cserver;
    r.setname       = std::string(setnm);
    r.status        = static_cast<Replica::ReplicaStatus>(cstatus);
    r.type          = static_cast<Replica::ReplicaType>(ctype);
    r.deserialize(cmeta);

    r["pool"]       = std::string(cpool);
    r["filesystem"] = std::string(cfilesystem);
  }
  catch ( ... ) {
    Err(domelogname, " Exception while reading stat of rfn " << rfn);
    return DmStatus(EINVAL, SSTR(" Exception while reading stat of rfn " << rfn));
  }

  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. repl:" << r.rfn);
  return DmStatus();
}


// Extended stat for replica file names in rfio syntax
DmStatus DomeMySql::getReplicabyId(dmlite::Replica &r, int64_t repid) {
  Log(Logger::Lvl4, domelogmask, domelogname, " repid:" << repid);
  try {
    Statement    stmt(conn_, CNS_DB,
                      "SELECT rowid, fileid, nbaccesses,\
                      atime, ptime, ltime,\
                      status, f_type, setname, poolname, host, fs, sfn, COALESCE(xattr, '')\
                      FROM Cns_file_replica\
                      WHERE rowid = ?");


    stmt.bindParam(0, repid);

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
      return DmStatus(DMLITE_NO_SUCH_REPLICA, "Replica %lld not found", repid);

    r.rfn           = crfn;
    r.server        = cserver;
    r.setname       = std::string(setnm);
    r.status        = static_cast<Replica::ReplicaStatus>(cstatus);
    r.type          = static_cast<Replica::ReplicaType>(ctype);
    r.deserialize(cmeta);

    r["pool"]       = std::string(cpool);
    r["filesystem"] = std::string(cfilesystem);
  }
  catch ( ... ) {
    Err(domelogname, " Exception while reading stat of replica id " << repid);
    return DmStatus(EINVAL, SSTR(" Exception while reading stat of replica id " << repid));
  }

  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. repl:" << r.rfn);
  return DmStatus();
}



DmStatus DomeMySql::getReplicas(std::vector<Replica> &reps, std::string lfn) {
  ExtendedStat meta;
  // Get the directory
  DmStatus st = getStatbyLFN(meta, lfn);
  if (!st.ok())
    return st;

  return getReplicas(reps, meta.stat.st_ino);
}

DmStatus DomeMySql::getReplicas(std::vector<Replica> &reps, ino_t inode)
{
  Replica   replica;
  char      cpool[512];
  char      setnm[512];
  char      cserver[512];
  char      cfilesystem[512];
  char      crfn[4096];
  char      cmeta[4096];
  char      ctype, cstatus;
  int i = 0;
  
  Log(Logger::Lvl4, domelogmask, domelogname, " inode:" << inode);
  
  // Get the correponding item from the cache, it can be empty or pending. It's unlocked
  boost::shared_ptr <DomeFileInfo > dfi = DOMECACHE->getFileInfoOrCreateNewOne(inode);
  int done = 0;
  {
    boost::unique_lock<boost::mutex> l(*dfi);
    
    if (dfi->status_statinfo == DomeFileInfo::NotFound)
      return DmStatus(ENOENT, SSTR("fileid " << inode << "' not found (cached)"));
    
    if (dfi->status_locations == DomeFileInfo::NotFound)
      return DmStatus(ENOENT, SSTR("fileid " << inode << "' replicas not found (cached)"));
    
    done = dfi->waitLocations(l);
    if(done) {
      reps = dfi->replicas;
    }
    
  }
  
  if (!done) {
    
    // MySQL statement
    try {
      Statement stmt(conn_, CNS_DB, "SELECT rowid, fileid, nbaccesses,\
      atime, ptime, ltime,\
      status, f_type, setname, poolname, host, fs, sfn, COALESCE(xattr, '')\
      FROM Cns_file_replica\
      WHERE fileid = ?");
      
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
      
      reps.clear();
      
      // Fetch
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
        
        reps.push_back(replica);
        ++i;
      };
      
      if (!i)
        return DmStatus(DMLITE_NO_SUCH_REPLICA, SSTR("No replicas for fileid " << inode));
    }
    catch ( ... ) {
      Err(domelogname, " Exception while getting replicas of fileid " << inode);
      
      
      boost::unique_lock<boost::mutex> l(*dfi);
      dfi->status_locations = DomeFileInfo::NotFound;
      dfi->signalSomeUpdate();
      
      return DmStatus(EINVAL, SSTR(" Exception while getting replicas of fileid " << inode));
    }
    
    // Now insert the new replica info into the cache and signal it.
    {
      boost::unique_lock<boost::mutex> l(*dfi);
      dfi->replicas = reps;
      dfi->status_locations = DomeFileInfo::Ok;
      dfi->signalSomeUpdate();
    }
    
  }
  
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. inode:" << inode << " nrepls:" << i);
  return DmStatus();
}

/// Extended stat for inodes
DmStatus DomeMySql::getStatbyParentFileid(dmlite::ExtendedStat& xstat, int64_t fileid, std::string name) {
  Log(Logger::Lvl4, domelogmask, domelogname, " parent_fileid:" << fileid << " name: '" << name << "'");
  
  // Get the correponding item from the cache, it can be empty or pending. It's unlocked
  boost::shared_ptr <DomeFileInfo > dfi = DOMECACHE->getFileInfoOrCreateNewOne(fileid, name);
  int done = 0;
  {
    boost::unique_lock<boost::mutex> l(*dfi);
    
    if (dfi->status_statinfo == DomeFileInfo::NotFound)
      return DmStatus(ENOENT, SSTR("file " << fileid << ":'" << name << "' not found (cached)"));
    
    done = dfi->waitStat(l);
    if(done) {
      xstat = dfi->statinfo;
    }
    
  }
  
  
  if (!done) {
    // The cache entry was empty and not pending. Now it's pending and we have to fill it with the query
    
    try {
      Statement    stmt(conn_, CNS_DB,
                        "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
                        filesize, atime, mtime, ctime, fileclass, status,\
                        csumtype, csumvalue, acl, xattr\
                        FROM Cns_file_metadata\
                        WHERE parent_fileid = ? AND name = ?");
      CStat        cstat;
      xstat = ExtendedStat();
      
      stmt.bindParam(0, fileid);
      stmt.bindParam(1, name);
      stmt.execute();
      
      bindMetadata(stmt, &cstat);
      
      if (!stmt.fetch()){
        
        boost::unique_lock<boost::mutex> l(*dfi);
        dfi->status_statinfo = DomeFileInfo::NotFound;
        dfi->signalSomeUpdate();
        return DmStatus(ENOENT, SSTR("file " << fileid << ":'" << name << "' not found"));
      }
      
      dumpCStat(cstat, &xstat);
    }
    catch ( ... ) {
      Err(domelogname, " Exception while reading stat of parent_fileid " << fileid);
      
      boost::unique_lock<boost::mutex> l(*dfi);
      dfi->status_statinfo = DomeFileInfo::NotFound;
      dfi->signalSomeUpdate();
      
      return DmStatus(ENOENT, SSTR(" Exception while reading stat of parent_fileid " << fileid));
    }
    
    
    // Now insert the new stat info into the cache and signal it.
    {
      boost::unique_lock<boost::mutex> l(*dfi);
      dfi->statinfo = xstat;
      dfi->status_statinfo = DomeFileInfo::Ok;
      dfi->signalSomeUpdate();
    }
  }
  
  
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. parent_fileid:" << fileid << " name:" << name << " sz:" << xstat.size());
  return DmStatus();
}







DmStatus DomeMySql::setSize(ino_t inode, int64_t filesize) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. inode: " << inode << " size: " << filesize );

  long unsigned int nrows = 0;


  try {
    // Just do the query on the fileid
    Statement stmt(conn_, CNS_DB,
                   "UPDATE Cns_file_metadata SET filesize = ?, ctime = UNIX_TIMESTAMP() WHERE fileid = ?");

    stmt.bindParam(0, filesize);
    stmt.bindParam(1, inode);

    // If no rows are affected then error
    if ( (nrows = stmt.execute() == 0) )
      return DmStatus(EINVAL, SSTR("Cannot set filesize for inode: " << inode << " nrows: " << nrows));

  }
  catch (DmException e) {
    Err(domelogname, " Exception while setting filesize for fileid: " << inode << "err: '" << e.what());
    return DmStatus(EINVAL, SSTR(" Exception while setting filesize for fileid: " << inode << "err: '" << e.what()));
  }

  DOMECACHE->wipeEntry(inode);
  Log(Logger::Lvl4, domelogmask, domelogname, "Exiting. inode: " << inode << " size: " << filesize );
  return DmStatus();
}










DmStatus DomeMySql::unlink(ino_t inode)
{
  Log(Logger::Lvl4, domelogmask, domelogname, " inode:" << inode);

  // Get file metadata
  ExtendedStat file;
  DmStatus r = this->getStatbyFileid(file, inode);
  if (!r.ok()) return r;
  

  // Non empty directories can not be removed with this method
  if (S_ISDIR(file.stat.st_mode) && file.stat.st_nlink > 0)
    return DmStatus(EISDIR, SSTR("Inode " << inode << " is a directory and it is not empty"));

  // Get the parent
  ExtendedStat parent;
  r = this->getStatbyFileid(parent, file.parent);
  if (!r.ok()) return r;

  try {
    // All preconditions are good! Start transaction.
    // Start transaction
    DomeMySqlTrans trans(this);

    
    {
      // Scope to make sure that the local objects that involve mysql
      // are destroyed before the transaction is closed
      Log(Logger::Lvl4, domelogmask, domelogname, "Deleting symlinks, comments, replicas.  inode:" << inode);

      // Remove associated symlink
      Statement delSymlink(conn_, CNS_DB, "DELETE FROM Cns_symlinks WHERE fileid = ?");
      delSymlink.bindParam(0, inode);
      delSymlink.execute();

      // Remove associated comments
      Statement delComment(conn_, CNS_DB, "DELETE FROM Cns_user_metadata WHERE u_fileid = ?");
      delComment.bindParam(0, inode);
      delComment.execute();

      // Remove replicas
      Statement delReplicas(conn_, CNS_DB, "DELETE FROM Cns_file_replica\
      WHERE fileid = ?");
      delReplicas.bindParam(0, inode);
      delReplicas.execute();

      // Remove file itself
      Log(Logger::Lvl4, domelogmask, domelogname, "Deleting file entry.  inode:" << inode);

      Statement delFile(this->conn_, CNS_DB, "DELETE FROM Cns_file_metadata WHERE fileid = ?");
      delFile.bindParam(0, inode);
      delFile.execute();

      // Decrement parent nlink
      Log(Logger::Lvl4, domelogmask, domelogname, "Fixing parent nlink.  inode:" << inode << " parent: " << parent.stat.st_ino);

      Statement nlinkStmt(this->conn_, CNS_DB, "SELECT nlink FROM Cns_file_metadata WHERE fileid = ? FOR UPDATE");
      nlinkStmt.bindParam(0, parent.stat.st_ino);
      nlinkStmt.execute();
      nlinkStmt.bindResult(0, &parent.stat.st_nlink);
      nlinkStmt.fetch();

      Statement nlinkUpdate(this->conn_, CNS_DB, "UPDATE Cns_file_metadata\
      SET nlink = ?, mtime = UNIX_TIMESTAMP(), ctime = UNIX_TIMESTAMP()\
      WHERE fileid = ?");
      parent.stat.st_nlink--;
      nlinkUpdate.bindParam(0, parent.stat.st_nlink);
      nlinkUpdate.bindParam(1, parent.stat.st_ino);
      nlinkUpdate.execute();

    }
    // Done!

    // Commit the local trans object
    trans.Commit();
  }
  catch (DmException e) {
    return DmStatus(EINVAL, SSTR("Cannot unlink fileid: " << inode << "err: '" << e.what()));
  }


  
  DOMECACHE->wipeEntry(inode, file.parent, file.name);

  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting.  inode:" << inode);
  return DmStatus();
}



DmStatus DomeMySql::updateExtendedAttributes(ino_t inode, const ExtendedStat& attr)
{
  Log(Logger::Lvl4, domelogmask, domelogname, " inode:" << inode << " nattrs:" << attr.size() );

  try {
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
      Log(Logger::Lvl4, domelogmask, domelogname, " inode:" << inode << " contextually setting short checksum:" << shortCsumType << ":" << csumValue );
      Statement stmt(conn_, CNS_DB, "UPDATE Cns_file_metadata\
      SET xattr = ?, csumtype = ?, csumvalue = ?\
      WHERE fileid = ?");

      stmt.bindParam(0, attr.serialize());
      stmt.bindParam(1, shortCsumType);
      stmt.bindParam(2, csumValue);
      stmt.bindParam(3, inode);

      stmt.execute();
    }
    else {

      Statement stmt(conn_, CNS_DB, "UPDATE Cns_file_metadata\
      SET xattr = ?\
      WHERE fileid = ?");

      stmt.bindParam(0, attr.serialize());
      stmt.bindParam(1, inode);

      stmt.execute();
    }

  }
  catch ( ... ) {
    return DmStatus(EINVAL, SSTR("Cannot update xattrs for fileid: " << inode << " xattrs: '" << attr.serialize() << "'"));
  }
  
  DOMECACHE->pushXstatInfo(attr, DomeFileInfo::Ok);
  
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. inode:" << inode << " nattrs:" << attr.size() );
  return DmStatus();
}



DmStatus DomeMySql::symlink(ino_t inode, const std::string &link)
{
  Log(Logger::Lvl4, domelogmask, domelogname, " lnk:" << link);

  try {
    Statement stmt(conn_, CNS_DB, "INSERT INTO Cns_symlinks\
    (fileid, linkname)\
    VALUES\
    (?, ?)");

    stmt.bindParam(0, inode);
    stmt.bindParam(1, link);

    stmt.execute();

  }
  catch ( ... ) {
    return DmStatus(EINVAL, SSTR("Cannot symlink fileid: " << inode << " to link '" << link << "'"));
  }

  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting.  lnk:" << link);

  return DmStatus();
}


DmStatus DomeMySql::getParent(ExtendedStat &statinfo,
                              const std::string& path,
                              std::string &parentPath,
                              std::string &name)
{
  if (path.empty())
    return DmStatus(EINVAL, "Empty path");

  std::vector<std::string> components = Url::splitPath(path);

  name = components.back();
  components.pop_back();

  parentPath = Url::joinPath(components);
  if(parentPath.empty()) parentPath = "/";

  // Get the files now
  return this->getStatbyLFN(statinfo, parentPath);

}



DmStatus DomeMySql::rename(ino_t inode, const std::string& name) {
  Log(Logger::Lvl4, domelogmask, domelogname, " inode:" << inode << " name:" << name);

  try {
    Statement changeNameStmt(conn_, CNS_DB, "UPDATE Cns_file_metadata\
    SET name = ?, ctime = UNIX_TIMESTAMP()\
    WHERE fileid = ?");

    changeNameStmt.bindParam(0, name);
    changeNameStmt.bindParam(1, inode);

    if (changeNameStmt.execute() == 0)
      return DmStatus(DMLITE_SYSERR(DMLITE_INTERNAL_ERROR), SSTR("Could not change the name of inode " <<
      inode << " name '" << name << "'"));


  }
  catch ( ... ) {
    return DmStatus(EINVAL, SSTR("Cannot rename fileid: " << inode << " to name '" << name << "'"));
  }

  DOMECACHE->wipeEntry(inode);
  
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting.  inode:" << inode << " name:" << name);

  return DmStatus();
}


dmlite::DmStatus DomeMySql::utime(ino_t inode, const utimbuf *buf) {
  Log(Logger::Lvl4, domelogmask, domelogname, " inode:" << inode);

  try {
    // If NULL, current time.
    struct utimbuf internal;
    if (buf == NULL) {
      buf = &internal;
      internal.actime  = time(NULL);
      internal.modtime = time(NULL);
    }

    // Change

    Statement stmt(conn_, CNS_DB,     "UPDATE Cns_file_metadata\
    SET atime = ?, mtime = ?, ctime = UNIX_TIMESTAMP()\
    WHERE fileid = ?");
    stmt.bindParam(0, buf->actime);
    stmt.bindParam(1, buf->modtime);
    stmt.bindParam(2, inode);

    stmt.execute();

  }
  catch ( ... ) {
    return DmStatus(EINVAL, SSTR("Cannot set time to fileid: " << inode));
  }

  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. inode:" << inode);

  return DmStatus();
}




DmStatus DomeMySql::opendir(DomeMySqlDir *&dir, const std::string& path) {
  Log(Logger::Lvl4, domelogmask, domelogname, " path: '" << path << "'");

  dir = NULL;
  ExtendedStat meta;

  Log(Logger::Lvl4, domelogmask, domelogname, " path:" << path);

  // Get the directory
  DmStatus st = getStatbyLFN(meta, path);
  if (!st.ok())
    return st;

  if (!S_ISDIR(meta.stat.st_mode))
    return DmStatus(ENOTDIR, SSTR("Not a directory '" << path << "'"));

  // Create the handle
  dir = new DomeMySqlDir();
  dir->dir = meta;
  dir->path = path;

  try {
    dir->stmt = new Statement(conn_, CNS_DB,
                              "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
                              filesize, atime, mtime, ctime, fileclass, status,\
                              csumtype, csumvalue, acl, xattr\
                              FROM Cns_file_metadata \
                              WHERE parent_fileid = ?\
                              ORDER BY name ASC");

    dir->stmt->bindParam(0, meta.stat.st_ino);
    dir->stmt->execute();
    bindMetadata(*dir->stmt, &dir->cstat);

    dir->eod = !dir->stmt->fetch();
  }
  catch (...) {
    Err(domelogname, " Exception while opening dir '" << path << "'");
    delete dir;
    dir = NULL;
    return DmStatus(EINVAL, SSTR(" Exception while opening dir '" << path << "'"));
  }

  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. path: '" << path << "'");

  return DmStatus();
}


DmStatus DomeMySql::closedir(DomeMySqlDir *&dir) {
  if (!dir) {
    Err(domelogname, " Trying to close a NULL dir. Not fatal, quite ugly.");
    return DmStatus();
  }

  std::string logpath = dir->path;
  int32_t entries = dir->entry;
  Log(Logger::Lvl4, domelogmask, domelogname, "Closing dir '" << logpath << "'");

  delete dir;
  dir = NULL;

  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. Read entries: " << entries <<
      " dir: '" << logpath << "'");
  return DmStatus();
}


ExtendedStat* DomeMySql::readdirx(DomeMySqlDir *&dir) {
  if (!dir) {
    Err(domelogname, " Trying to read a NULL dir.");
    return NULL;
  }

  std::string logpath = dir->path;
  Log(Logger::Lvl4, domelogmask, domelogname, "Reading dir '" << logpath << "'");

  try {

    if (!dir->eod) {
      dir->entry++;

      dumpCStat(dir->cstat, &dir->current);
      dir->eod = !dir->stmt->fetch();

      Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. item:" << dir->current.name);
      return &dir->current;
    }

  }
  catch ( DmException e ) {
    Err(domelogname, " Exception while reading dir '" << dir->path << "' err: " << e.code() <<
      ":" << e.what() );
    delete dir;
    dir = NULL;
  }
  return NULL;
}




DmStatus DomeMySql::setChecksum(const ino_t fid, const std::string &csumtype, const std::string &csumvalue) {

  // This is a convenience function, which could be overridden, but normally should not
  // We translate a legacy checksum (e.g. AD for adler32)  into the proper extended xattrs to be set
  // (e.g. checksum.adler32)
  // We can also pass a long checksum name (e.g. checksum.adler32)

  Log(Logger::Lvl4, domelogmask, domelogname, " fileid: " << fid << " csumtype:" << csumtype << " csumvalue:" << csumvalue);

  ExtendedStat ckx;
  DmStatus ret = this->getStatbyFileid(ckx, fid);
  if (!ret.ok())
    return ret;

  std::string k = csumtype;

  // If it looks like a legacy chksum then try to xlate its name
  if (csumtype.length() == 2)
    k = checksums::fullChecksumName(csumtype);

  if (!checksums::isChecksumFullName(k))
    return DmStatus(EINVAL, SSTR("'" << csumtype << "' is not a valid checksum type."));

  if (csumvalue.length() == 0)
    return DmStatus(EINVAL, SSTR("'" << csumvalue << "' is not a valid checksum value."));


  ckx[k] = csumvalue;
  updateExtendedAttributes(fid, ckx);

  
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. fileid: " << fid);

  return DmStatus();

}
