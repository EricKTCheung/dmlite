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

#include "boost/thread.hpp"

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

  Log(Logger::Lvl3, domelogmask, domelogname, "Directory size updated. fileid: '" << fileid <<
  "' increment: " << increment << " nrows: " << nrows );

  return 0;
}

















DmStatus DomeMySql::getStatbyLFN(dmlite::ExtendedStat &meta, std::string path, bool followSym) {
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
    return DmStatus(ENOTDIR, "'" + meta.name + "' is not an absolute path.");
  }

  DmStatus st = getStatbyParentFileid(meta, 0, "/");
  if(!st.ok()) return st;

  parent = meta.stat.st_ino;
  components.erase(components.begin());

  for (unsigned i = 0; i < components.size(); ) {
    // Check that the parent is a directory first
    if (!S_ISDIR(meta.stat.st_mode) && !S_ISLNK(meta.stat.st_mode))
      return DmStatus(ENOTDIR, "'" + meta.name + "' is not a directory, and is referenced by '" + path + "'. Internal DB error.");

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




dmlite::DmStatus DomeMySql::readLink(SymLink link, int64_t fileid)
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

  if (!stmt.fetch())
    return DmStatus(ENOENT, fileid + " not found");

  dumpCStat(cstat, &xstat);
  }
  catch ( ... ) {
    Err(domelogname, " Exception while reading stat of fileid " << fileid);
    return DmStatus(EINVAL, " Exception while reading stat of fileid " + fileid);
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
      return DmStatus(ENOENT, "replica '" + rfn + "' not found");

    dumpCStat(cstat, &xstat);
  }
  catch ( ... ) {
    Err(domelogname, " Exception while reading stat of rfn " << rfn);
    return DmStatus(EINVAL, " Exception while reading stat of rfn " + rfn);
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
      return DmStatus(DMLITE_NO_SUCH_REPLICA, "Replica %s not found", rfn.c_str());

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
    return DmStatus(EINVAL, " Exception while reading stat of rfn " + rfn);
  }

  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. repl:" << r.rfn);
  return DmStatus();
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
    
    
  }
  catch ( ... ) {
    Err(domelogname, " Exception while getting replicas of fileid " << inode);
    return DmStatus(EINVAL, SSTR(" Exception while getting replicas of fileid " << inode));
  }
  
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. inode:" << inode << " nrepls:" << i);
  return DmStatus();
}

/// Extended stat for inodes
DmStatus DomeMySql::getStatbyParentFileid(dmlite::ExtendedStat& xstat, int64_t fileid, std::string name) {
  Log(Logger::Lvl4, domelogmask, domelogname, " parent_fileid:" << fileid << " name: '" << name << "'");
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

    if (!stmt.fetch())
      return DmStatus(ENOENT, SSTR(fileid << " not found"));

    dumpCStat(cstat, &xstat);
  }
  catch ( ... ) {
    Err(domelogname, " Exception while reading stat of parent_fileid " << fileid);
    return DmStatus(ENOENT, SSTR(" Exception while reading stat of parent_fileid " << fileid));
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
      
      // Remove file itself
      Statement delFile(this->conn_, CNS_DB, "DELETE FROM Cns_file_metadata WHERE fileid = ?");
      delFile.bindParam(0, inode);
      delFile.execute();
      
      // Decrement parent nlink
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
  
  Log(Logger::Lvl4, domelogmask, domelogname, "Deleting symlinks, comments, replicas.  inode:" << inode);
  
  try {
    // Scope to make sure that the local objects that involve mysql
    // are destroyed before the transaction is closed
    
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
    
    
  }
  catch ( DmException e ) {
    return DmStatus(EINVAL, SSTR("Cannot remove symlinks, comments or replicas of fileid: " << inode << "err: '" << e.what()));
  }
  
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting.  inode:" << inode);
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
