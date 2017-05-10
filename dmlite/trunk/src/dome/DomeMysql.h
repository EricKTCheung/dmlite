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



/** @file   DomeMySQL.h
 * @brief  A helper class that wraps DomeMySQL stuff
 * @author Fabrizio Furano
 * @date   Jan 2016
 */

#ifndef DOMEMYSQL_H
#define DOMEMYSQL_H

#include <string>
#include <mysql/mysql.h>
#include "status.h"
#include "inode.h"
#include "DomeMetadataCache.hh"


class DomeStatus;
class DomeQuotatoken;
class DomeFsInfo;
class DomeGroupInfo;
class DomeUserInfo;
class DomeMySqlDir;






class DomeMySql {
public:

  /// The db conn string is not needed, as the mysqlholder should
  /// be configured before this class
  /// Creating an instance of DomeMySql acquires a mysql connection and keeps it
  /// until the destruction
  DomeMySql();
  virtual ~DomeMySql();

  static void configure(std::string host, std::string username, std::string password, int port, int poolsize);
  /// Transaction control.
  /// To have the scoped behaviour the DomeMySqlTrans can be used
  int begin();
  int rollback();
  int commit();

  // ------------------------------------------
  // ---------------- Functions for internal DOME usage

  /// Loads spaces and quotas into the given status. Thread safe.
  int getSpacesQuotas(DomeStatus &st);

  /// Loads groups into the given DOME status. Thread safe.
  int getGroups(DomeStatus &st);
  /// Loads users into the given DOME status. Thread safe.
  int getUsers(DomeStatus &st);
  
  /// Load from the DB, matching the given poolname and path
  int getQuotaTokenByKeys(DomeQuotatoken &qtk);
  
  /// Load the defined pools
  int getPools(DomeStatus &st);
  
  /// Loads the defined filesystems, parses them into server names, etc. into the status
  int getFilesystems(DomeStatus &st);

  // ------------------------------------------
  // ------------------ dmlite authn functions
  /// Loads the groups into the given vector
  dmlite::DmStatus getGroupsVec(std::vector<DomeGroupInfo> &groups);
  /// Loads the users into the given vector
  dmlite::DmStatus getUsersVec(std::vector<DomeUserInfo> &users);
  
  /// Get a group by name
  dmlite::DmStatus getGroupbyName(DomeGroupInfo &grp, const std::string& groupName);
  /// Get a group by name
  dmlite::DmStatus getGroupbyGid(DomeGroupInfo &grp, gid_t gid);
  /// Add a new group
  dmlite::DmStatus newGroup(DomeGroupInfo &group, const std::string& gname);
  /// Add a new user
  dmlite::DmStatus newUser(DomeUserInfo &user, const std::string& uname);
  
  dmlite::DmStatus updateUser(const DomeUserInfo& user);
  dmlite::DmStatus deleteUser(const std::string& userName);
  dmlite::DmStatus getUser(DomeUserInfo &user, const std::string& username);
  dmlite::DmStatus getUser(DomeUserInfo &user, const int uid);
  dmlite::DmStatus updateGroup(const DomeGroupInfo& group);
  dmlite::DmStatus deleteGroup(const std::string& groupName);
  // ----------------------------------------
  

  
  // --------------------------------------------
  // ---------- Namespace functions
  
  /// Add a new namespace entity. Fills the new fileid directly into the stat struct
  dmlite::DmStatus create(dmlite::ExtendedStat& nf);
  dmlite::DmStatus makedir(const dmlite::ExtendedStat &parent, std::string dname, mode_t mode, int uid, int gid);
  dmlite::DmStatus createfile(const dmlite::ExtendedStat &parent, std::string fname, mode_t mode, int uid, int gid);
  dmlite::DmStatus getComment(std::string &comment, ino_t inode);
  dmlite::DmStatus setComment(ino_t inode, const std::string& comment);
  
  /// Extended stat for logical file names
  dmlite::DmStatus getStatbyLFN(dmlite::ExtendedStat &st, const std::string lfn, bool followSym = false);
  /// Get information about the parent directory
  dmlite::DmStatus getParent(dmlite::ExtendedStat &statinfo,
                                const std::string& path,
                                std::string &parentPath,
                                std::string &name);
  
  /// Sets the file size given the LFN
  dmlite::DmStatus setSize(ino_t fileid, int64_t filesize);
  /// Sets various file metadata fields
  dmlite::DmStatus setMode(ino_t inode, uid_t uid, gid_t gid, mode_t mode, const dmlite::Acl& acl);
  /// Removes a logical file entry
  dmlite::DmStatus unlink(ino_t inode);
  /// Adjust the size of the parent directories by a certain amount
  dmlite::DmStatus addFilesizeToDirs(dmlite::ExtendedStat file, int64_t sizediff);
  /// Add a symlink
  dmlite::DmStatus symlink(ino_t inode, const std::string &link);
  
  /// Get all the Replicas
  dmlite::DmStatus getReplicas(std::vector<dmlite::Replica> &reps, ino_t inode);
  /// Get all the Replicas
  dmlite::DmStatus getReplicas(std::vector<dmlite::Replica> &reps, std::string lfn);
  
  /// Extended stat for replica file names in rfio syntax
  dmlite::DmStatus getStatbyRFN(dmlite::ExtendedStat &st, std::string rfn);

  /// Gets replica information
  dmlite::DmStatus getReplicabyRFN(dmlite::Replica &rep, std::string rfn);
  /// Gets replica information
  dmlite::DmStatus getReplicabyId(dmlite::Replica &r, int64_t repid);
  /// Extended stat for inodes
  dmlite::DmStatus getStatbyFileid(dmlite::ExtendedStat &xstat, int64_t fileid );
  
  /// Extended stat for parent inodes
  dmlite::DmStatus getStatbyParentFileid(dmlite::ExtendedStat& xstat, int64_t fileid, std::string name);
  
  /// Move an entity to a different parent dir
  dmlite::DmStatus move(ino_t inode, ino_t dest);
  /// Change the name of a file
  dmlite::DmStatus rename(ino_t inode, const std::string& name);
  /// Read a link
  dmlite::DmStatus readLink(dmlite::SymLink &link, int64_t fileid);
  
  /// Start reading a dir
  dmlite::DmStatus opendir(DomeMySqlDir *&dir, const std::string& path);
  /// Close a dir
  dmlite::DmStatus closedir(DomeMySqlDir *&dir);
  /// Read an item from a dir
  dmlite::ExtendedStat* readdirx(DomeMySqlDir *&dir);
  
  /// Add a replica
  dmlite::DmStatus addReplica(const dmlite::Replica& replica);
  /// Updates the fields of a replica
  dmlite::DmStatus updateReplica(const dmlite::Replica& rdata);
  
  /// Updates the time fields. Set buffer to NULL to set with the current time
  dmlite::DmStatus utime(ino_t inode, const utimbuf *utim);
  
  /// Update the ext attributes of a file/dir Also propagates checksums to the legacy fields
  dmlite::DmStatus updateExtendedAttributes(ino_t inode, const dmlite::ExtendedStat& attr);
  /// Update the ext attributes of a file/dir Also propagates checksums to the legacy fields
  dmlite::DmStatus setChecksum(const ino_t fid, const std::string &csumtype, const std::string &csumvalue);
  
  /// Utility to check perms for a directory tree. Tells if a certain user can reach a certain file
  dmlite::DmStatus traverseBackwards(const dmlite::SecurityContext &secctx, dmlite::ExtendedStat& meta);
  // --------------------------------------------------
  
  /// Adds or overwrites a quotatoken
  int setQuotatoken(DomeQuotatoken &qtk, std::string &clientid);
  int setQuotatokenByStoken(DomeQuotatoken &qtk);

  /// Deletes a quotatoken
  int delQuotatoken(DomeQuotatoken &qtk, std::string &clientid);

  /// Deletes a replica
  int delReplica(int64_t replicaid, const std::string &rfn);

  /// Add/subtract an integer to used space of a directory
  int addtoDirectorySize(int64_t fileid, int64_t increment);

  /// Add/subtract an integer to the u_space of a quota(space)token
  /// u_space is the free space, to be DEcremented on write
  int addtoQuotatokenUspace(DomeQuotatoken &qtk, int64_t increment);
  int addtoQuotatokenUspace(std::string &s_token, int64_t increment);


  /// Adds a new filesystem to a pool that may or may not exist
  int addFs(DomeFsInfo &newfs);



  /// Add a new, empty pool
  int addPool(std::string &poolname, long defsize, char stype);

  /// Modify an existing filesystem, which points to a pool that may or may not exist
  int modifyFs(DomeFsInfo &newfs);
  
  /// Removes a fs
  int rmFs(std::string &server, std::string &fs);
  
  /// Removes a pool and all the related filesystems
  int rmPool(std::string &poolname);
  

  
protected:
  // The corresponding factory.
  //NsMySqlFactory* factory_;

  /// Transaction level, so begins and commits can be nested.
  unsigned transactionLevel_;


private:

  // Connection
  MYSQL *conn_;

};






/// Convenience class that releases a transaction on destruction
class DomeMySqlTrans {
public:
  DomeMySqlTrans(DomeMySql *o)
  {
    obj = o;
    obj->begin();
  }

  ~DomeMySqlTrans() {
    if (obj != 0) obj->rollback();
    obj = 0;
  }

  void Commit() {
    if (obj != 0) obj->commit();
    obj = 0;
  }

private:
  DomeMySql *obj;

};

#endif
