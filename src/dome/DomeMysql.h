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


class DomeStatus;
class DomeQuotatoken;
class DomeFsInfo;


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

  // All the helper primitives are here, for quick usage

  /// Loads spaces and quotas into the given status. Thread safe.
  int getSpacesQuotas(DomeStatus &st);

  /// Loads users and groups into the given status. Thread safe.
  int getGroups(DomeStatus &st);
  int getUsers(DomeStatus &st);

  /// Load from the DB, matching the given poolname and path
  int getQuotaTokenByKeys(DomeQuotatoken &qtk);

  /// Load the defined pools
  int getPools(DomeStatus &st);

  /// Loads the defined filesystems, parses them into server names, etc. into the status
  int getFilesystems(DomeStatus &st);

  /// Extended stat for logical file names
  dmlite::DmStatus getStatbyLFN(dmlite::ExtendedStat &st, std::string lfn, bool followSym = false);

  /// Extended stat for replica file names in rfio syntax
  dmlite::DmStatus getStatbyRFN(dmlite::ExtendedStat &st, std::string rfn);

  /// Gets replica information
  dmlite::DmStatus getReplicabyRFN(dmlite::Replica &rep, std::string rfn);

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

  /// Removes a pool and all the related filesystems
  int rmPool(std::string &poolname);

  /// Adds a new filesystem to a pool that may or may not exist
  int addFs(DomeFsInfo &newfs);

  /// Modify an existing filesystem, which points to a pool that may or may not exist
  int modifyFs(DomeFsInfo &newfs);

  /// Add a new, empty pool
  int addPool(std::string &poolname, long defsize, char stype);

  /// Removes a fs
  int rmFs(std::string &server, std::string &fs);

  /// Sets the file size given the LFN
  dmlite::DmStatus setSize(std::string lfn, int64_t filesize);

protected:
  // The corresponding factory.
  //NsMySqlFactory* factory_;

  /// Transaction level, so begins and commits can be nested.
  unsigned transactionLevel_;

  /// Extended stat for inodes
  dmlite::DmStatus getStatbyFileid(dmlite::ExtendedStat &xstat, int64_t fileid );
  dmlite::DmStatus getStatbyParentFileid(dmlite::ExtendedStat& xstat, int64_t fileid, std::string name);
  dmlite::DmStatus readLink(dmlite::SymLink link, int64_t fileid);
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
