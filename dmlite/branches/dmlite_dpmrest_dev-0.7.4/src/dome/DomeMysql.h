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
  
  /// Load from the DB, matching the given poolname and path
  int getQuotaTokenByKeys(DomeQuotatoken &qtk);
  
  /// Loads the defined filesystems, parses them into server names, etc. into the status
  int getFilesystems(DomeStatus &st);
  
  /// Adds or overwrites a quotatoken
  int setQuotatoken(DomeQuotatoken &qtk, std::string &clientid);
  
  /// Deletes a quotatoken
  int delQuotatoken(DomeQuotatoken &qtk, std::string &clientid);
  
  /// Removes a pool and all the related filesystems
  int rmPool(std::string &poolname);
  
  /// Adds a new filesystem to a pool that may or may not exist
  int addFsPool(DomeFsInfo &newfs);

  /// Add a new, empty pool
  int addPool(std::string &poolname, long defsize, char stype);
  
  /// Removes a fs
  int rmFs(std::string &server, std::string &fs);
  
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