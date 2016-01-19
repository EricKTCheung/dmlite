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

#include <string>
#include <mysql/mysql.h>


class DpmrStatus;




class DomeMySql {
public:
  /// Acquires a mysql connection on creation
  DomeMySql(std::string &dbconnstring);
  
  int begin();
  int rollback();
  int commit();
  
  /// Loads spaces and quotas into the status
  int getSpacesQuotas(DpmrStatus &st);
  
  protected:
    // The corresponding factory.
    //NsMySqlFactory* factory_;

    /// Transaction level, so begins and commits can be nested.
    unsigned transactionLevel_;

  private:
    /// The db conn string we use
    std::string nsDb_;

    // Connection
    MYSQL *conn_;

};






  /// Convenience class that releases a resource on destruction
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

