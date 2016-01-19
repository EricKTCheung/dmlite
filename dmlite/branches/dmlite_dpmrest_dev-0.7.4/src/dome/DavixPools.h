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



/// @file   DavixPools.h
/// @brief  Davix pool implementation
/// @author Fabrizio Furano <furano@cern.ch>
/// @date   Jan 2016



#ifndef DAVIXPOOLS_H
#define DAVIXPOOLS_H


#ifdef __APPLE__
#include <bsm/audit_errno.h>
#endif

#include <algorithm>
#include <stdlib.h>
#include "utils/logger.h"
#include "utils/poolcontainer.h"
#include <davix.hpp>

namespace dmlite {
  

/// Factory for mysql connections
/// This is just mechanics of how the Poolcontainer class works
/// and wraps the creation of the actual mysql conns
class DavixConnectionFactory: public dmlite::PoolElementFactory<MYSQL*> {
public:
  MySqlConnectionFactory();

  MYSQL* create();
  void   destroy(MYSQL*);
  bool   isValid(MYSQL*);

  // Attributes
  std::string  host;
  unsigned int port;
  std::string  user;
  std::string  passwd;

  
  int dirspacereportdepth;
protected:
private:
};

/// Holder of mysql connections, base class singleton holding the mysql conn pool
class MySqlHolder {
public:
  
  static dmlite::PoolContainer<MYSQL*> &getMySqlPool() throw(dmlite::DmException);
  static bool configure(const std::string& key, const std::string& value);
  ~MySqlHolder();        

private:
  int poolsize;
  
  // Ctor initializes the local mysql factory and
  // creates the shared pool of mysql conns
  MySqlHolder();         

  static MySqlHolder *getInstance();
  static MySqlHolder *instance;
  
  /// Connection factory.
  MySqlConnectionFactory connectionFactory_;

  /// Connection pool.
  static dmlite::PoolContainer<MYSQL*> *connectionPool_;
  
};



}



#endif
