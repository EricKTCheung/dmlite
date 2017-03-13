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



/// @file   MySqlPools.h
/// @brief  MySQL pool implementation
/// @author Fabrizio Furano <furano@cern.ch>
/// @date   Dec 2015


#ifdef __APPLE__
#include <bsm/audit_errno.h>
#endif

#include <algorithm>
#include <cstring>
#include <pthread.h>
#include <stdlib.h>
#include <mysql/mysql.h>
#include "utils/logger.h"

#include "utils/mysqlpools.h"


using namespace dmlite;


// Logger stuff
Logger::bitmask mysqlpoolslogmask = ~0;
Logger::component mysqlpoolslogname = "Mysqlpools";



pthread_once_t dmlite::initialize_mysql_thread = PTHREAD_ONCE_INIT;
pthread_key_t  dmlite::destructor_key;

void dmlite::destroy_thread(void*)
{
  mysql_thread_end();
}

void dmlite::init_thread(void)
{
  mysql_thread_init();
  pthread_key_create(&destructor_key, destroy_thread);
}


//------------------------------------------
// MySqlHolder
// Holder of mysql connections, base class singleton holding the mysql conn pool
//

// Static mysql-related stuff
PoolContainer<MYSQL*> *MySqlHolder::connectionPool_ = 0;
MySqlHolder *MySqlHolder::instance = 0;

MySqlHolder *MySqlHolder::getInstance() {
  if (!instance) {
    instance = new MySqlHolder();
  }
  return instance;
}

MySqlHolder::MySqlHolder() {
    mysql_library_init(0, NULL, NULL);
    poolsize = 0;
    connectionPool_ = 0;
}

MySqlHolder::~MySqlHolder() {
  if (connectionPool_) delete connectionPool_;
  poolsize = 0;
  connectionPool_ = 0;

}


PoolContainer<MYSQL*> &MySqlHolder::getMySqlPool()  throw(DmException){

  MySqlHolder *h = getInstance();

  if (!h->connectionPool_) {
    Log(Logger::Lvl1, mysqlpoolslogmask, mysqlpoolslogname, "Creating MySQL connection pool" <<
        h->connectionFactory_.user << "@" << h->connectionFactory_.host << ":" <<h->connectionFactory_.port <<
        " size: " << h->poolsize);
    h->connectionPool_ = new PoolContainer<MYSQL*>(&h->connectionFactory_, h->poolsize);
  }

  return *(h->connectionPool_);
}

bool MySqlHolder::configure(const std::string& key, const std::string& value) {
  MySqlHolder *h = getInstance();
  bool gotit = true;

  LogCfgParm(Logger::Lvl4, mysqlpoolslogmask, mysqlpoolslogname, key, value);

  if (key == "MySqlHost")
    h->connectionFactory_.host = value;
  else if (key == "MySqlUsername")
    h->connectionFactory_.user = value;
  else if (key == "MySqlPassword")
    h->connectionFactory_.passwd = value;
  else if (key == "MySqlPort")
    h->connectionFactory_.port = atoi(value.c_str());
  else if (key == "NsPoolSize") {
    int n = atoi(value.c_str());
      h->poolsize = (n < h->poolsize ? h->poolsize : n);
      if (h->connectionPool_)
        h->connectionPool_->resize(h->poolsize);
    }
  else if (key == "MySqlDirectorySpaceReportDepth")
    h->connectionFactory_.dirspacereportdepth = atoi(value.c_str());
  else gotit = false;

  if (gotit)
    LogCfgParm(Logger::Lvl4,  mysqlpoolslogmask, mysqlpoolslogname, key, value);

  return gotit;
}


void MySqlHolder::configure(std::string host, std::string username, std::string password, int port, int poolsize) {
  MySqlHolder *h = MySqlHolder::getInstance();

  Log(Logger::Lvl4, mysqlpoolslogmask, mysqlpoolslogname, "Configuring MySQL access. host:'" << host <<
  "' user:'" << username <<
  "' port:'" << port <<
  "' poolsz:" << poolsize);;

  h->connectionFactory_.host = host;
  h->connectionFactory_.user = username;
  h->connectionFactory_.passwd = password;
  h->connectionFactory_.port = port;

  h->poolsize = (poolsize < h->poolsize ? h->poolsize : poolsize);
  if (h->connectionPool_)
    h->connectionPool_->resize(h->poolsize);


}


// -----------------------------------------
// MySqlConnectionFactory
//
MySqlConnectionFactory::MySqlConnectionFactory() {

  dirspacereportdepth = 6;
  Log(Logger::Lvl4, mysqlpoolslogmask, mysqlpoolslogname, "MySqlConnectionFactory started");
  // Nothing
}


MYSQL* MySqlConnectionFactory::create()
{
  MYSQL*  c;
  my_bool reconnect  = 1;
  my_bool truncation = 0;

  Log(Logger::Lvl4, mysqlpoolslogmask, mysqlpoolslogname, "Connecting... " << user << "@" << host << ":" << port);

  c = mysql_init(NULL);

  mysql_options(c, MYSQL_OPT_RECONNECT,          &reconnect);
  mysql_options(c, MYSQL_REPORT_DATA_TRUNCATION, &truncation);

  if (mysql_real_connect(c, host.c_str(),
                         user.c_str(), passwd.c_str(),
                         NULL, port, NULL, CLIENT_FOUND_ROWS) == NULL) {
    std::string err("Could not connect! ");
    err += mysql_error(c);
    mysql_close(c);
#ifdef __APPLE__
    throw DmException(DMLITE_DBERR(BSM_ERRNO_ECOMM), err);
#else
    throw DmException(DMLITE_DBERR(ECOMM), err);
#endif
  }

  Log(Logger::Lvl3, mysqlpoolslogmask, mysqlpoolslogname, "Connected. " << user << "@" << host << ":" << port);
  return c;
}

void MySqlConnectionFactory::destroy(MYSQL* c)
{
  Log(Logger::Lvl4, mysqlpoolslogmask, mysqlpoolslogname, "Destroying... ");
  mysql_close(c);
  Log(Logger::Lvl3, mysqlpoolslogmask, mysqlpoolslogname, "Destroyed. ");
}

bool MySqlConnectionFactory::isValid(MYSQL*)
{
  // Reconnect set to 1, so even if the connection dropped,
  // it will reconnect.
  return true;
}
