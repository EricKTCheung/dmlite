/*
 * Copyright 2016 CERN
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



/// @file   DomeDavixPool.cpp
/// @brief  Davix contexts pool implementation
/// @author Fabrizio Furano <furano@cern.ch>
/// @date   Jan 2016


#ifdef __APPLE__
#include <bsm/audit_errno.h>
#endif

#include <algorithm>
#include <cstring>
#include <pthread.h>
#include <stdlib.h>
#include <davix.hpp>
#include "utils/Config.hh"
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/utils/poolcontainer.h>
#include "DavixPool.h"

using namespace dmlite;

// Logger stuff
Logger::bitmask dmlite::davixpoollogmask = 0;
Logger::component dmlite::davixpoollogname = "DavixPool";
  
//------------------------------------------
// DavixCtxPoolHolder
// Holder of mysql connections, base class singleton holding the mysql conn pool
//

// Static  stuff
PoolContainer<DavixStuff *> *DavixCtxPoolHolder::connectionPool_ = 0;
DavixCtxPoolHolder *DavixCtxPoolHolder::instance = 0;

DavixCtxPoolHolder *DavixCtxPoolHolder::getInstance() {
  if (!instance) {
    instance = new DavixCtxPoolHolder();
  }
  return instance;
}

DavixCtxPoolHolder::DavixCtxPoolHolder() {
  poolsize = CFG->GetLong("glb.davix.poolsize", 10);
    
    connectionPool_ = 0;
}

DavixCtxPoolHolder::~DavixCtxPoolHolder() {
  if (connectionPool_) delete connectionPool_;
  poolsize = 0;
  connectionPool_ = 0;

}


PoolContainer<DavixStuff*> &DavixCtxPoolHolder::getDavixCtxPool()  throw(DmException){

  DavixCtxPoolHolder *h = getInstance();
  
  if (!h->connectionPool_) {
    Log(Logger::Lvl1, davixpoollogmask, davixpoollogname, "Creating Davix::Context connection pool. size: " << h->poolsize);
    
    
    h->connectionPool_ = new PoolContainer<DavixStuff*>(&h->connectionFactory_, h->poolsize);
  }
  
  return *(h->connectionPool_);
}




// -----------------------------------------
// DavixCtxFactory
//
DavixCtxFactory::DavixCtxFactory() {
  

  Log(Logger::Lvl4, davixpoollogmask, davixpoollogname, "DavixCtxFactory started");
  // Nothing
}


DavixStuff* DavixCtxFactory::create()
{
  DavixStuff*  c;
  
  Log(Logger::Lvl4, davixpoollogmask, davixpoollogname, "Creating DavixStuff... ");

  c = new DavixStuff();
  
  Log(Logger::Lvl3, davixpoollogmask, davixpoollogname, "Ok.");
  return c;
}

void DavixCtxFactory::destroy(DavixStuff* c)
{
  Log(Logger::Lvl4, davixpoollogmask, davixpoollogname, "Destroying... ");
  
  delete c;
  
  Log(Logger::Lvl3, davixpoollogmask, davixpoollogname, "Destroyed. ");
}

bool DavixCtxFactory::isValid(DavixStuff*)
{
  // Reconnect set to 1, so even if the connection dropped,
  // it will reconnect.
  return true;
}



