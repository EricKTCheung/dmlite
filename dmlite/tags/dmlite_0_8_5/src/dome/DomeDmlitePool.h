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

/// @file   DomeDmlitePool.h
/// @brief  Pool of dmlite contexts
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
/// @date   Jan 2016



#ifndef DMLITEPOOL_H
#define DMLITEPOOL_H

#ifdef __APPLE__
#include <bsm/audit_errno.h>
#endif

#include <algorithm>
#include <stdlib.h>
#include "utils/logger.h"
#include "utils/poolcontainer.h"
#include <dmlite/cpp/dmlite.h>
#include "DomeLog.h"

class DmlitePool;

class DmlitePoolHandler {
private:
  dmlite::StackInstance *inst;
  DmlitePool *pool;

  // disallow copy and assign
  DmlitePoolHandler(const DmlitePoolHandler& that);
  void operator=(const DmlitePoolHandler&);
public:
  DmlitePoolHandler(DmlitePool* _pool, bool cancreate = true);
  ~DmlitePoolHandler();

  // overload operator->
  dmlite::StackInstance* operator->() const {
    return inst;
  }
};

class DmlitePool {
public:
  DmlitePool(std::string configfile) {
    try {
      pluginManager = new dmlite::PluginManager();
      pluginManager->loadConfiguration(configfile);
      //catalogFactory = pluginManager->getCatalogFactory();
    }
    catch(dmlite::DmException& e) {
      Log(Logger::Lvl1, domelogmask, domelogname, "Error initializing dmlite StackInstance."
          << "Reason: " << e.what() << std::endl);
      exit(1);
    }
  }

  ~DmlitePool() {
    //delete catalogFactory;
    delete pluginManager;
  }

  dmlite::StackInstance* GetStackInstance(bool cancreate = true);
  void ReleaseStackInstance(dmlite::StackInstance *inst);
private:
    dmlite::PluginManager *pluginManager;
    dmlite::CatalogFactory *catalogFactory;

    /// Mutex for protecting dmlite
    boost::mutex dmlitemutex;

    std::queue<dmlite::StackInstance *> siqueue;
};

#endif
