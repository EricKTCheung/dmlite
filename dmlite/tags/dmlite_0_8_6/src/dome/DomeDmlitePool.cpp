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


/// @file   DomeDmlitePool.cpp
/// @brief  dmlite contexts pool implementation
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
/// @date   Jan 2016


#ifdef __APPLE__
#include <bsm/audit_errno.h>
#endif

#include <algorithm>
#include <cstring>
#include <pthread.h>
#include <stdlib.h>
#include <davix.hpp>
#include "DomeLog.h"
#include "utils/Config.hh"
#include "DomeDmlitePool.h"
#include "cpp/authn.h"


using namespace dmlite;

DmlitePoolHandler::~DmlitePoolHandler() {
  if(pool) {
    pool->ReleaseStackInstance(inst);
  }

  pool = NULL;
  inst = NULL;
}

DmlitePoolHandler::DmlitePoolHandler(DmlitePool* _pool, bool cancreate) {
  pool = _pool;
  inst = pool->GetStackInstance(cancreate);
}

StackInstance* DmlitePool::GetStackInstance(bool cancreate) {
  dmlite::StackInstance *si = 0;

  {
    boost::unique_lock< boost::mutex > l(dmlitemutex);
    if (siqueue.size() > 0) {
      si = siqueue.front();
      siqueue.pop();
    }
  }

  if (!si && cancreate) {
    boost::unique_lock< boost::mutex > l(dmlitemutex);
    try {
      Log(Logger::Lvl1, domelogmask, domelogname, "Creating new stack instance");
      si = new dmlite::StackInstance(pluginManager);
    } catch (dmlite::DmException & e) {
      Log(Logger::Lvl1, domelogmask, domelogname, "Cannot create StackInstance. Catched exception: "
                                                  << e.code() << " what: " << e.what());
    }
  }

  Log(Logger::Lvl4, domelogmask, domelogname, "Got stack instance " << si);

  // reset the security credentials
  SecurityContext *context = si->getAuthn()->createSecurityContext();
  si->setSecurityContext(*context);
  delete context;

  return si;
}

void DmlitePool::ReleaseStackInstance(dmlite::StackInstance *inst) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Releasing stack instance " << inst);
  if (inst) {
    boost::unique_lock< boost::mutex > l(dmlitemutex);
    siqueue.push(inst);
  }
}
