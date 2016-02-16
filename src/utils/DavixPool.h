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



/// @file   DomeDavixPool.h
/// @brief  Pool of davix contexts
/// @author Fabrizio Furano <furano@cern.ch>
/// @date   Jan 2016



#ifndef UTILS_DAVIXPOOL_H
#define UTILS_DAVIXPOOL_H


#ifdef __APPLE__
#include <bsm/audit_errno.h>
#endif

#include <algorithm>
#include <stdlib.h>
#include "utils/logger.h"
#include "utils/poolcontainer.h"
#include "utils/Config.hh"
 
#include <davix/davix.hpp>

namespace dmlite {

extern Logger::bitmask davixpoollogmask;
extern Logger::component davixpoollogname;

  class DavixStuff {
  public:
    DavixStuff(Davix::RequestParams params) {
      ctx = new Davix::Context();
      parms = new Davix::RequestParams(params);
    }
    
    ~DavixStuff() {
      delete parms;
      delete ctx;
      parms = 0;
      ctx = 0;
    }
    
    Davix::Context *ctx;
    Davix::RequestParams *parms;
  };
  
  /// Factory for davix contexts
  /// This is just mechanics of how the Poolcontainer class works
  /// and wraps the creation of the actual instances
  class DavixCtxFactory: public dmlite::PoolElementFactory<DavixStuff *> {
  public:
    DavixCtxFactory();
    
    DavixStuff* create();
    void   destroy(DavixStuff*);
    bool   isValid(DavixStuff*);

    void configure(const std::string &key, const std::string &value);
    void setRequestParams(const Davix::RequestParams &params);
  protected:
  private:
    Davix::RequestParams params_;

    std::string davix_cert_path;
    std::string davix_privkey_path;
  };

  class DavixCtxPool : public dmlite::PoolContainer<DavixStuff *> {
  public:
    DavixCtxPool(PoolElementFactory<DavixStuff *> *factory, int n) :
    dmlite::PoolContainer<DavixStuff *>(factory, n) { }
  };
  

class DavixGrabber : public PoolGrabber<DavixStuff*> {
public:
  DavixGrabber(DavixCtxPool &pool, bool block = true) : 
  PoolGrabber<DavixStuff *>(pool, block) {}
};

}


#endif
