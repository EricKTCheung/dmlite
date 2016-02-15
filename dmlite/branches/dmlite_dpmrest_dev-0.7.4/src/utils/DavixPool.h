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
    DavixStuff() {
      ctx = new Davix::Context();
      parms = new Davix::RequestParams();
     
      // set timeouts, etc
      long timeout;
      struct timespec spec_timeout;
      timeout = CFG->GetLong("glb.restclient.conn_timeout", 15);
      Log(Logger::Lvl1, davixpoollogmask, davixpoollogname, "Connection timeout is set to : " << timeout);
      spec_timeout.tv_sec = timeout;
      spec_timeout.tv_nsec =0;
      parms->setConnectionTimeout(&spec_timeout);
    
      timeout = CFG->GetLong("glb.restclient.ops_timeout", 15);
      spec_timeout.tv_sec = timeout;
      spec_timeout.tv_nsec = 0;
      parms->setOperationTimeout(&spec_timeout);
      Log(Logger::Lvl1, davixpoollogmask, davixpoollogname, "Operation timeout is set to : " << timeout);
    
      Davix::X509Credential cred;
      Davix::DavixError* tmp_err = NULL;
      
      
      
      // get ssl check
      bool ssl_check = CFG->GetBool("glb.davix.ssl_check", true);
      Log(Logger::Lvl1, davixpoollogmask, davixpoollogname,"SSL CA check for davix is set to  " + std::string((ssl_check) ? "TRUE" : "FALSE"));
      parms->setSSLCAcheck(ssl_check);
      // ca check
      std::string ca_path = CFG->GetString("glb.restclient.ca_path", (char *)"/etc/grid/security/certificates");
      if( ca_path.size() > 0){
        Log(Logger::Lvl1, davixpoollogmask, davixpoollogname, "CA Path :  " << ca_path);
        parms->addCertificateAuthorityPath(ca_path);
      }
      
      
      cred.loadFromFilePEM(CFG->GetString("glb.restclient.cli_private_key", (char *)""), CFG->GetString("glb.restclient.cli_certificate", (char *)""), "", &tmp_err);
      if( tmp_err ){
        std::ostringstream os;
        os << "Cannot load cert-privkey " << CFG->GetString("glb.restclient.cli_certificate", (char *)"") << "-" <<
          CFG->GetString("glb.restclient.cli_private_key", (char *)"") << ", Error: "<< tmp_err->getErrMsg();
        
        throw DmException(EPERM, os.str());
      }
  
      parms->setClientCertX509(cred);
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
    
  protected:
  private:
  };
  
  /// Holder of mysql connections, base class singleton holding the mysql conn pool
  class DavixCtxPoolHolder {
  public:
    
    static dmlite::PoolContainer<DavixStuff*> &getDavixCtxPool() throw(dmlite::DmException);
    
    ~DavixCtxPoolHolder();        
    
  private:
    int poolsize;
    
    // Ctor initializes the local factory and
    // creates the shared pool of conns
    DavixCtxPoolHolder();         
    
    static DavixCtxPoolHolder *getInstance();
    static DavixCtxPoolHolder *instance;
    
    /// Connection factory.
    DavixCtxFactory connectionFactory_;
    
    /// Connection pool.
    static dmlite::PoolContainer<DavixStuff *> *connectionPool_;
    
  };
  






/// Convenience class that releases a Davix instance on destruction
class DavixScopedGetter {
public:
  DavixScopedGetter()
  {
    obj = DavixCtxPoolHolder::getDavixCtxPool().acquire();
  }
  
  ~DavixScopedGetter() {
    DavixCtxPoolHolder::getDavixCtxPool().release(obj);
    
  }
  
  DavixStuff *get() {
    return obj;
  }
  
private:
  DavixStuff *obj;
  
};


}


#endif
