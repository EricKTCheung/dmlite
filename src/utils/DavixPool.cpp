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
Logger::bitmask dmlite::davixpoollogmask = ~0;
Logger::component dmlite::davixpoollogname = "DavixPool";

// -----------------------------------------
// DavixCtxFactory
//
DavixCtxFactory::DavixCtxFactory() {
  Log(Logger::Lvl4, davixpoollogmask, davixpoollogname, "DavixCtxFactory started");

  // initialize default parameters
  struct timespec spec_timeout;
  spec_timeout.tv_sec = 300;
  spec_timeout.tv_nsec = 0;
  params_.setConnectionTimeout(&spec_timeout);
  params_.setOperationTimeout(&spec_timeout);
  params_.addCertificateAuthorityPath("/etc/grid-security/certificates");
  params_.setOperationRetry(0);

  // Nothing
}

void DavixCtxFactory::configure(const std::string &key, const std::string &value) {
  if (key == "DavixConnTimeout") {
    long conn_timeout = atoi(value.c_str());
    struct timespec spec_timeout;
    spec_timeout.tv_sec = conn_timeout;
    spec_timeout.tv_nsec = 0;
    params_.setConnectionTimeout(&spec_timeout);
  }
  else if(key == "DavixOpsTimeout") {
    long ops_timeout = atoi(value.c_str());
    struct timespec spec_timeout;
    spec_timeout.tv_sec = ops_timeout;
    spec_timeout.tv_nsec =0;
    params_.setOperationTimeout(&spec_timeout);
  }
  else if(key == "DavixSSLCheck") {
    long v = true;
    if(value == "False") v = false;
    params_.setSSLCAcheck(v);
  }
  else if(key == "DavixCAPath") {
    if(!value.empty()) {
      params_.addCertificateAuthorityPath(value);
    }
  }
  else if(key == "DavixCertPath") {
    davix_cert_path = value;
  }
  else if(key == "DavixPrivateKeyPath") {
    davix_privkey_path = value;
  }

  // should I load certificate and key?
  if( (key == "DavixCertPath" || key == "DavixPrivateKeyPath") &&
     (!davix_cert_path.empty() && !davix_privkey_path.empty()) ) {

     Davix::X509Credential cred;
     Davix::DavixError* tmp_err = NULL;

     cred.loadFromFilePEM(davix_privkey_path, davix_cert_path, "", &tmp_err);
     if( tmp_err ){
       std::ostringstream os;
       os << "Cannot load cert-privkey " << davix_cert_path << "-" << davix_privkey_path << ", Error: " << tmp_err->getErrMsg();
       throw DmException(EPERM, os.str());
     }
     params_.setClientCertX509(cred);
  }
}

void DavixCtxFactory::setRequestParams(const Davix::RequestParams &params)
{
  params_ = params;
}


DavixStuff* DavixCtxFactory::create()
{
  DavixStuff*  c;

  Log(Logger::Lvl4, davixpoollogmask, davixpoollogname, "Creating DavixStuff... ");

  c = new DavixStuff(params_);

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
