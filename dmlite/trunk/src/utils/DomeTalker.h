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


/// @file   DomeTalker.h
/// @brief  Utility class, used to send requests to Dome
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
/// @date   Mar 2016

#ifndef UTILS_DOMETALKER_H
#define UTILS_DOMETALKER_H

#include <davix/davix.hpp>
#include "DavixPool.h"
#include "cpp/authn.h"

#include <boost/property_tree/ptree.hpp>

namespace dmlite {

enum DomeHttpCode {
  DOME_HTTP_OK = 200,

  DOME_HTTP_BAD_REQUEST = 400,
  DOME_HTTP_DENIED = 403,
  DOME_HTTP_NOT_FOUND = 404,
  DOME_HTTP_CONFLICT = 409,
  DOME_HTTP_UNPROCESSABLE = 422,

  DOME_HTTP_INTERNAL_SERVER_ERROR = 500,
  DOME_HTTP_INSUFFICIENT_STORAGE = 507
};

int http_status(const dmlite::DmException &e);

class DomeTalker {
public:
  DomeTalker(DavixCtxPool &pool, const SecurityCredentials *creds, std::string uri, std::string verb, std::string cmd);
  ~DomeTalker();

  bool execute();
  bool execute(const boost::property_tree::ptree &params);
  bool execute(const std::string &str);
  bool execute(const std::ostringstream &ss);

  // only send a single json param
  bool execute(const std::string &key, const std::string &value);

  // only send two
  bool execute(const std::string &key1, const std::string &value1,
               const std::string &key2, const std::string &value2);

  // get error message, if it exists
  std::string err();

  // get response status
  int status();

  // produce the appropriate code for a dmlite exception
  int dmlite_code();

  const boost::property_tree::ptree& jresp();
  const std::string& response();
private:
  DavixCtxPool &pool_;
  const SecurityCredentials *creds_;
  std::string verb_;
  std::string uri_;
  std::string cmd_;

  std::string target_;

  DavixGrabber grabber_;
  DavixStuff *ds_;

  Davix::DavixError *err_;
  std::string response_;
  boost::property_tree::ptree json_;
  bool parsedJson_;
  int status_;
};

}
#endif