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


/// @file   DomeTalker.cpp
/// @brief  Utility class, used to send requests to Dome
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
/// @date   Mar 2016

#include "DomeTalker.h"
#include <boost/property_tree/json_parser.hpp>

using namespace dmlite;

DomeTalker::DomeTalker(DavixCtxPool &pool, const SecurityContext *sec, std::string verb, Davix::Uri uri, std::string cmd)
    : pool_(pool), creds_(sec->credentials), verb_(verb), uri_(uri), cmd_(cmd),
    grabber_(pool_), ds_(grabber_) {

  err_ = NULL;
  parsedJson_ = false;
}

DomeTalker::~DomeTalker() {
  Davix::DavixError::clearError(&err_);
}

bool DomeTalker::execute() {
  return this->execute("");
}

bool DomeTalker::execute(const std::ostringstream &ss) {
  return this->execute(ss.str());
}

bool DomeTalker::execute(const std::string &str) {
  Davix::DavixError::clearError(&err_);

  Davix::HttpRequest req(*ds_->ctx, uri_, &err_);
  if(err_) return false;

  req.setRequestMethod(verb_);
  req.addHeaderField("cmd", cmd_);
  req.addHeaderField("remoteclientdn", creds_.clientName);
  req.addHeaderField("remoteclientaddr", creds_.remoteAddress);

  req.setParameters(*ds_->parms);
  req.setRequestBody(str);

  int rc = req.executeRequest(&err_);
  response_ = req.getAnswerContentVec();
  status_ = req.getRequestCode();

  if(rc || err_) return false;
  return true;
}

const std::vector<char>& DomeTalker::response() {
  return response_;
}

const boost::property_tree::ptree& DomeTalker::jresp() {
  if(parsedJson_) return json_;

  std::istringstream iss(&response_[0]);
  boost::property_tree::read_json(iss, json_);
  parsedJson_ = true;
  return json_;
}

bool DomeTalker::execute(const boost::property_tree::ptree &params) {
  std::ostringstream ss;
  boost::property_tree::write_json(ss, params);
  return this->execute(ss.str());
}

bool DomeTalker::execute(const std::string &key, const std::string &value) {
  boost::property_tree::ptree params;
  params.put(key, value);
  return this->execute(params);
}

std::string DomeTalker::err() {
  if(err_) {
    if(response_.size() != 0) return err_->getErrMsg() + " - " + &response_[0];
    return err_->getErrMsg();
  }
  return "";
}

int DomeTalker::status() {
  return status_;
}