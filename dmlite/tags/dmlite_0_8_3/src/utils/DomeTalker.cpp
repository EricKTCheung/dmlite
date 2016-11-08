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
#include "utils/DomeUtils.h"
#include <boost/property_tree/json_parser.hpp>

using namespace dmlite;




DomeTalker::DomeTalker(DavixCtxPool &pool, const DomeCredentials &creds, std::string uri,
                       std::string verb, std::string cmd)
    : pool_(pool), creds_(creds), uri_(DomeUtils::trim_trailing_slashes(uri)), verb_(verb), cmd_(cmd),
    grabber_(pool_), ds_(grabber_) {

  err_ = NULL;
  parsedJson_ = false;
  target_ = uri_ + "/command/" + cmd_;
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

  Davix::Uri target(target_);
  Davix::HttpRequest req(*ds_->ctx, target, &err_);
  if(err_) return false;

  req.setRequestMethod(verb_);

  if(!creds_.clientName.empty()) {
    req.addHeaderField("remoteclientdn", creds_.clientName);
  }

  if(!creds_.remoteAddress.empty()) {
    req.addHeaderField("remoteclienthost", creds_.remoteAddress);
  }

  if(!creds_.groups.empty()) {
    req.addHeaderField("remoteclientgroups", DomeUtils::join(",", creds_.groups));
  }

  req.setParameters(*ds_->parms);
  req.setRequestBody(DomeUtils::unescape_forward_slashes(str));

  int rc = req.executeRequest(&err_);
  response_ = std::string(req.getAnswerContentVec().begin(), req.getAnswerContentVec().end());
  status_ = req.getRequestCode();

  if(rc || err_) return false;
  return true;
}

const std::string& DomeTalker::response() {
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

bool DomeTalker::execute(const std::string &key1, const std::string &value1,
                         const std::string &key2, const std::string &value2) {
  boost::property_tree::ptree params;
  params.put(key1, value1);
  params.put(key2, value2);
  return this->execute(params);
}

std::string DomeTalker::err() {
  if(err_) {
    std::ostringstream os;
    os << "Error when issuing request to '" << target_ << "'. Status " << status_ << ". ";
    os << "DavixError: '" << err_->getErrMsg() << "'. ";

    if(response_.size() != 0) {
      os << "Response (" << response_.size() << " bytes): '" << response_ << "'.";
    }
    else {
      os << "No response to show.";
    }

    return os.str();
  }
  return "";
}

int DomeTalker::status() {
  return status_;
}

struct StatusCodePair {
  int status;
  int code;
};

static struct StatusCodePair pairs[] = {
  { DOME_HTTP_OK,                   DMLITE_SUCCESS  },
  { DOME_HTTP_BAD_REQUEST,          EINVAL          },
  { DOME_HTTP_NOT_FOUND,            ENOENT          },
  { DOME_HTTP_CONFLICT,             EEXIST          },
  { DOME_HTTP_INSUFFICIENT_STORAGE, ENOSPC          },
  { DOME_HTTP_DENIED,               EACCES          }
};

int dmlite::http_status(const dmlite::DmException &e) {
  for(size_t i = 0; i < sizeof(pairs) / sizeof(pairs[0]); i++) {
    if(pairs[i].code == DMLITE_ERRNO(e.code())) {
      return pairs[i].status;
    }
  }
  return DOME_HTTP_INTERNAL_SERVER_ERROR;
}

int DomeTalker::dmlite_code() {
  for(size_t i = 0; i < sizeof(pairs); i++) {
    if(pairs[i].status == status_) {
      return pairs[i].code;
    }
  }
  return EINVAL;
}
