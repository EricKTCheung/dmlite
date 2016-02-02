/*
 * Copyright 2015 <copyright holder> <email>
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



/** @file   DomeReq.cpp
 * @brief  A helper class that describes a request to dpmrest, with helpers to parse it
 * @author Fabrizio Furano
 * @date   Aug 2015
 */

#include "DomeReq.h"
#include "DomeLog.h"
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

DomeReq::DomeReq(FCGX_Request &request) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Ctor");
  char *s;

  if ( (s = FCGX_GetParam("REQUEST_METHOD", request.envp)) )
    verb = s;
  if ( (s = FCGX_GetParam("PATH_INFO", request.envp)) )
    object = s;
  if ( (s = FCGX_GetParam("HTTP_CMD", request.envp)) )
    domecmd = s;

  // We assume that the body fits in 4K, otherwise we ignore it ?!?
  char buf[4096];
  int nb = FCGX_GetStr(buf, sizeof(buf)-1, request.in);
  if (nb < sizeof(buf)) {
    buf[nb] = '\0';
    Log(Logger::Lvl4, domelogmask, domelogname, "Body: '" << buf << "'");
  }

  takeJSONbodyfields( buf );
}


int DomeReq::takeJSONbodyfields(char *body) {

  // We assume that the body that we received is in JSON format
  std::istringstream s(body);
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering: '" << body << "'");
  try {
    boost::property_tree::read_json(s, bodyfields);
  } catch (boost::property_tree::json_parser_error e) {
    Err("takeJSONbodyfields", "Could not process JSON: " << e.what() << " '" << body << "'");
    return -1;
  }
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting: '" << body << "'");
  return 0;
}


int DomeReq::getJSONbodyfields(std::string &body) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering: '" << body << "'");
  std::ostringstream s(body);
  try {
    boost::property_tree::write_json(s, bodyfields);
  } catch (boost::property_tree::json_parser_error e) {
    Err("getJSONbodyfields", "Could not process JSON: " << e.what() << " '" << body << "'");
    return -1;
  }

  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting: '" << body << "'");
  return 0;
}

int DomeReq::SendSimpleResp(FCGX_Request &request, int httpcode, const std::ostringstream &body) {
  return SendSimpleResp(request, httpcode, body.str());
}

int DomeReq::SendSimpleResp(FCGX_Request &request, int httpcode, const boost::property_tree::ptree &body) {
  std::ostringstream os;
  boost::property_tree::write_json(os, body);
  return SendSimpleResp(request, httpcode, os.str());
}

int DomeReq::SendSimpleResp(FCGX_Request &request, int httpcode, const std::string &body) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering: code: " << httpcode << " body: '" << body << "'");
  int rc = 0;

  std::ostringstream hdr;
  hdr << "Status: " << httpcode << "\r\n" <<
    "Content-type: text\r\n\r\n";

  rc = FCGX_PutS(hdr.str().c_str(), request.out);
  if (rc <= 0) return rc;

  FCGX_PutS(body.c_str(), request.out);
  if (rc <= 0) return rc;

  Log(Logger::Lvl3, domelogmask, domelogname, "Entering: code: " << httpcode << " body: '" << body << "'");
}
