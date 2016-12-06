/*
 * Copyright 2015 CERN
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



/** @file   DomeReq.h
 * @brief  A helper class that describes a request to dpmrest, with helpers to parse it
 * @author Fabrizio Furano
 * @date   Dec 2015
 */


#ifndef DOMEREQ_H
#define DOMEREQ_H



#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <fcgiapp.h>

#include "cpp/authn.h"
#include "utils/DomeTalker.h"

/// Class that describes a request
/// Requests have some fields, that normally come from parsing a CGI request
/// The CGI request can carry a payload in the BODY, which is assumed to be in JSON format
/// This class can parse the input request into data structures that are easier to manage
/// Please note that this class is used for all the dpmrest requests, also the inter-cluster ones
class DomeReq {

public:


    DomeReq(FCGX_Request &request);

    /// The req method of the request, e.g. GET
    std::string verb;
    /// The object of the request, typically a filename or pathname
    std::string object;
    /// The dome command got from the request, e.g dome_getspaceinfo
    std::string domecmd;

    /// Here we put the fields that come from the body of the request
    /// These are in the form of JSON keyvalue pairs
    boost::property_tree::ptree bodyfields;

    /// Fills the fields from a JSON representation, usually taken from the body of the incoming request
    /// Returns 0 if OK
    int takeJSONbodyfields(char *body);

    /// The request also may carry information about the client that submitted it.
    /// For example, these will be the credentials of a disk server.
    /// ** These are NOT the credentials of the remote user **
    /// dpmrest can apply authorization schemes to this information. The consequence
    /// is that we can filter in a simple way the entities that can send commands.
    std::string clientdn, clienthost;
    std::vector<std::string> clientfqans;
    std::vector<std::string> clientauthkeys;

    /// And these are the identity of the remote user that originated the request,
    /// for the cases where it applies.
    dmlite::DomeCredentials creds;

    // dmlite::SecurityContext sec;
    // std::string remoteclientdn;
    // std::string remoteclienthost;


    /// Utility function to send a quick response. Returns <= 0 if error
    static int SendSimpleResp(FCGX_Request &request, int httpcode, const std::ostringstream &body, const char *logwhereiam = 0);
    static int SendSimpleResp(FCGX_Request &request, int httpcode, const std::string &body, const char *logwhereiam = 0);
    static int SendSimpleResp(FCGX_Request &request, int httpcode, const boost::property_tree::ptree &body, const char *logwhereiam = 0);
private:

};

#endif // MAIN_OBJECT_H
