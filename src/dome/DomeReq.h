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

    /// Produces a JSON representation of the fields contained in the property tree
    /// Returns 0 if OK
    int getJSONbodyfields(std::string &body);

    /// The request also may carry information about the client that submitted it
    /// dpmrest can apply authorization schemes to this client. This is usually root
    /// or dpmmgr in the historical dpm architecture. We should not be limited
    /// by that while keeping dev/maintenance cost low.
    std::string clientdn;
    std::vector<std::string> fqans;
    std::vector<std::string> authkeys;

    /// Utility function to send a quick response
    static int SendSimpleResp(FCGX_Request &request, int httpcode, const std::ostringstream &body);
    static int SendSimpleResp(FCGX_Request &request, int httpcode, const std::string &body);
    static int SendSimpleResp(FCGX_Request &request, int httpcode, const boost::property_tree::ptree &body);
private:

};

#endif // MAIN_OBJECT_H
