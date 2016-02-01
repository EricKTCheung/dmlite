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



/** @file   DomeCoreXeq.cpp
 * @brief  A part of the implementation of DomeCore. Functions implementing commands
 * @author Fabrizio Furano
 * @date   Feb 2016
 */


#include "DomeCore.h"
#include "DomeLog.h"
#include "DomeDmlitePool.h"
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <sys/vfs.h>
#include <unistd.h>





int DomeCore::dome_put(DomeReq &req, FCGX_Request &request) {


};
int DomeCore::dome_putdone(DomeReq &req, FCGX_Request &request) {};

int DomeCore::dome_getspaceinfo(DomeReq &req, FCGX_Request &request) {
  int rc = 0;
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering");


  boost::property_tree::ptree jresp;
  for (int i = 0; i < status.fslist.size(); i++) {
    std::string fsname, poolname;
    boost::property_tree::ptree top;



    fsname = "fsinfo^" + status.fslist[i].server + "^" + status.fslist[i].fs;

    // Add this server if not already there

    if (status.role == status.roleHead) { // Only headnodes report about pools
      jresp.put(boost::property_tree::ptree::path_type(fsname+"^poolname", '^'), status.fslist[i].poolname);
      jresp.put(boost::property_tree::ptree::path_type(fsname+"^fsstatus", '^'), status.fslist[i].status);
    }
    jresp.put(boost::property_tree::ptree::path_type(fsname+"^freespace", '^'), status.fslist[i].freespace);
    jresp.put(boost::property_tree::ptree::path_type(fsname+"^physicalsize", '^'), status.fslist[i].physicalsize);

    if (status.role == status.roleHead) { //Only headnodes report about pools
      poolname = "poolinfo^" + status.fslist[i].poolname;
      long long tot, free;
      status.getPoolSpaces(status.fslist[i].poolname, tot, free);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^poolstatus", '^'), 0);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^freespace", '^'), free);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^physicalsize", '^'), tot);

      poolname = "poolinfo^" + status.fslist[i].poolname + "^fsinfo^" + status.fslist[i].server + "^" + status.fslist[i].fs;

      jresp.put(boost::property_tree::ptree::path_type(poolname+"^fsstatus", '^'), status.fslist[i].status);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^freespace", '^'), status.fslist[i].freespace);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^physicalsize", '^'), status.fslist[i].physicalsize);
    }
  }


  std::ostringstream os;
  os << "Content-type: text\r\n\r\n";
  boost::property_tree::write_json(os, jresp);

  FCGX_PutS(os.str().c_str(), request.out);

  Log(Logger::Lvl3, domelogmask, domelogname, "Result: " << rc);
  return rc;
};

int DomeCore::dome_getquotatoken(DomeReq &req, FCGX_Request &request) {};
int DomeCore::dome_setquotatoken(DomeReq &req, FCGX_Request &request) {};
int DomeCore::dome_delquotatoken(DomeReq &req, FCGX_Request &request) {};

int DomeCore::dome_chksum(DomeReq &req, FCGX_Request &request) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering");
  std::ostringstream os;
  os << "Content-type: text\r\n\r\n";

  std::string chksumtype = req.bodyfields.get<std::string>("checksum-type", "null");
  std::string pfn = req.bodyfields.get<std::string>("pfn", "");
  std::string forcerecalc_str = req.bodyfields.get<std::string>("force-recalc", "false");

  bool forcerecalc = false;
  if(forcerecalc_str == "false" || forcerecalc_str == "0" || forcerecalc_str == "no") {
    forcerecalc = false;
  } else if(forcerecalc_str == "true" || forcerecalc_str == "1" || forcerecalc_str == "yes") {
    forcerecalc = true;
  }

  // DmlitePool dmpool("/etc/dmlite.conf");

  boost::property_tree::write_json(os, req.bodyfields);
  FCGX_PutS(os.str().c_str(), request.out);
}

int DomeCore::dome_chksumstatus(DomeReq &req, FCGX_Request &request) {

}

int DomeCore::dome_ispullable(DomeReq &req, FCGX_Request &request) {};
int DomeCore::dome_get(DomeReq &req, FCGX_Request &request) {};
int DomeCore::dome_pulldone(DomeReq &req, FCGX_Request &request) {};
int DomeCore::dome_statpool(DomeReq &req, FCGX_Request &request) {
  
  int rc = 0;
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering");
  
  if (status.role == status.roleDisk) { // Only headnodes report about pools
    std::ostringstream os;
    os << "I am a disk node. Only head nodes know pools.";
    return DomeReq::SendSimpleResp(request, 500, os);
  }
  
  std::string pn = req.bodyfields.get("poolname", "");
  if ( !pn.size() ) {
    std::ostringstream os;
    os << "Pool '" << pn << "' not found.";
    return DomeReq::SendSimpleResp(request, 404, os);
  }
  

  
  
  long long tot, free;
  status.getPoolSpaces(pn, tot, free);
      
  boost::property_tree::ptree jresp;      
  for (int i = 0; i < status.fslist.size(); i++)
    if (status.fslist[i].poolname == pn) {
      std::string fsname, poolname;
      boost::property_tree::ptree top;
      
      
     
      poolname = "poolinfo^" + status.fslist[i].poolname;
      
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^poolstatus", '^'), 0);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^freespace", '^'), free);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^physicalsize", '^'), tot);
      
      poolname = "poolinfo^" + status.fslist[i].poolname + "^fsinfo^" + status.fslist[i].server + "^" + status.fslist[i].fs;
      
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^fsstatus", '^'), status.fslist[i].status);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^freespace", '^'), status.fslist[i].freespace);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^physicalsize", '^'), status.fslist[i].physicalsize);
      
    }
  
  
  std::ostringstream os;
  boost::property_tree::write_json(os, jresp);
  rc = DomeReq::SendSimpleResp(request, 200, os);
  
  
  Log(Logger::Lvl3, domelogmask, domelogname, "Result: " << rc);
  return rc;
  
  
  
};


int DomeCore::dome_pull(DomeReq &req, FCGX_Request &request) {};
int DomeCore::dome_dochksum(DomeReq &req, FCGX_Request &request) {};
int DomeCore::dome_statfs(DomeReq &req, FCGX_Request &request) {};
