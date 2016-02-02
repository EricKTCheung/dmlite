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

#include <dmlite/cpp/authn.h>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/catalog.h>

using namespace dmlite;

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

// std::string getChecksumFromExtensible(std::string chksumtype, Extensible *ext) {
//   for(Extensible::const_iterator it = extensible->begin(); it != extensible->end(); it++) {
//     if(checksums::isChecksumFullName(it->first)) {
//       std::string key = it->first;
//       std::string value = Extensible::anyToString(it->second);
//
//       if("checksum." + chksumtype == key) {
//         std::ostringstream os;
//         os << " { \"checksum\" : \"" << value << "\" }";
//         return DomeReq::SendSimpleResp(request, 200, os);
//       }
//     }
//   }
// }

void DomeCore::calculateChecksum(std::string lfn, std::string pfn) {
  if(status.role == status.roleHead) {
    // add to the queue
  }
  if(status.role == status.roleDisk) {
    // start task executor
  }
}

int DomeCore::dome_chksum(DomeReq &req, FCGX_Request &request) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering");
  DmlitePoolHandler stack(dmpool);

  try {
    dmlite::Catalog* catalog = stack->getCatalog();

    std::string chksumtype = req.bodyfields.get<std::string>("checksum-type", "null");
    std::string fullchecksum = "checksum." + chksumtype;
    std::string pfn = req.bodyfields.get<std::string>("pfn", "");
    std::string forcerecalc_str = req.bodyfields.get<std::string>("force-recalc", "false");

    bool forcerecalc = false;
    if(forcerecalc_str == "false" || forcerecalc_str == "0" || forcerecalc_str == "no") {
      forcerecalc = false;
    } else if(forcerecalc_str == "true" || forcerecalc_str == "1" || forcerecalc_str == "yes") {
      forcerecalc = true;
    }

    // unconditional calculation of checksum?
    if(status.role == status.roleDisk || forcerecalc) {
      calculateChecksum(req.object, pfn);
      std::ostringstream os("Checksum calculation initiated and is pending");
      return DomeReq::SendSimpleResp(request, 202, os);
    }

    // i am a head node, and not forced to do a recalc
    // check if the checksum exists in the db

    // first the lfn checksum
    std::string lfnchecksum;
    ExtendedStat xstat = catalog->extendedStat(req.object);

    if(xstat.hasField(fullchecksum)) {
      lfnchecksum = xstat.getString(fullchecksum);
    }

    // then the pfn
    std::string pfnchecksum;

    std::ostringstream os;
    // if(pfn != "") {
    //   std::vector<Replica> replicas = catalog->getReplicas(req.object);
    //   for(std::vector<Replica>::iterator it = replicas.begin(); it != replicas.end(); it++) {
    //     os << "Found replica with id " << it->replicaId << std::endl;
    //   }
    // }
    //os << " { \"checksum\" : \"" << xstat.getString("checksum." + chksumtype) << "\" }";
    return DomeReq::SendSimpleResp(request, 200, os);
  }
  catch (dmlite::DmException& e) {
    std::ostringstream os;
    os << "An error has occured - file not found?\r\n";
    os << "Dmlite exception: " << e.what();
    return DomeReq::SendSimpleResp(request, 404, os);
  }

  std::ostringstream os;
  os << "Unknown error" << std::endl;
  return DomeReq::SendSimpleResp(request, 404, os);
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
