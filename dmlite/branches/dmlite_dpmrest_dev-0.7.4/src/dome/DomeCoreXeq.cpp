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

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()


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

static bool str_to_bool(std::string str) {
  bool value = false;

  if(str == "false" || str == "0" || str == "no") {
    value = false;
  } else if(str == "true" || str == "1" || str == "yes") {
    value = true;
  }
  return value;
}

static std::string bool_to_str(bool b) {
  if(b) return "true";
  else return "false";
}

int DomeCore::calculateChecksum(std::string lfn, Replica replica, bool forcerecalc) {
  GenPrioQueueItem::QStatus qstatus = GenPrioQueueItem::Waiting;
  std::string namekey = lfn + "###" + replica.rfn;
  std::vector<std::string> qualifiers;

  qualifiers.push_back(""); // the first qualifier is common for all items,
                            // so the global limit triggers

  qualifiers.push_back(replica.server); // server name as second qualifier, so
                                        // the per-node limit triggers

  qualifiers.push_back(bool_to_str(forcerecalc));

  status.chksum->touchItemOrCreateNew(namekey, status, 0, qualifiers);
  return DomeReq::SendSimpleResp(request, 202, SSTR("Initiated checksum calculation on " << replica.rfn
                                                     << ", check back later"));
}

int DomeCore::calculateChecksumDisk(std::string lfn, std::string pfn, bool forcerecalc) {
  return DomeReq::SendSimpleResp(request, 202, "Initiated checksum calculation on disk node");
}

static Replica pickReplica(std::string lfn, std::string pfn, DmlitePoolHandler &stack) {
  std::vector<Replica> replicas = stack->getCatalog()->getReplicas(lfn);
  if(replicas.size() == 0) {
    throw DmException(DMLITE_CFGERR(ENOENT), "The provided LFN does not correspond to any replicas");
  }

  if(pfn != "") {
    for(std::vector<Replica>::iterator it = replicas.begin(); it != replicas.end(); it++) {
      if(it->rfn == pfn) {
        return *it;
      }
    }
    throw DmException(DMLITE_CFGERR(ENOENT), "The provided PFN does not correspond to any of LFN's replicas");
  }

  // no explicit pfn? pick a random replica
  int index = rand() % replicas.size();
  return replicas[index];
}

int DomeCore::dome_chksum(DomeReq &req, FCGX_Request &request) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering");

  try {
    DmlitePoolHandler stack(dmpool);

    std::string chksumtype = req.bodyfields.get<std::string>("checksum-type", "null");
    std::string fullchecksum = "checksum." + chksumtype;
    std::string pfn = req.bodyfields.get<std::string>("pfn", "");
    bool forcerecalc = str_to_bool(req.bodyfields.get<std::string>("force-recalc", "false"));

    // If I am a disk node, I need to unconditionally start the calculation
    if(status.role == status.roleDisk) {
      if(pfn == "") {
        return DomeReq::SendSimpleResp(request, 404, "pfn is obligatory when issuing a dome_chksum to a disk node");
      }
      return calculateChecksumDisk(req.object, pfn, forcerecalc);
    }

    // I am a head node
    if(forcerecalc) {
      Replica replica = pickReplica(req.object, pfn, stack);
      return calculateChecksum(req.object, replica, forcerecalc);
    }

    // Not forced to do a recalc - maybe I can find the checksums in the db
    std::string lfnchecksum;
    std::string pfnchecksum;
    Replica replica;

    // retrieve lfn checksum
    ExtendedStat xstat = stack->getCatalog()->extendedStat(req.object);
    if(xstat.hasField(fullchecksum)) {
      lfnchecksum = xstat.getString(fullchecksum);
    }

    // retrieve pfn checksum
    if(pfn != "") {
      replica = pickReplica(req.object, pfn, stack);
      if(replica.hasField(fullchecksum)) {
        pfnchecksum = replica.getString(fullchecksum);
      }
    }

    // can I send a response right now?
    if(lfnchecksum != "" && (pfn == "" || pfnchecksum != "")) {
      boost::property_tree::ptree jresp;
      jresp.put("checksum", lfnchecksum);
      if(pfn != "") {
        jresp.put("pfn-checksum", pfnchecksum);
      }
      return DomeReq::SendSimpleResp(request, 200, jresp);
    }

    // something is missing, need to calculate
    if(pfn == "") {
      replica = pickReplica(req.object, pfn, stack);
    }
    return calculateChecksum(req.object, replica, forcerecalc);
  }
  catch(dmlite::DmException& e) {
    std::ostringstream os("An error has occured.\r\n");
    os << "Dmlite exception: " << e.what();
    return DomeReq::SendSimpleResp(request, 404, os);
  }

  return DomeReq::SendSimpleResp(request, 500, "Something has gone terribly wrong.");
}

int DomeCore::dome_chksumstatus(DomeReq &req, FCGX_Request &request) {
  if(status.role == status.roleDisk) {
    return DomeReq::SendSimpleResp(request, 500, "chksumstatus only available on head nodes");
  }

  return DomeReq::SendSimpleResp(request, 500, "Unknown error");
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

int DomeCore::dome_getdirspaces(DomeReq &req, FCGX_Request &request) {

  // Crawl upwards the directory hierarchy of the given path
  // stopping when a matching one is found
  // The result is a list, as more than one quotatoken can be
  // assigned to a directory
  // The quota tokens indicate the pools that host the files written into
  // this directory subtree

  Log(Logger::Lvl4, domelogmask, domelogname, "Entering");

  std::string absPath =  req.bodyfields.get<std::string>("path", "");
  if ( !absPath.size() ) {
    std::ostringstream os;
    os << "Path '" << absPath << "' not found.";
    return DomeReq::SendSimpleResp(request, 404, os);
  }

  // Make sure it's an absolute lfn path
  if (absPath[0] != '/')  {
    std::ostringstream os;
    os << "Path '" << absPath << "' is not an absolute path.";
    return DomeReq::SendSimpleResp(request, 404, os);
  }

  // Remove any trailing slash
  while (absPath[ absPath.size()-1 ] == '/') {
    absPath.erase(absPath.size() - 1);
  }

  Log(Logger::Lvl4, domelogmask, domelogname, "Getting spaces for path: '" << absPath << "'");
  long long totspace = 0LL;
  long long usedspace = 0LL;
  long long poolfree = 0LL;
  // Crawl
  {
    boost::unique_lock<boost::recursive_mutex> l(status);
    while (absPath.length() > 0) {

      Log(Logger::Lvl4, domelogmask, domelogname, "Processing: '" << absPath << "'");
      // Check if any matching quotatoken exists
      std::pair <std::multimap<std::string, DomeQuotatoken>::iterator, std::multimap<std::string, DomeQuotatoken>::iterator> myintv;
      myintv = status.quotas.equal_range(absPath);

      if (myintv.first != status.quotas.end()) {
        for (std::multimap<std::string, DomeQuotatoken>::iterator it = myintv.first; it != myintv.second; ++it) {
          totspace += it->second.t_space;

          // Now find the free space in the mentioned pool
          long long ptot, pfree;
          status.getPoolSpaces(it->second.poolname, ptot, pfree);
          poolfree += pfree;

          Log(Logger::Lvl1, domelogmask, domelogname, "Quotatoken '" << it->second.u_token << "' of pool: '" <<
          it->second.poolname << "' matches path '" << absPath << "' totspace: " << totspace);
        }


        // Now get the size of this directory, using the dmlite catalog
        usedspace = 0;
        DmlitePoolHandler stack(dmpool);
        try {
          struct dmlite::ExtendedStat st = stack->getCatalog()->extendedStat(absPath);
          usedspace = st.stat.st_size;
        }
        catch (dmlite::DmException e) {
          Err(domelogname, "Ignore exception stat-ing '" << absPath << "'");
        }

        break;
      }

      // No match found, look upwards by trimming the last token from absPath
      size_t pos = absPath.rfind("/");
      absPath.erase(pos);
    }
  }
  // Prepare the response
  // NOTE:
  //  A pool may be assigned to many dir subtrees at the same time, hence
  //   the best value that we can publish for the pool is how much free space it has
  //  The quotatotspace is the sum of all the quotatokens assigned to this subtree
  //   In limit cases the tot quota can be less than the used space, hence it's better
  //   to publish the tot space rather than the free space in the quota.
  //   One of these cases could be when the sysadmin (with punitive mood) manually assigns
  //   a quota that is lower than the occupied space. This is a legitimate attempt
  //   to prevent clients from writing there until the space decreases...
  boost::property_tree::ptree jresp;
  jresp.put("quotatotspace", totspace);
  long long sp = (totspace - usedspace);
  jresp.put("quotafreespace", (sp < 0 ? 0 : sp));
  jresp.put("poolfreespace", poolfree);
  jresp.put("usedspace", usedspace);

  std::ostringstream os;
  boost::property_tree::write_json(os, jresp);
  int rc = DomeReq::SendSimpleResp(request, 200, os);


  Log(Logger::Lvl3, domelogmask, domelogname, "Result: " << rc);
  return rc;

}




int DomeCore::dome_pull(DomeReq &req, FCGX_Request &request) {};
int DomeCore::dome_dochksum(DomeReq &req, FCGX_Request &request) {};
int DomeCore::dome_statfs(DomeReq &req, FCGX_Request &request) {};
