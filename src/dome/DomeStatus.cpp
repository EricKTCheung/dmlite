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



/** @file   DomeStatus.cpp
 * @brief  A helper class that describes the internal status of the storage system, pools, disks, etc
 * @author Fabrizio Furano
 * @date   Jan 2016
 */


#include "DomeStatus.h"
#include "DomeMysql.h"
#include "DomeLog.h"
#include "DomeReq.h"
#include "utils/DomeUtils.h"
#include "utils/Config.hh"
#include <sys/vfs.h>
#include <unistd.h>
#include "utils/DomeTalker.h"
#include <values.h>
#include <algorithm>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include <fstream>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <sys/param.h>
#include <stdio.h>

#include "cpp/utils/urls.h"
#include "cpp/dmlite.h"
#include "cpp/catalog.h"

using namespace dmlite;


bool DomeFsInfo::canPullFile(DomeStatus &st) {
  char pooltype;
  int64_t defsz;
  st.getPoolInfo(poolname, defsz, pooltype);
  return ( ((pooltype == 'V') || (pooltype == 'v')) && (freespace > defsz) );
}

DomeStatus::DomeStatus() {
  davixPool = NULL;
  lastreloadusersgroups = lastfscheck = lastreload = 0;

  struct addrinfo hints, *info, *p;
  int gai_result;

  globalputcount = 0;

  // Horrible kludge to get our hostname
  char hostname[1024];
  hostname[1023] = '\0';
  gethostname(hostname, 1023);

  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC; /*either IPV4 or IPV6*/
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_CANONNAME;

  if ((gai_result = getaddrinfo(hostname, "http", &hints, &info)) != 0) {
    fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(gai_result));
    exit(1);
  }

  for(p = info; p != NULL; p = p->ai_next) {
    if (p->ai_canonname && strlen(p->ai_canonname) > myhostname.size())
      myhostname =  p->ai_canonname;
  }

  freeaddrinfo(info);


  Log(Logger::Lvl1, domelogmask, domelogname, "My automatically detected hostname is: " << myhostname);
  myhostname = CFG->GetString("glb.myhostname", myhostname.c_str());
  Log(Logger::Lvl1, domelogmask, domelogname, "Overriding my hostname to: " << myhostname);

  // Create a dmlite pool
  dmpool = new DmlitePool(CFG->GetString("glb.dmlite.configfile", (char *)"/etc/dmlite.conf"));

}
long DomeStatus::getGlobalputcount() {
  boost::unique_lock<boost::recursive_mutex> l(*this);

  globalputcount++;
  globalputcount %= MAXINT;

  return (globalputcount);
}


/// Helper function that reloads all the filesystems from the DB or asking the head node
int DomeStatus::loadFilesystems() {

  if (role == roleHead) {
    DomeMySql sql;
    sql.getPools(*this);
    return sql.getFilesystems(*this);
  }

  // Disk node case. We ask the head node and match the
  // filesystems against the local server name
  std::string domeurl = CFG->GetString("disk.headnode.domeurl", (char *)"(empty url)/");
  domeurl += "/";

  DomeTalker talker(*davixPool, NULL, domeurl,
                    "GET", "dome_getspaceinfo");

  if(!talker.execute()) {
    Err(domelogname, talker.err());
    return -1;
  }

  Log(Logger::Lvl4, domelogmask, domelogname, "Head node answered: '" << talker.response() << "'");

  try {

    // Now overwrite the fs entries we have with the ones we got
    boost::unique_lock<boost::recursive_mutex> l(*this);

    // Loop on the servers of the response, looking for one that matches this server
    BOOST_FOREACH(const boost::property_tree::ptree::value_type &srv, talker.jresp().get_child("fsinfo.")) {
      // v.first is the name of the server.
      // v.second is the child tree representing the server
      if (srv.first != this->myhostname) continue;

      // Now loop on the filesystems of the response
      // Now we loop through the filesystems reported by this server
      BOOST_FOREACH(const boost::property_tree::ptree::value_type &fs, srv.second) {
        // v.first is the name of the server.
        // v.second is the child tree representing the server


        // Find the corresponding server:fs info in our array, and get the counters
        bool found = false;
        Log(Logger::Lvl4, domelogmask, domelogname, "Processing: " << srv.first << " " << fs.first);
        for (unsigned int ii = 0; ii < fslist.size(); ii++) {

          Log(Logger::Lvl4, domelogmask, domelogname, "Checking: " << fslist[ii].server << " " << fslist[ii].fs);
          if (fslist[ii].fs == fs.first) {
            found = true;
            break;
          }
        }
        if (!found) {
          Log(Logger::Lvl1, domelogmask, domelogname, "Learning new fs from head node: " << srv.first << ":" << fs.first <<
          " poolname: '" << fs.second.get<std::string>("poolname") << "'" );

          DomeFsInfo newfs;
          newfs.poolname = fs.second.get<std::string>("poolname");;
          newfs.server = myhostname;
          servers.insert(myhostname);
          newfs.fs = fs.first;

          fslist.push_back(newfs);
        }

      } // foreach


    } // foreach

  }
  catch (boost::property_tree::ptree_error &e) {
    Err("loadFilesystems", "Could not process JSON: " << e.what() << " '" << talker.response() << "'");
    return -1;
  }

  return 0;
}

/// Helper function that reloads all the quotas from the DB
int DomeStatus::loadQuotatokens() {
  DomeMySql sql;

  return sql.getSpacesQuotas(*this);
}

/// Helper function that reloads all the users from the DB. Returns 0 on failure
int DomeStatus::loadUsersGroups() {

  if (role != roleHead) return 1;

  DomeMySql sql;

  sql.getUsers(*this);

  sql.getGroups(*this);

  // Make sure that group 0 (root) always exists
  DomeGroupInfo gi;
  if ( !getGroup(0, gi) ) {
    gi.banned = 0;
    gi.groupid = 0;
    gi.groupname = "root";
    gi.xattr = "";
    insertGroup(gi);
  }

  // And now also load the gridmap file
  int cnt = 0;
  FILE *mf;
  std::string gridmapfile = CFG->GetString("head.gridmapfile", (char *)"/etc/lcgdm-mapfile");
  char buf[1024];

  if ((mf = fopen(gridmapfile.c_str(), "r")) == NULL) {
    buf[0] = '\0';
    strerror_r(errno, buf, 1024);
    Err("loadUsersGroups", "Could not process gridmap file: '" << gridmapfile << "' err: " << errno << "-" << buf);
    return 0;
  }


  char *p, *q;
  char *user, *vo;

  boost::unique_lock<boost::recursive_mutex> l(*this);
  gridmap.clear();

  while (fgets(buf, sizeof(buf), mf)) {
    buf[strlen (buf) - 1] = '\0';
    p = buf;

    // Skip leading blanks
    while (isspace(*p))
      p++;

    if (*p == '\0') continue; // Empty line
    if (*p == '#') continue;  // Comment

    if (*p == '"') {
      q = p + 1;
      if ((p = strrchr (q, '"')) == NULL) continue;
    }
    else {
      q = p;
      while (!isspace(*p) && *p != '\0')
        p++;
      if (*p == '\0') continue; // No VO
    }

    *p = '\0';
    user = q;
    p++;

    // Skip blanks between DN and VO
    while (isspace(*p))
      p++;
    q = p;

    while (!isspace(*p) && *p != '\0' && *p != ',')
      p++;
    *p = '\0';
    vo = q;

    // Insert
    //mfe->voForDn[user] = vo;

    Log(Logger::Lvl5, domelogmask, domelogname, "Mapfile DN: " << user << " -> " << vo);

    gridmap.insert( std::pair<std::string,std::string>(user, vo) );
    cnt++;
  }

  
  Log(Logger::Lvl1, domelogmask, domelogname, "Loaded " << cnt << " mapfile entries.");

  if (fclose(mf))
    Err(domelogname, "Error closing file '" << gridmapfile.c_str() << "'");

  return 1;
}

void DomeStatus::updateQuotatokens(const std::vector<DomeQuotatoken> &tokens) {
   boost::unique_lock<boost::recursive_mutex> l(*this);

   // overwrite all quotatokens with those in the vector
   quotas.clear();

   for(size_t i = 0; i < tokens.size(); i++) {
     quotas.insert(std::pair<std::string, DomeQuotatoken>(tokens[i].path, tokens[i]));
   }
}

int DomeStatus::getPoolSpaces(std::string &poolname, long long &total, long long &free, int &poolstatus) {
  total = 0LL;
  free = 0LL;
  bool rc = 1;
  poolstatus = DomeFsInfo::FsStaticDisabled;
  boost::unique_lock<boost::recursive_mutex> l(*this);

  // Loop over the filesystems and just sum the numbers
  for (unsigned int i = 0; i < fslist.size(); i++)
    if (fslist[i].poolname == poolname) {

      if (fslist[i].isGoodForRead()) {

        if (poolstatus == DomeFsInfo::FsStaticDisabled)
          poolstatus = DomeFsInfo::FsStaticReadOnly;

        if (fslist[i].isGoodForWrite()) {
          free += fslist[i].freespace;
          poolstatus = DomeFsInfo::FsStaticActive;
        }

        total += fslist[i].physicalsize;
      }



      rc = 0;
    }

  return rc;
}

bool DomeStatus::existsPool(std::string &poolname) {

  boost::unique_lock<boost::recursive_mutex> l(*this);

  // Loop over the filesystems and just sum the numbers
  for (unsigned int i = 0; i < fslist.size(); i++)
    if (fslist[i].poolname == poolname) {
      return true;
    }

  return false;
}

bool DomeStatus::getPoolInfo(std::string &poolname, int64_t &pool_defsize, char &pool_stype) {

  boost::unique_lock<boost::recursive_mutex> l(*this);

  // Loop over the filesystems and just sum the numbers
  for (unsigned int i = 0; i < fslist.size(); i++)
    if (fslist[i].poolname == poolname) {
      try {
        pool_defsize = poolslist[fslist[i].poolname].defsize;
        pool_stype = poolslist[fslist[i].poolname].stype;
      }catch ( ... ) {
        pool_defsize = 0;
        pool_stype = 'P';
      }
      return true;
    }

  return false;
}



void DomeStatus::notifyQueues() {
  //boost::unique_lock<boost::mutex> lock(queue_mtx);
  queue_cond.notify_one();
}

void DomeStatus::waitQueues() {

  boost::unique_lock<boost::mutex> lock(queue_mtx);
  int dur = (int)CFG->GetLong("glb.tickfreq", 10);
  boost::system_time const timeout=boost::get_system_time()+ boost::posix_time::seconds(dur);
  queue_cond.timed_wait(lock, timeout);

}

int DomeStatus::tickQueues(time_t timenow) {

  Log(Logger::Lvl4, domelogmask, domelogname, "Tick. Now: " << timenow);

  // Give life to the queues
  checksumq->tick();
  filepullq->tick();

  this->tickChecksums();
  this->tickFilepulls();

  return 0;
}

int DomeStatus::tick(time_t timenow) {

  Log(Logger::Lvl4, domelogmask, domelogname, "Tick. Now: " << timenow);


  // -----------------------------------
  // Actions to be performed less often...
  // -----------------------------------




  // Actions to be performed less often...
  if ( this->role == this->roleHead && (timenow - lastreload >= CFG->GetLong("glb.reloadfsquotas", 60)) ) {
    // At regular intervals, one minute or so,
    // reloading the filesystems and the quotatokens is a good idea
    Log(Logger::Lvl4, domelogmask, domelogname, "Reloading quotas.");
    loadQuotatokens();

    lastreload = timenow;
  }

  if ( this->role == this->roleHead && (timenow - lastreloadusersgroups >= CFG->GetLong("glb.reloadusersgroups", 60)) ) {
    // At regular intervals, one minute or so,
    // reloading the users and groups tables is a good idea
    Log(Logger::Lvl4, domelogmask, domelogname, "Reloading users/groups.");
    loadUsersGroups();

    lastreloadusersgroups = timenow;
  }

  if ( timenow - lastfscheck >= CFG->GetLong("glb.fscheckinterval", 60)) {
    // At regular intervals, one minute or so,
    // checking the filesystems is a good idea for a disk server
    Log(Logger::Lvl4, domelogmask, domelogname, "Checking disk spaces.");

    //if (role == roleDisk)
      loadFilesystems();

    checkDiskSpaces();

    lastfscheck = timenow;
  }

  return 0;
}




/// Dispatch the running of file pulls to disk servers
void DomeStatus::tickFilepulls() {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering.");
  boost::unique_lock<boost::recursive_mutex> l(*this);

  GenPrioQueueItem_ptr next;
  while((next = filepullq->getNextToRun()) != NULL) {
    Log(Logger::Lvl3, domelogmask, domelogname, "Scheduling file pull: " << next->namekey);

    // parse queue item contents
    std::vector<std::string> qualifiers = next->qualifiers;

    if(qualifiers.size() != 7) {
      Err(domelogname, "INCONSISTENCY in the internal file pull queue. Internal error. Invalid size of qualifiers: " << qualifiers.size());
      continue;
    }

    std::string lfn = next->namekey;
    std::string server = next->qualifiers[2];
    std::string fs = next->qualifiers[3];
    std::string rfn = next->qualifiers[4];

    SecurityContext sec;
    sec.credentials.clientName = qualifiers[5];
    sec.credentials.remoteAddress = qualifiers[6];


    // Try getting the default space for the pool
    int64_t pool_defsize;
    char pool_stype;
    DomeFsInfo fsinfo;
    if(!PfnMatchesAnyFS(server, fs, fsinfo)) {
      Err("dome_pull", SSTR("pfn does not match any of the filesystems of this server."));
      continue;
    }
    if (!getPoolInfo(fsinfo.poolname, pool_defsize, pool_stype)) {
      Err("dome_pull", SSTR("Can't get pool for fs: '" << fsinfo.server << ":" << fsinfo.fs));
      continue;
    }

    // send pull command to the disk to initiate calculation
    Log(Logger::Lvl1, domelogmask, domelogname, "Contacting disk server " << server << " for pulling '" << rfn << "'");
    std::string diskurl = "https://" + server + "/domedisk/";



    DomeTalker talker(*davixPool, &sec, diskurl,
                      "POST", "dome_pull");

    boost::property_tree::ptree params;
    params.put("lfn", lfn);
    params.put("pfn", DomeUtils::pfn_from_rfio_syntax(rfn));
    params.put("neededspace", pool_defsize);

    if(!talker.execute(params)) {
      Err(domelogname, "ERROR when issuing dome_pull to diskserver: " << talker.err());
      continue;
    }
  }
  Log(Logger::Lvl4, domelogmask, domelogname, "Exiting.");
}






void DomeStatus::tickChecksums() {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering.");
  boost::unique_lock<boost::recursive_mutex> l(*this);

  GenPrioQueueItem_ptr next;
  while((next = checksumq->getNextToRun()) != NULL) {
    Log(Logger::Lvl3, domelogmask, domelogname, "Scheduling calculation of checksum: " << next->namekey);

    // parse queue item contents
    std::vector<std::string> qualifiers = next->qualifiers;
    std::vector<std::string> namekey = DomeUtils::split(next->namekey, "[#]");

    if(namekey.size() != 3) {
      Log(Logger::Lvl1, domelogmask, domelogname, "INCONCISTENCY in the internal checksum queue. Invalid namekey: " << next->namekey);
      continue;
    }

    if(qualifiers.size() != 5) {
      Log(Logger::Lvl1, domelogmask, domelogname, "INCONCISTENCY in the internal checksum queue. Invalid size of qualifiers: " << qualifiers.size());
      continue;
    }

    std::string lfn = namekey[0];
    std::string rfn = namekey[1];
    std::string checksumtype = namekey[2];

    std::string server = qualifiers[1];
    bool updateLfnChecksum = DomeUtils::str_to_bool(qualifiers[2]);

    SecurityContext sec;
    sec.credentials.clientName = qualifiers[3];
    sec.credentials.remoteAddress = qualifiers[4];

    // send dochksum to the disk to initiate calculation
    Log(Logger::Lvl3, domelogmask, domelogname, "Contacting disk server " << server << " for checksum calculation.");
    std::string diskurl = "https://" + server + "/domedisk/";

    DomeTalker talker(*davixPool, &sec, diskurl,
                      "POST", "dome_dochksum");

    boost::property_tree::ptree params;
    params.put("checksum-type", checksumtype);
    params.put("update-lfn-checksum", DomeUtils::bool_to_str(updateLfnChecksum));
    params.put("pfn", DomeUtils::pfn_from_rfio_syntax(rfn));
    params.put("lfn", lfn);

    if(!talker.execute(params)) {
      Log(Logger::Lvl1, domelogmask, domelogname, "Error when issuing dome_dochksum: " << talker.err());
      continue;
    }
  }
  Log(Logger::Lvl4, domelogmask, domelogname, "Exiting.");
}

void DomeStatus::setDavixPool(dmlite::DavixCtxPool *pool) {
  davixPool = pool;
}


// In the case of a disk server, checks the free/used space in the mountpoints
void DomeStatus::checkDiskSpaces() {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering");

  if (role == roleDisk) {
    boost::unique_lock<boost::recursive_mutex> l(*this);


    // Loop over the filesystems, and check those that match our hostname

    int nfs = 0;

    for (unsigned int i = 0; i < fslist.size(); i++) {

      if ( fslist[i].server == myhostname ) {
        struct statfs buf;
        // this is supposed to be in this disk server. We statfs it, simply.
        int rc = statfs(fslist[i].fs.c_str(), &buf);
        if ( !rc ) {
          fslist[i].freespace = buf.f_bavail * buf.f_bsize;

          fslist[i].physicalsize = buf.f_blocks * buf.f_bsize;
          fslist[i].activitystatus = DomeFsInfo::FsOnline;
          Log(Logger::Lvl1, domelogmask, domelogname, "fs: " << fslist[i].fs << " phys: " << fslist[i].physicalsize << " free: " << fslist[i].freespace);

          nfs++;
        }
        else {
          Err("checkDiskSpaces", "statfs() on fs '" << fslist[i].fs << " returned " << rc << " errno:" << errno << ":" << strerror(errno));
          fslist[i].freespace = 0LL;
          fslist[i].physicalsize = 0LL;
          fslist[i].activitystatus = DomeFsInfo::FsBroken;
        }
      }

    }


    Log(Logger::Lvl1, domelogmask, domelogname, "Number of local filesystems: " << nfs);
  }

  if (role == roleHead) {
    // Head node case. We request dome_getspaceinfo to each server, then loop on the results and calculate the head numbers
    // If a server does not reply, mark as disabled all its filesystems

    std::set<std::string> srv;
    {
      // Let's work on a local copy, to avoid a longer locking
      boost::unique_lock<boost::recursive_mutex> l(*this);
      srv = servers;
    }
    // Contact all the servers, sequentially
    for (std::set<std::string>::iterator servername = srv.begin(); servername != srv.end(); servername++) {

      Log(Logger::Lvl4, domelogmask, domelogname, "Contacting disk server: " << *servername);

      // https is mandatory to contact disk nodes, as they must be able to apply
      // decent authorization rules
      std::string url = "https://" + *servername +
      CFG->GetString("head.diskdomemgmtsuffix", (char *)"/domedisk/");

      DomeTalker talker(*davixPool, NULL, url,
                        "GET", "dome_getspaceinfo");

      boost::property_tree::ptree myresp;
      bool haveJresp = false;

      if(!talker.execute()) {
        Err("checkDiskSpaces", "Error when issuing dome_getspaceinfo: " << talker.err());
      }
      else {
        Log(Logger::Lvl4, domelogmask, domelogname, "Disk server: " << *servername << " answered: '" << talker.response() << "'");
        try {
          myresp = talker.jresp();
          haveJresp = true;
        }
        catch (boost::property_tree::json_parser_error &e) {
          Err("checkDiskSpaces", "Could not process JSON: " << e.what() << " '" << talker.response() << "'");
          continue;
        }
      }

      // Now process the values from the Json, or just disable the filesystems. Lock the status!
      {
        boost::unique_lock<boost::recursive_mutex> l(*this);
        bool someerror = true;
        // Loop through the server names, childs of fsinfo

        if (haveJresp) {

          try {
            BOOST_FOREACH(const boost::property_tree::ptree::value_type &srv, myresp.get_child("fsinfo")) {
              // v.first is the name of the server.
              // v.second is the child tree representing the server

              // Now we loop through the filesystems reported by this server
              BOOST_FOREACH(const boost::property_tree::ptree::value_type &fs, srv.second) {
                // v.first is the name of the server.
                // v.second is the child tree representing the server

                // Find the corresponding server:fs info in our array, and get the counters
                Log(Logger::Lvl4, domelogmask, domelogname, "Processing: " << srv.first << " " << fs.first);
                for (unsigned int ii = 0; ii < fslist.size(); ii++) {

                  Log(Logger::Lvl4, domelogmask, domelogname, "Checking: " << fslist[ii].server << " " << fslist[ii].fs);
                  if ((fslist[ii].server == srv.first) && (fslist[ii].fs == fs.first)) {

                    Log(Logger::Lvl3, domelogmask, domelogname, "Matched: " << fslist[ii].server << " " << fslist[ii].fs);
                    Log(Logger::Lvl3, domelogmask, domelogname, "Getting: " << fs.second.get<long long>( "freespace", 0 ) << " " << fs.second.get<long long>( "physicalsize", 0 ));
                    fslist[ii].freespace = fs.second.get<long long>( "freespace", 0 );
                    fslist[ii].physicalsize = fs.second.get<long long>( "physicalsize", 0 );
                    int actst = fs.second.get<int>( "activitystatus", 0 );

                    // Clearly log the state transitions if there is one
                    if ((fslist[ii].activitystatus != (DomeFsInfo::DomeFsActivityStatus)actst) &&
                      ((DomeFsInfo::DomeFsActivityStatus)actst == DomeFsInfo::FsOnline) ) {

                      Log(Logger::Lvl1, domelogmask, domelogname, "Enabling filesystem: " << fslist[ii].server << " " << fslist[ii].fs);

                      }
                      fslist[ii].activitystatus = (DomeFsInfo::DomeFsActivityStatus)actst;
                  }




                } // loop fs
              } // foreach
            } // foreach

            someerror = false;

          }  catch (boost::property_tree::ptree_error e) {
            Err("checkDiskSpaces", "Error processing JSON response: " << e.what());
            continue;
          }

        } // if !tmp_err

        if (someerror) {
          // The communication with the server had problems
          // Disable all the filesystems belonging to that server
          Log(Logger::Lvl4, domelogmask, domelogname, "Disabling filesystems for server '" << *servername << "'");
          for (unsigned int ii = 0; ii < fslist.size(); ii++) {
            if (fslist[ii].server == *servername) {
              // If there was some error, disable the filesystem and give a clear warning
              Err(domelogname, "Server down or other trouble. Disabling filesystem: '" << fslist[ii].server << " " << fslist[ii].fs << "'");
              fslist[ii].activitystatus = DomeFsInfo::FsBroken;
            }

          } // loop fs
        } // if someerror
      } // lock

      // Here we should disable all the filesystems that belong to the given server
      // that were absent in the successful server's response.
      // Not critical by now, may be useful in the future


    } // for
    Log(Logger::Lvl3, domelogmask, domelogname, "Exiting.");
  } // if role head


}


bool DomeStatus::getQuotatoken(const std::string &s_token, DomeQuotatoken &tk) {
  for(std::multimap<std::string, DomeQuotatoken>::iterator it = quotas.begin(); it != quotas.end(); it++) {
    if(it->second.s_token == s_token) {
      tk = it->second;
      return true;
    }
  }
  return false;
}

int DomeStatus::getQuotatoken(const std::string &path, const std::string &poolname, DomeQuotatoken &tk) {

  std::pair <std::multimap<std::string, DomeQuotatoken>::iterator, std::multimap<std::string, DomeQuotatoken>::iterator> myintv;
  myintv = quotas.equal_range(path);


  for (std::multimap<std::string, DomeQuotatoken>::iterator it = myintv.first;
       it != myintv.second;
       ++it) {

    Log(Logger::Lvl4, domelogmask, domelogname, "Checking: '" << it->second.path << "' versus '" << path );
    // If the path of this quotatoken matches...
    if ( it->second.poolname == poolname ) {
      tk = it->second;

      Log(Logger::Lvl3, domelogmask, domelogname, "Found quotatoken '" << it->second.u_token << "' of pool: '" <<
      it->second.poolname << "' matches path '" << path << "' quotatktotspace: " << it->second.t_space);

      return 0;
    }
  }

  Log(Logger::Lvl3, domelogmask, domelogname, "No quotatoken found for pool: '" <<
      poolname << "' path '" << path << "'");
  return 1;
}



int DomeStatus::delQuotatoken(const std::string &path, const std::string &poolname, DomeQuotatoken &tk) {

  std::pair <std::multimap<std::string, DomeQuotatoken>::iterator, std::multimap<std::string, DomeQuotatoken>::iterator> myintv;
  myintv = quotas.equal_range(path);


  for (std::multimap<std::string, DomeQuotatoken>::iterator it = myintv.first;
       it != myintv.second;
       ++it) {

    Log(Logger::Lvl4, domelogmask, domelogname, "Checking: '" << it->second.path << "' versus '" << path );
    // If the path of this quotatoken matches...
    if ( it->second.poolname == poolname ) {
      tk = it->second;

      Log(Logger::Lvl4, domelogmask, domelogname, "Deleting quotatoken '" << it->second.u_token << "' of pool: '" <<
      it->second.poolname << "' matches path '" << path << "' quotatktotspace: " << it->second.t_space);

      quotas.erase(it);
      return 0;
    }
  }

  Log(Logger::Lvl3, domelogmask, domelogname, "No quotatoken found for pool: '" <<
      poolname << "' path '" << path << "'");
  return 1;
}


// static bool predFsMatchesPool(DomeFsInfo &fsi, std::string &pool) {
//     return ( fsi.poolname == pool );
// }

bool DomeStatus::PfnMatchesFS(std::string &server, std::string &pfn, DomeFsInfo &fs) {

  if (server != fs.server) return false;

  size_t pos = pfn.find(fs.fs);
    if (pos == 0) {
      // Here, a filesystem is a substring of the pfn. To be its parent filesystem,
      // either the string sizes are the same, or there is a slash in the pfn where the fs ends
      if (fs.fs.size() == pfn.size()) return true;
      if (pfn[fs.fs.size()] == '/') return true;
    }

  return false;

}

bool DomeStatus::PfnMatchesAnyFS(std::string &srv, std::string &pfn) {


    // Lock status!
  boost::unique_lock<boost::recursive_mutex> l(*this);

  // Loop on the filesystems, looking for one that is a proper substring of the pfn
  for (std::vector<DomeFsInfo>::iterator fs = fslist.begin(); fs != fslist.end(); fs++) {

    if (PfnMatchesFS(srv, pfn, *fs))
      return true;

  }

  return false;

}


bool DomeStatus::PfnMatchesAnyFS(std::string &srv, std::string &pfn, DomeFsInfo &fsinfo) {


    // Lock status!
  boost::unique_lock<boost::recursive_mutex> l(*this);

  // Loop on the filesystems, looking for one that is a proper substring of the pfn
  for (std::vector<DomeFsInfo>::iterator fs = fslist.begin(); fs != fslist.end(); fs++) {

    if (PfnMatchesFS(srv, pfn, *fs)) {
      fsinfo = *fs;
      return true;
    }

  }

  return false;

}

bool DomeStatus::LfnMatchesAnyCanPullFS(std::string lfn, DomeFsInfo &fsinfo) {

  // Lock status!
  boost::unique_lock<boost::recursive_mutex> l(*this);
  std::string lfn1(lfn);

  while (lfn1.length() > 0) {

    Log(Logger::Lvl4, domelogmask, domelogname, "Processing: '" << lfn1 << "'");
    // Check if any matching quotatoken exists
    std::pair <std::multimap<std::string, DomeQuotatoken>::iterator, std::multimap<std::string, DomeQuotatoken>::iterator> myintv;
    myintv = quotas.equal_range(lfn1);

    if (myintv.first != myintv.second) {
      for (std::multimap<std::string, DomeQuotatoken>::iterator it = myintv.first; it != myintv.second; ++it) {

          Log(Logger::Lvl4, domelogmask, domelogname, "pool: '" << it->second.poolname << "' matches path '" << lfn);

          // Now loop on the FSs belonging to this pool
          // and check if at least one of them can pull files in from external sources
          for (std::vector<DomeFsInfo>::iterator fs = fslist.begin(); fs != fslist.end(); fs++) {
            if ((fs->poolname == it->second.poolname) && fs->canPullFile(*this)) {
              Log(Logger::Lvl1, domelogmask, domelogname, "CanPull pool: '" << it->second.poolname << "' matches path '" << lfn);
              fsinfo = *fs;
              return true;
            }
          } // for

      } // for
    }

    // No match found, look upwards by trimming the last token from absPath
    size_t pos = lfn1.rfind("/");
    lfn1.erase(pos);
  }
  return false;

}

// which quotatoken should apply to lfn?
// return true and set the token, or return false if no tokens match
// If more than one quotatokens match then the first one is selected
// TODO: honour the fact that multiple quotatokens may match
bool DomeStatus::whichQuotatokenForLfn(const std::string &lfn, DomeQuotatoken &token) {
  Log(Logger::Lvl4, domelogmask, domelogname, "lfn: '" << lfn << "'");

  // lock status
  boost::unique_lock<boost::recursive_mutex> l(*this);

  std::string path = lfn;
  while( !path.empty() ) {
    Log(Logger::Lvl4, domelogmask, domelogname, "  checking '" << path << "'");

    typedef std::multimap<std::string, DomeQuotatoken>::iterator MapIter;
    std::pair<MapIter, MapIter> interval = quotas.equal_range(path);

    if(interval.first != interval.second) {
      Log(Logger::Lvl4, domelogmask, domelogname, " match for lfn '" << lfn << "'" << "and quotatoken " << interval.first->second.u_token);
      token = interval.first->second;
      return true;
    }

    // no match found, look upwards by trimming the last slash from path
    size_t pos = path.rfind("/");
    path.erase(pos);
  }

  Log(Logger::Lvl3, domelogmask, domelogname, " No quotatokens match lfn '" << lfn << "'");
  return false;
}

long long DomeStatus::getDirUsedSpace(const std::string &path) {
  ExtendedStat st;

  DomeMySql sql;
  DmStatus sts = sql.getStatbyLFN(st, path);
  if (!sts.ok()) {
    Err(domelogname, "Ignore exception stat-ing '" << path << "'");
    return 0;
  }

  return st.stat.st_size;
}

static bool isSubdir(const std::string &child, const std::string &parent) {
  if(child.size()+1 <= parent.size()) return false;

  // needed to correctly handle the case of /dir12/test, /dir1, which should return false
  if(child[child.size()-1] != '/' && child[parent.size()] != '/') return false;

  // child must start with parent
  if(child.compare(0, parent.size(), parent) != 0) return false;

  return true;
}

long long DomeStatus::getQuotatokenUsedSpace(const DomeQuotatoken &token) {
  Log(Logger::Lvl4, domelogmask, domelogname, "tk: '" << token.u_token);

  long long totused;
  boost::unique_lock<boost::recursive_mutex> l(*this);

  totused = getDirUsedSpace(token.path);
  Log(Logger::Lvl4, domelogmask, domelogname, "directory usage for '" << token.path << "': " << totused);

  typedef std::multimap<std::string, DomeQuotatoken>::iterator MapIter;
  MapIter it = quotas.lower_bound(token.path);
  if(it == quotas.end()) {
    Err(domelogname, "Error: getQuotatokenUsedSpace called on invalid quotatoken with path '" << token.path << "'");
    return -1;
  }

  it++; // it currently points to "token" itself - need to progress by one

  while(it != quotas.end() && isSubdir(it->second.path, token.path)) {
    Log(Logger::Lvl4, domelogmask, domelogname, "removing space of sub-quotatoken '" << it->second.u_token << "' (" << it->second.path << ")");
    totused -= getDirUsedSpace(it->second.path);

    // skip all potential subdirs of sub-quotatoken, or we'll seriously mess up the calculation
    std::string skip = it->second.path;

    it++;
    while(it != quotas.end() && isSubdir(it->second.path, skip)) {
      it++;
    }
  }
  return totused;
}

bool DomeStatus::fitsInQuotatoken(const DomeQuotatoken &token, const int64_t size) {
  long long totused = getQuotatokenUsedSpace(token);
  bool rc = ((token.t_space > totused) && (token.t_space - totused > size));
  Log(Logger::Lvl3, domelogmask, domelogname, "tk: '" << token.u_token << "' path: '" << token.path
      << "' size:" << size << " totused: " << totused << " outcome: " << rc);

  return rc;
}

bool DNMatchesHost(std::string dn, std::string host) {
  std::string s = "CN="+host;

  // Simple version, if the common name appears in the dn then we are right
  if (dn.find(s) != std::string::npos) return true;

  return false;
}

bool DomeStatus::isDNaKnownServer(std::string dn) {
  // We know this server if its DN matches our own hostname, it's us !
  if (DNMatchesHost(dn, myhostname)) return true;

  // We know this server if its DN matches the DN of the head node
  if (DNMatchesHost(dn, headnodename)) return true;

  // We know this server if its DN matches the hostname of a disk server
  for (std::set<std::string>::iterator i = servers.begin() ; i != servers.end(); i++) {
    if (DNMatchesHost(dn, *i)) return true;
  }

  // We don't know this server
  return false;
}



bool DomeStatus::canwriteintoQuotatoken(DomeReq &req, DomeQuotatoken &token) {


  // lock status
  boost::unique_lock<boost::recursive_mutex> l(*this);


  // True if one of the groups of the remote user matches the quotatk
  // Loop on the gids written in the quotatoken
  // For each of them, check if the user belongs to it
  for (unsigned int i = 0; i < token.groupsforwrite.size(); i++) {
    DmlitePoolHandler stack(dmpool);

    DomeGroupInfo gi;
    char *endptr;
    long int gid = strtol(token.groupsforwrite[i].c_str(), &endptr, 10);
    if ( (*endptr) || (errno == ERANGE && (gid == LONG_MAX || gid == LONG_MIN))
      || (errno != 0 && gid == 0)) {
        Err(domelogname, "gid: '" << token.groupsforwrite[i] <<
          "' in quotatoken '" << token.s_token << "' is not a gid. Wrong/corrupted info in quotatokens ?");
        continue;
      }

      if (!getGroup(gid, gi)) {
        Err(domelogname, "In quotatoken " << token.s_token << " group: '" << token.groupsforwrite[i] << "' gid: " <<
        gid << " unknown");
      continue;
      }

      if ( std::find(req.creds.groups.begin(), req.creds.groups.end(),
          gi.groupname) != req.creds.groups.end() ) {
            Log(Logger::Lvl3, domelogmask, domelogname, "group: '" << token.groupsforwrite[i] << "' gid: " <<
              gid << " can write in quotatoken " << token.s_token);

            return true;
      }
  }

  Err(domelogname, "Cannot write in quotatoken " << token.s_token);
  return false;
}

/// Gets user info from uid. Returns 0 on failure
int DomeStatus::getUser(int uid, DomeUserInfo &ui) {
  // lock status
  boost::unique_lock<boost::recursive_mutex> l(*this);

  try {
    ui = usersbyuid.at(uid);
  }
  catch ( ... ) {
    return 0;
  }

  return 1;

}

/// Gets user info from name. Returns 0 on failure
int DomeStatus::getUser(std::string username, DomeUserInfo &ui) {
  // lock status
  boost::unique_lock<boost::recursive_mutex> l(*this);

  try {
    ui = usersbyname.at(username);
  }
  catch ( ... ) {
    return 0;
  }

  return 1;

}
/// Gets group info from uid. Returns 0 on failure
int DomeStatus::getGroup(int gid, DomeGroupInfo &gi) {
  // lock status
  boost::unique_lock<boost::recursive_mutex> l(*this);

  try {
    gi = groupsbygid.at(gid);
  }
  catch ( ... ) {
    return 0;
  }

  return 1;
}
/// Gets user info from name. Returns 0 on failure
int DomeStatus::getGroup(std::string groupname, DomeGroupInfo &gi) {
  // lock status
  boost::unique_lock<boost::recursive_mutex> l(*this);

  try {
    gi = groupsbyname.at(groupname);
  }
  catch ( ... ) {
    return 0;
  }

  return 1;
}

/// Inserts/overwrites an user
int DomeStatus::insertUser(DomeUserInfo &ui) {
  // lock status
  boost::unique_lock<boost::recursive_mutex> l(*this);

  usersbyname[ui.username] = ui;
  usersbyuid[ui.userid] = ui;

  return 0;
}
/// Inserts/overwrites a group
int DomeStatus::insertGroup(DomeGroupInfo &gi) {
  // lock status
  boost::unique_lock<boost::recursive_mutex> l(*this);

  groupsbygid[gi.groupid] = gi;
  groupsbyname[gi.groupname] = gi;

  return 0;
}

std::string DomeQuotatoken::getGroupsString(bool putzeroifempty) {
  if (putzeroifempty && (groupsforwrite.size() == 0))
    return "0";

  return DomeUtils::join(",", groupsforwrite);
}
