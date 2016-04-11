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
#include "utils/DomeUtils.h"
#include "utils/Config.hh"
#include <sys/vfs.h>
#include <unistd.h>
#include "utils/DomeTalker.h"
#include <values.h>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <sys/param.h>


#include "cpp/dmlite.h"
#include "cpp/catalog.h"

using namespace dmlite;


DomeStatus::DomeStatus() {
  davixPool = NULL;
  lastfscheck = lastreload = 0;

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

  Log(Logger::Lvl1, domelogmask, domelogname, "My hostname is: " << myhostname);

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
    return sql.getFilesystems(*this);
  }
  
  // Disk node case. We ask the head node and match the
  // filesystems against the local server name
  std::string domeurl = CFG->GetString("disk.headnode.domeurl", (char *)"(empty url)/");
  domeurl += "/";
  
  DomeTalker talker(*davixPool, NULL, domeurl,
                    "GET", "dome_getspaceinfo");
  
  if(!talker.execute()) {
    Err(domelogname, "Error when issuing dome_getspaceinfo: " << talker.err());
    return -1;
  }
  
  Log(Logger::Lvl4, domelogmask, domelogname, "Head node answered: '" << talker.response() << "'");
  
  try {
    
    // Now overwrite the fs entries we have with the ones we got
    boost::unique_lock<boost::recursive_mutex> l(*this);
    
    // Loop on the servers of the response, looking for one that matches this server
    BOOST_FOREACH(const boost::property_tree::ptree::value_type &srv, talker.jresp().get_child("fsinfo")) {
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
    Err("checkDiskSpaces", "Could not process JSON: " << e.what() << " '" << talker.response() << "'");
    return -1;
  }
  
  return 0;
}

/// Helper function that reloads all the quotas from the DB
int DomeStatus::loadQuotatokens() {
  DomeMySql sql;

  return sql.getSpacesQuotas(*this);
}


int DomeStatus::insertQuotatoken(DomeQuotatoken &mytk) {

   boost::unique_lock<boost::recursive_mutex> l(*this);

    // Insert this quota, by overwriting any other quota that has the same path and same pool
    std::pair <std::multimap<std::string, DomeQuotatoken>::iterator, std::multimap<std::string, DomeQuotatoken>::iterator> myintv;
    myintv = quotas.equal_range(mytk.path);

    for (std::multimap<std::string, DomeQuotatoken>::iterator it = myintv.first;
       it != myintv.second;
       ++it) {
          if (it->second.poolname == mytk.poolname) {
            quotas.erase(it);
            break;
          }
        }
        
    quotas.insert( std::pair<std::string, DomeQuotatoken>(mytk.path, mytk) );
    
    return 0;
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


int DomeStatus::tick(time_t timenow) {
  
  Log(Logger::Lvl4, domelogmask, domelogname, "Tick. Now: " << timenow);
  
  // Give life to the queues
  checksumq->tick();
  filepullq->tick();
  
  this->tickChecksums();
  this->tickFilepulls();
  // -----------------------------------
  // Actions to be performed less often...
  // -----------------------------------
  
  
  
  
  // Actions to be performed less often...
  if ( timenow - lastreload >= CFG->GetLong("glb.reloadfsquotas", 60)) {
    // At regular intervals, one minute or so,
    // reloading the filesystems and the quotatokens is a good idea
    Log(Logger::Lvl4, domelogmask, domelogname, "Reloading quotas.");
    loadQuotatokens();   
    
    lastreload = timenow;
  }
  
  if ( timenow - lastfscheck >= CFG->GetLong("glb.fscheckinterval", 60)) {
    // At regular intervals, one minute or so,
    // checking the filesystems is a good idea for a disk server
    Log(Logger::Lvl4, domelogmask, domelogname, "Checking disk spaces.");

    if (role == roleDisk)
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

    SecurityCredentials creds;
    creds.clientName = qualifiers[5];
    creds.remoteAddress = qualifiers[6];

    // send pull command to the disk to initiate calculation
    Log(Logger::Lvl1, domelogmask, domelogname, "Contacting disk server " << server << " for pulling '" << rfn << "'");
    std::string diskurl = "https://" + server + "/domedisk/" + lfn;


    DomeTalker talker(*davixPool, &creds, diskurl,
                      "POST", "dome_pull");

    if(!talker.execute("lfn", lfn, "pfn", DomeUtils::pfn_from_rfio_syntax(rfn))) {
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

    SecurityCredentials creds;
    creds.clientName = qualifiers[3];
    creds.remoteAddress = qualifiers[4];

    // send dochksum to the disk to initiate calculation
    Log(Logger::Lvl3, domelogmask, domelogname, "Contacting disk server " << server << " for checksum calculation.");
    std::string diskurl = "https://" + server + "/domedisk/" + lfn;

    DomeTalker talker(*davixPool, &creds, diskurl,
                      "POST", "dome_dochksum");

    boost::property_tree::ptree params;
    params.put("checksum-type", checksumtype);
    params.put("update-lfn-checksum", DomeUtils::bool_to_str(updateLfnChecksum));
    params.put("pfn", DomeUtils::pfn_from_rfio_syntax(rfn));

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
      
      Log(Logger::Lvl4, domelogmask, domelogname, "Found quotatoken '" << it->second.u_token << "' of pool: '" <<
      it->second.poolname << "' matches path '" << path << "' quotatktotspace: " << it->second.t_space);
      
      return 0;
    }
  }
  
  Log(Logger::Lvl4, domelogmask, domelogname, "No quotatoken found for pool: '" <<
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
  
  Log(Logger::Lvl4, domelogmask, domelogname, "No quotatoken found for pool: '" <<
      poolname << "' path '" << path << "'");
  return 1;
}
  

static bool predFsMatchesPool(DomeFsInfo &fsi, std::string &pool) {
    return ( fsi.poolname == pool );
}

int DomeStatus::rmPoolfs(std::string &poolname) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Removing filesystems of pool: '" << poolname << "'");
  
  // Lock status!
  boost::unique_lock<boost::recursive_mutex> l(*this);
  
  std::vector<DomeFsInfo>::iterator new_end;
  
  new_end = std::remove_if(fslist.begin(), fslist.end(), boost::bind(&predFsMatchesPool, _1, poolname));
  fslist.erase(new_end, fslist.end()); 
  
  Log(Logger::Lvl3, domelogmask, domelogname, "Removed filesystems of pool: '" << poolname << "'");
  return 0;
  
}

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

int DomeStatus::addPoolfs(std::string &srv, std::string &newfs, std::string &poolname) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Adding filesystem. srv: '" << srv << "' fs: '" << newfs << "' pool: '" << poolname << "'");
  
  // Lock status!
  boost::unique_lock<boost::recursive_mutex> l(*this);
  
  DomeFsInfo fsi;
  fsi.poolname = poolname;
  fsi.server = srv;
  fsi.fs = newfs;
  
  // Make sure it's not already there or that we are not adding a parent/child of an existing fs
  for (std::vector<DomeFsInfo>::iterator fs = fslist.begin(); fs != fslist.end(); fs++) {
    if ( PfnMatchesFS(srv, newfs, *fs) )
      return 1;
  }
  
  fslist.push_back(fsi);
  return 0;
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
    
    if (myintv.first != quotas.end()) {
      for (std::multimap<std::string, DomeQuotatoken>::iterator it = myintv.first; it != myintv.second; ++it) {
          
          Log(Logger::Lvl4, domelogmask, domelogname, "pool: '" << it->second.poolname << "' matches path '" << lfn);    
          
          // Now loop on the FSs belonging to this pool
          // and check if at least one of them can pull files in from external sources
          for (std::vector<DomeFsInfo>::iterator fs = fslist.begin(); fs != fslist.end(); fs++) {
            if ((fs->poolname == it->second.poolname) && fs->canPullFile()) {
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


bool DomeStatus::LfnMatchesPool(std::string lfn, std::string pool) {
  
  // Lock status!
  boost::unique_lock<boost::recursive_mutex> l(*this);
  std::string lfn1(lfn);
  
  while (lfn1.length() > 0) {
    
    Log(Logger::Lvl4, domelogmask, domelogname, "Processing: '" << lfn1 << "'");
    // Check if any matching quotatoken exists
    std::pair <std::multimap<std::string, DomeQuotatoken>::iterator, std::multimap<std::string, DomeQuotatoken>::iterator> myintv;
    myintv = quotas.equal_range(lfn1);
    
    if (myintv.first != quotas.end()) {
      for (std::multimap<std::string, DomeQuotatoken>::iterator it = myintv.first; it != myintv.second; ++it) {
        if (it->second.poolname == pool) {
          
          Log(Logger::Lvl1, domelogmask, domelogname, "pool: '" << it->second.poolname << "' matches path '" << lfn);    
          
          return true;
        }
      }
    }
    
    // No match found, look upwards by trimming the last token from absPath
    size_t pos = lfn1.rfind("/");
    lfn1.erase(pos);
  }
  return false;
  
}

long long DomeStatus::getPathFreeSpace(const std::string &path) {
  // Lock status!
  boost::unique_lock<boost::recursive_mutex> l(*this);
  std::string lfn1(path);
  
  
  // Loop, going up in the parent directory hierarchy
  while (lfn1.length() > 0) {
    long long totfree = 0LL;  
    long long totused = 0LL;
    Log(Logger::Lvl4, domelogmask, domelogname, "Processing: '" << lfn1 << "'");
    
    // Check if any matching quotatoken exists, with enough free space
    std::pair <std::multimap<std::string, DomeQuotatoken>::iterator, std::multimap<std::string, DomeQuotatoken>::iterator> myintv;
    myintv = quotas.equal_range(lfn1);
    
    // Any quotatokens here ?
    if (myintv.first != quotas.end()) {
      
      // Gets the used space in the dir
      DmlitePoolHandler stack(dmpool);
      ExtendedStat st;
      // Try to get the used space in the dir of the quotatoken
      try {
        st = stack->getCatalog()->extendedStat(lfn1);
        totused = st.stat.st_size;
      }
      catch (DmException e) {
        std::ostringstream os;
        os << "Found quotatokens for non-existing path '"<< path << "' : " << e.code() << "-" << e.what();
      
        Err(domelogname, os.str());
        continue;
      }
          
      // Loop on all the quotatokens of this dir  
      for (std::multimap<std::string, DomeQuotatoken>::iterator it = myintv.first; it != myintv.second; ++it) {
        long long pooltotal, poolfree;
        int poolst;
        
        // Gets the free space in the pool pointed to by this quotatoken
        if (!getPoolSpaces(it->second.poolname, pooltotal, poolfree, poolst)) {
          Log(Logger::Lvl1, domelogmask, domelogname, "pool: '" << it->second.poolname << "' matches path '" << lfn1 << "' poolfree: " << poolfree << " poolstatus: " << poolst);    
          
          // Consider as free space the minimum between free space on the pool and quota size in the same pool
          totfree += MIN(poolfree, it->second.t_space);
        }
      }
      
      // Remember, with quotatokens we exit at the first match in the parent directories
      return MAX( 0, totfree - totused );
    }
    
    // No match found, look upwards by trimming the last token from absPath
    size_t pos = lfn1.rfind("/");
    lfn1.erase(pos);
  }
  return 0;
}


bool DomeStatus::LfnFitsInFreespace(std::string lfn, size_t space) {
  
  // Calculate parent path
  std::string parent;
  
  return (getPathFreeSpace(parent) > (long long)space);
  
}

bool DNMatchesHost(std::string dn, std::string host) {
  std::string s = "/CN="+host;
  
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



