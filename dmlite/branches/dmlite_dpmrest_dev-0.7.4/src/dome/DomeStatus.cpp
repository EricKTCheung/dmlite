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
#include "utils/Config.hh"
#include <sys/vfs.h>
#include <unistd.h>
#include "utils/DavixPool.h"
#include <values.h>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

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
  
}
long DomeStatus::getGlobalputcount() {
  boost::unique_lock<boost::recursive_mutex> l(*this);
  
  globalputcount = ++globalputcount % MAXINT;
  
  return (globalputcount);
}


/// Helper function that adds a filesystem to the list and its corresponding server to the server list
/// Filesystems so far can't be deleted without a restart
int DomeStatus::addFilesystem(DomeFsInfo &fs) {
}

/// Helper function that reloads all the filesystems from the DB
int DomeStatus::loadFilesystems() {
  DomeMySql sql;
  
  return sql.getFilesystems(*this);
}

/// Helper function that reloads all the quotas from the DB
int DomeStatus::loadQuotatokens() {
  DomeMySql sql;
  
  return sql.getSpacesQuotas(*this);
}

int DomeStatus::getPoolSpaces(std::string &poolname, long long &total, long long &free) {
  total = 0LL;
  free = 0LL;
  boost::unique_lock<boost::recursive_mutex> l(*this);
  
  // Loop over the filesystems and just sum the numbers
  for (int i = 0; i < fslist.size(); i++)
    if (fslist[i].poolname == poolname) {
      total += fslist[i].physicalsize;
      free += fslist[i].freespace;
    }
    
    return 0;
}

int DomeStatus::tick(time_t timenow) {
  
  Log(Logger::Lvl4, domelogmask, domelogname, "Tick. Now: " << timenow);
  
  // Give life to the queues
  checksumq->tick();
  filepullq->tick();
  
  
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
    
    checkDiskSpaces();
    
    lastfscheck = timenow;
  }
  
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
    
    for (int i = 0; i < fslist.size(); i++) {
      
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
      int errcode = 0;
      
      // https is mandatory to contact disk nodes, as they must be able to apply
      // decent authorization rules
      std::string url = "https://" + *servername +
      CFG->GetString("head.diskdomemgmtsuffix", (char *)"/domedisk/");
      Davix::Uri uri(url);
      
      Davix::DavixError* tmp_err = NULL;
      DavixGrabber hdavix(*davixPool);
      DavixStuff *ds(hdavix);
      Davix::GetRequest req(*ds->ctx, url, &tmp_err);
      
      if( tmp_err ){
        Err("checkDiskSpaces", "Cannot initialize Query to" << url << ", Error: "<< tmp_err->getErrMsg());
        continue;
      }
      
      req.addHeaderField("cmd", "dome_getspaceinfo");
      
      // Set decent timeout values for the operation
      req.setParameters(ds->parms);
      
      if (req.executeRequest(&tmp_err) == 0)
        errcode = req.getRequestCode();
      
      // Prepare the text status message to display
      boost::property_tree::ptree myresp;
      
      if (tmp_err) {
        Err("checkDiskSpaces", "HTTP status error on" << url << ", Error: "<< tmp_err->getErrMsg());
        // Here we had an error of some kind while trying to contact the disk server
        // We don't distinguish between TCP and HTTP errors, in any case we will disable the filesystems
      } else {
        
        // Here we have the response... splat its body into a ptree to parse it
        std::istringstream is(req.getAnswerContent());
        
        Log(Logger::Lvl4, domelogmask, domelogname, "Disk server: " << *servername << " answered: '" << is.str() << "'");
        
        try {
          boost::property_tree::read_json(is, myresp);
          
        } catch (boost::property_tree::json_parser_error e) {
          Err("checkDiskSpaces", "Could not process JSON: " << e.what() << " '" << is.str() << "'");
          continue;
        }
      }
      
      // Now process the values from the Json, or just disable the filesystems. Lock the status!
      {
        boost::unique_lock<boost::recursive_mutex> l(*this);
        
        // Loop through the server names, childs of fsinfo
        
        if (!tmp_err) {
          BOOST_FOREACH(const boost::property_tree::ptree::value_type &srv, myresp.get_child("fsinfo")) {
            // v.first is the name of the server.
            // v.second is the child tree representing the server
            
            // Now we loop through the filesystems reported by this server
            BOOST_FOREACH(const boost::property_tree::ptree::value_type &fs, srv.second) {
              // v.first is the name of the server.
              // v.second is the child tree representing the server
              
              // Find the corresponding server:fs info in our array, and get the counters
              Log(Logger::Lvl4, domelogmask, domelogname, "Processing: " << srv.first << " " << fs.first);
              for (int ii = 0; ii < fslist.size(); ii++) {
                
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
        } // if tmp_err
        
        else {
          // The communication with the server had problems
          // Disable all the filesystems belonging to that server
          Log(Logger::Lvl4, domelogmask, domelogname, "Disabling filesystems for server '" << *servername << "'");
          for (int ii = 0; ii < fslist.size(); ii++) {
            if (fslist[ii].server == *servername) {
              // If there was some error, disable the filesystem and give a clear warning
              Err(domelogname, "Server down or other trouble. Disabling filesystem: '" << fslist[ii].server << " " << fslist[ii].fs << "'");
              fslist[ii].activitystatus = DomeFsInfo::FsBroken;
            }
            
          } // loop fs
          
        } // if tmp_err
        
        
      } // lock
      
      
      
      // Here we should disable all the filesystems that belong to the given server
      // that were absent in the successful server's response.
      // Not critical by now, may be useful in the future
      
      
    } // for
    Log(Logger::Lvl3, domelogmask, domelogname, "Exiting.");
  } // if role head
  
  
}
