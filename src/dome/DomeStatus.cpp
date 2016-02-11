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
  
  
  struct addrinfo hints, *info, *p;
  int gai_result;
  
  globalputcount = 0;
  
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
          Log(Logger::Lvl1, domelogmask, domelogname, "fs: " << fslist[i].fs << " phys: " << fslist[i].physicalsize << " free: " << fslist[i].freespace);
          nfs++;
        }
        else {
          Err("checkDiskSpaces", "statfs() on fs '" << fslist[i].fs << " returned " << rc << " errno:" << errno << ":" << strerror(errno));
        }
      }
      
    }
    
    
    Log(Logger::Lvl1, domelogmask, domelogname, "Number of local filesystems: " << nfs);
  }
  
  if (role == roleHead) {
    // Head node case. We request dome_getspaceinfo to each server, then loop on the results and calculate the head numbers
    
    std::set<std::string> srv;
    {
      // Let's work on a local copy, to avoid a longer locking
      boost::unique_lock<boost::recursive_mutex> l(*this);
      srv = servers;
    }
    
    for (std::set<std::string>::iterator i = srv.begin(); i != srv.end(); i++) {
      
      Log(Logger::Lvl4, domelogmask, domelogname, "Contacting disk server: " << *i);
      int errcode = 0;
      std::string url = CFG->GetString("head.diskdomeprotopfx", (char *)"http") + "://" + *i +
                                      CFG->GetString("head.diskdomemgmtsuffix", (char *)"/domedisk/");
      Davix::Uri uri(url);
      
      Davix::DavixError* tmp_err = NULL;
      DavixStuff *ds = DavixCtxPoolHolder::getDavixCtxPool().acquire();
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
      if (tmp_err) {
        Err("checkDiskSpaces", "HTTP status error on" << url << ", Error: "<< tmp_err->getErrMsg());
        continue;
      }
      
      // Here we have the response... splat its body into a ptree to parse it
      std::istringstream is(req.getAnswerContent());
      
      Log(Logger::Lvl4, domelogmask, domelogname, "Disk server: " << *i << " answered: '" << is.str() << "'");
      
      boost::property_tree::ptree myresp;
      
      try {
        boost::property_tree::read_json(is, myresp);
        
      } catch (boost::property_tree::json_parser_error e) {
        Err("checkDiskSpaces", "Could not process JSON: " << e.what() << " '" << is.str() << "'");
        continue;
      }
      
      
      // Now process the values from the Json. Lock the status!
      {
        boost::unique_lock<boost::recursive_mutex> l(*this);
        
        // Loop through the server names, childs of fsinfo

        BOOST_FOREACH(const boost::property_tree::ptree::value_type &srv, myresp.get_child("fsinfo")) {
          // v.first is the name of the server.
          // v.second is the child tree representing the server
          
          // Now we loop through the filesystems of this server
          BOOST_FOREACH(const boost::property_tree::ptree::value_type &fs, srv.second) {
            // v.first is the name of the server.
            // v.second is the child tree representing the server
            
            // Find the corresponding server:fs info in our array, and get the counters
            Log(Logger::Lvl4, domelogmask, domelogname, "Processing: " << srv.first << " " << fs.first);
            for (int ii = 0; ii < fslist.size(); ii++) {
              
              Log(Logger::Lvl4, domelogmask, domelogname, "Checking: " << fslist[ii].server << " " << fslist[ii].fs);
              if ((fslist[ii].server == srv.first) &&
                (fslist[ii].fs == fs.first)) {
                
                Log(Logger::Lvl3, domelogmask, domelogname, "Matched: " << fslist[ii].server << " " << fslist[ii].fs);
                Log(Logger::Lvl3, domelogmask, domelogname, "Getting: " << fs.second.get<long long>( "freespace" ) << " " << fs.second.get<long long>( "physicalsize" ));
                fslist[ii].freespace = fs.second.get<long long>( "freespace" );
                fslist[ii].physicalsize = fs.second.get<long long>( "physicalsize" );
              
              
              }
            }
            
          }  
        }
        
      } // lock
      
    } // for
    Log(Logger::Lvl3, domelogmask, domelogname, "Exiting.");
  } // if role head
  
  
}
