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
#include "DomeDavixPool.h"




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
    Log(Logger::Lvl4, domelogmask, domelogname, "Reloading quotas and filesystems");
    loadQuotatokens();
    loadFilesystems();   
    
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
  
  boost::unique_lock<boost::recursive_mutex> l(*this);
  
  if (role == roleDisk) {
    
    // Loop over the filesystems, and check those that match our hostname
    // Give a horrible error if none is found
    char myhostname[64];
    gethostname(myhostname, 64);
    int nfs = 0;
    
    for (int i = 0; i < fslist.size(); i++) {
      
      if ( !strcmp(fslist[i].server.c_str(), myhostname) ) {
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
    
    if (!nfs) {
      Err("checkDiskSpaces", "This server has no filesystems ?!?!?");
    }
    
    Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. Number of local filesystems: " << nfs);
    
  }
  else { // TODO: Head node case. We request dome_getspaceinfo to each server, then loop on the results and calculate the head numbers
    
    
    DavixStuff *ds = DavixCtxPoolHolder::getDavixCtxPool().acquire();
    
    
    Log(Logger::Lvl3, domelogmask, domelogname, "Exiting.");
  }
  
  
}
