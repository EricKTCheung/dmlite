/*
 * Copyright 2017 CERN
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


/** @file  DomeMetadataCache.hh
 * @brief  In-memory cache of frequently accessed file information
 * @author Fabrizio Furano
 * @date   Mar 2017
 */



#include "DomeMetadataCache.hh"
#include <time.h>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread_time.hpp>
#include "DomeMysql.h"
#include "status.h"

DomeMetadataCache *DomeMetadataCache::instance = 0;




// ******************************************
// ******************* DomeFileInfo
//

DomeFileInfo::DomeFileInfo(DomeFileID file_id) {
  
  this->fileid = file_id;
  parentfileid = 0;
  locfilename = "";
  
  status_statinfo = NoInfo;
  status_locations = NoInfo;
  
  time_t t = time(0);
  lastupdtime = t;
  lastupdreqtime = t;
  lastreftime = t;
  
  

}

DomeFileInfo::DomeFileInfo(DomeFileID parentfid, std::string name) {
  
  this->fileid = 0;
  this->parentfileid = parentfid;
  locfilename = name;
  
  status_statinfo = NoInfo;
  status_locations = NoInfo;
  
  time_t t = time(0);
  lastupdtime = t;
  lastupdreqtime = t;
  lastreftime = t;
  
  
  
}

DomeFileInfo::~DomeFileInfo() {
  Log(Logger::Lvl4, domelogmask, "~DomeFileInfo", "I am being deleted. fileid: " << fileid);
}

void DomeFileInfo::setToNoInfo() {
  const char *fname = "DomeFileInfo::setToNoInfo";
  Log(Logger::Lvl4, domelogmask, fname, "Entering");
  
  boost::unique_lock<mutex> l2(*this);
  statinfo = dmlite::ExtendedStat();
  
  status_statinfo = NoInfo;
  
  replicas.clear();
  status_locations = NoInfo;
}


void DomeFileInfo::takeStat(const dmlite::ExtendedStat &st) {
  const char *fname = "DomeFileInfo::takeStat";
  Log(Logger::Lvl4, domelogmask, fname, st.name << " sz:" << st.stat.st_size << " mode:" << st.stat.st_mode);
  
  boost::unique_lock<mutex> l2(*this);
  
  statinfo = st; 
  
  status_statinfo = DomeFileInfo::Ok;
  
  
}


void DomeFileInfo::addReplica( const dmlite::Replica & replica ) {
  const char *fname = "DomeFileInfo::addReplica";
  Log(Logger::Lvl4, domelogmask, fname, "Adding replica '" << replica.rfn << "' to fileid " << fileid);
  
  replicas.push_back(replica);
}


void DomeFileInfo::addReplica( const std::vector<dmlite::Replica> &reps ) {
  const char *fname = "DomeFileInfo::addReplica";
  Log(Logger::Lvl4, domelogmask, fname, "Adding " << replicas.size() << "replicas to fileid " << fileid);

  replicas.insert(replicas.end(), reps.begin(), reps.end());
}


int DomeFileInfo::waitForSomeUpdate(boost::unique_lock<boost::mutex> &l, int sectmout) {
  
  boost::system_time const timeout = boost::get_system_time() + boost::posix_time::seconds(sectmout);
  
  if (!condvar.timed_wait(l, timeout))
    return 1; // timeout
  else
    return 0; // signal catched
      
  return 0;
}

int DomeFileInfo::signalSomeUpdate() {
  condvar.notify_all();
  
  return 0;
}

int DomeFileInfo::waitStat(boost::unique_lock<boost::mutex> &l, int sectmout)  {
  const char *fname = "DomeFileInfo::waitStat";
  
  // If it's a cache hit we just exit
  if ((status_statinfo == Ok) || (status_statinfo == NotFound)) {
    Log(Logger::Lvl4, domelogmask, fname, "Info found. Fileid: " << fileid <<
    " status_statinfo: " << status_statinfo);
    
    return 1;
  }
  
  // By convention, if there is noinfo then it's our responsibility to fill it, hence it becomes pending
  if (status_statinfo == NoInfo) {
    Log(Logger::Lvl4, domelogmask, fname, "Shall fill stat info. Fileid: " << fileid << 
    "parent_fileid: " << parentfileid << " locfilename: '" << locfilename <<
    "' status_statinfo: " << status_statinfo);
    
    status_statinfo = InProgress;
    
    return 0;
  }
  
  // If still pending, we wait for the file object to get a notification
  // then we recheck...
  time_t timelimit = time(0) + sectmout;
  
  Log(Logger::Lvl4, domelogmask, fname, "Starting check-wait. Fileid: " << fileid <<
  "parent_fileid: " << parentfileid << " locfilename: '" << locfilename <<
  "' status_statinfo: " << status_statinfo << "'");
  
  while (status_statinfo == InProgress) {
    // Ignore the timeouts, exit only on an explicit notification
    waitForSomeUpdate(l, 1);
    // On global timeout... stop waiting
    if (time(0) > timelimit) {
      Log(Logger::Lvl1, domelogmask, fname, "Timeout. Fileid:" << fileid <<
      "parent_fileid: " << parentfileid << " locfilename: '" << locfilename << "'");
      break;
    }
  }
  
  Log(Logger::Lvl3, domelogmask, fname, "Finished check-wait. Fileid: " << fileid <<
  "parent_fileid: " << parentfileid << " locfilename: '" << locfilename <<
  "' status_statinfo: " << status_statinfo);
  
  // We are here if someone else's lookup has finished OR in the case of timeout
  // If the stat is still marked as in progress it means that the information was not filled
  if (status_statinfo == InProgress)
    return 2;
  
  // If it's a cache hit we just exit
  if ((status_statinfo == Ok) || (status_statinfo == NotFound)) {
    Log(Logger::Lvl4, domelogmask, fname, "Info found. Fileid: " << fileid <<
    " status_statinfo: " << status_statinfo);
    
    return 1;
  }
  
  return 3;
}


int DomeFileInfo::waitLocations(boost::unique_lock<boost::mutex> &l, int sectmout)  {
  const char *fname = "DomeFileInfo::waitLocations";
  
  // If it's a cache hit we just exit
  if ((status_locations == Ok) || (status_locations == NotFound)) {
    Log(Logger::Lvl4, domelogmask, fname, "Info found. Fileid: " << fileid <<
    " status_statinfo: " << status_statinfo << " status_locations: " << status_locations);
    
    return 1;
  }
  
  // By convention, if there is noinfo then it's our responsibility to fill it, hence it becomes pending
  if (status_locations == NoInfo) {
    Log(Logger::Lvl4, domelogmask, fname, "Shall fill locations info. Fileid: " << fileid << 
    "parent_fileid: " << parentfileid << " locfilename: '" << locfilename <<
    "' status_statinfo: " << status_statinfo << " status_locations: " << status_locations);
    
    status_locations = InProgress;
    
    return 0;
  }
  
  // If still pending, we wait for the file object to get a notification
  // then we recheck...
  time_t timelimit = time(0) + sectmout;
  
  Log(Logger::Lvl4, domelogmask, fname, "Starting check-wait. Fileid: " << fileid <<
  "parent_fileid: " << parentfileid << " locfilename: '" << locfilename <<
  "' status_statinfo: " << status_statinfo << " status_locations: " << status_locations);
  
  while (status_statinfo == InProgress) {
    // Ignore the timeouts, exit only on an explicit notification
    waitForSomeUpdate(l, 1);
    // On global timeout... stop waiting
    if (time(0) > timelimit) {
      Log(Logger::Lvl1, domelogmask, fname, "Timeout. Fileid:" << fileid <<
      "parent_fileid: " << parentfileid << " locfilename: '" << locfilename << "'");
      break;
    }
  }
  
  Log(Logger::Lvl3, domelogmask, fname, "Finished check-wait. Fileid: " << fileid <<
  "parent_fileid: " << parentfileid << " locfilename: '" << locfilename <<
  "' status_statinfo: " << status_statinfo << " status_locations: " << status_locations);
  
  // We are here if someone else's lookup has finished OR in the case of timeout
  // If the stat is still marked as in progress it means that the information was not filled
  if (status_locations == InProgress)
    return 2;
  
  // If it's a cache hit we just exit
  if ((status_locations == Ok) || (status_locations == NotFound)) {
    Log(Logger::Lvl4, domelogmask, fname, "Info found. Fileid: " << fileid <<
    " status_statinfo: " << status_statinfo << " status_locations: " << status_locations);
    
    return 1;
  }
  
  return 3;
}




//
// ******************************************
// ******************* DomeMetadataCache
//



// Purge the least recently used element
// Returns 0 if the element was purged, non0 if it was not possible

int DomeMetadataCache::purgeLRUitem_fileid() {
  const char *fname = "DomeMetadataCache::purgeLRUitem";
  
  {
    // No LRU item, the LRU list is empty
    if (lrudata.empty()) {
      Log(Logger::Lvl4, domelogmask, fname, "LRU list is empty. Nothing to purge.");
      return 1;
    }
    
    // Take the key of the lru item   
    DomeFileID s = lrudata.left.begin()->second;
    Log(Logger::Lvl4, domelogmask, fname, "LRU item is fileid " << s);
    
    // Lookup its instance in the cache
    boost::shared_ptr<DomeFileInfo> fi = databyfileid[s];
    
    if (!fi) {
      Err(fname, "Could not find the LRU item in the cache. Fixing the internal inconsistency.");
      
      // Purge it from the lru list
      lrudata.right.erase(s);
      
      return 2;
    }
    
    
    {
      boost::unique_lock<mutex> lck(*fi);
      if ( (fi->status_statinfo == DomeFileInfo::InProgress) ||
        (fi->status_locations == DomeFileInfo::InProgress) ) {
        Log(Logger::Lvl4, domelogmask, fname, "The LRU item is marked as pending. Cannot purge fileid " << fi->fileid);
        return 3;
      }
      
    }
    
    // We have decided that we can delete it...
    
    // Purge it from the lru list
    lrudata.right.erase(s);
    
    // Remove the item from the map
    databyfileid.erase(s);   
    
    // The object will be deleted as soon as all the references to it disappear
  }
  
  return 0;
}




int DomeMetadataCache::purgeLRUitem_parent() {
  const char *fname = "DomeMetadataCache::purgeLRUitem";
  
  
  {
    // No LRU item, the LRU list is empty
    if (lrudata_parent.empty()) {
      Log(Logger::Lvl4, domelogmask, fname, "LRU_parent list is empty. Nothing to purge.");
      return 1;
    }
    
    // Take the key of the lru item   
    DomeFileInfoParent s = lrudata_parent.left.begin()->second;
    Log(Logger::Lvl4, domelogmask, fname, "LRU_parent item is " << s.parentfileid << "'" << s.name << "'");
    
    // Lookup its instance in the cache
    boost::shared_ptr<DomeFileInfo> fi = databyparent[s];
    
    if (!fi) {
      Err(fname, "Could not find the LRU_parent item in the cache. Fixing.");
      lrudata_parent.right.erase(s);
      return 2;
    }
    
    
    {
      boost::unique_lock<mutex> lck(*fi);
      if ( (fi->status_statinfo == DomeFileInfo::InProgress) ||
        (fi->status_locations == DomeFileInfo::InProgress) ) {
        Log(Logger::Lvl4, domelogmask, fname, "The LRU item is marked as pending. Cannot purge " << fi->fileid);
        return 3;
      }
      
    }
    
    // We have decided that we can delete it...
    
    // Purge it from the lru list
    lrudata_parent.right.erase(s);
    
    // Remove the item from the map
    databyparent.erase(s);   
    
    // The object will be deleted as soon as all the references to it disappear
  }
  return 0;
}




// Purge the items that were not touched since a longer time

void DomeMetadataCache::purgeExpired_fileid() {
  const char *fname = "DomeMetadataCache::purgeExpired";
  int d = 0;
  time_t timelimit = time(0) - maxttl;
  time_t timelimit_max = time(0) - maxmaxttl;
  time_t timelimit_neg = time(0) - maxttl_negative;
  
  bool dodelete = false;
  std::map< DomeFileID, boost::shared_ptr<DomeFileInfo> >::iterator i_deleteme;
  
  for (std::map< DomeFileID, boost::shared_ptr<DomeFileInfo> >::iterator i = databyfileid.begin();
       i != databyfileid.end(); i++) {
    
    if (dodelete) {
      databyfileid.erase(i_deleteme);
      dodelete = false;
    }
    dodelete = false;
  
  boost::shared_ptr<DomeFileInfo> fi = i->second;
  
  if (fi) {
    
    {
      boost::unique_lock<mutex> lck(*fi);
      
      time_t tl = timelimit;
      if ( (fi->status_statinfo == DomeFileInfo::NotFound) ||
        (fi->status_locations == DomeFileInfo::NotFound) ) {
        tl = timelimit_neg;
      }
      
      if ((fi->lastreftime < tl) || (fi->lastreftime < timelimit_max)) {
        // The item is old...
       
        if ( (fi->status_statinfo == DomeFileInfo::InProgress) ||
          (fi->status_locations == DomeFileInfo::InProgress) ) {
          Err(fname, "Found pending expired entry. Cannot purge fileid " << fi->statinfo.stat.st_ino);
          continue;
        }
        
        if (Logger::get()->getLevel() >= 4)
          Log(Logger::Lvl4, domelogmask, fname, "purging expired fileid " << fi->statinfo.stat.st_ino <<
            " name: '" << fi->statinfo.name << "' status_statinfo: " << fi->status_statinfo <<
            " status_locations: " << fi->status_locations <<
            " lastreftime: " << fi->lastreftime << " timelimit: " << tl << " timelimit_max: " << timelimit_max);
        else
          Log(Logger::Lvl2, domelogmask, fname, "purging expired fileid " << fi->statinfo.stat.st_ino <<
            " name: '" << fi->statinfo.name << "'");
        
        lrudata.right.erase(fi->statinfo.stat.st_ino);
        
        DomeFileInfoParent k;
        k.name = fi->statinfo.name;
        k.parentfileid = fi->statinfo.parent;
        lrudata_parent.right.erase(k);
        
        dodelete = true;
        i_deleteme = i;
        
        d++;
        
      }
    }
    
    // The entry will disappear when there are no more references to it
    
  }
  
       }
       
       if (dodelete) {
         databyfileid.erase(i_deleteme);
         dodelete = false;
       }
       
       if (d > 0)
         Log(Logger::Lvl1, domelogmask, fname, "purged " << d << " expired items.");
}




void DomeMetadataCache::purgeExpired_parent() {
  const char *fname = "DomeMetadataCache::purgeExpired_parent";
  int d = 0;
  time_t timelimit = time(0) - maxttl;
  time_t timelimit_max = time(0) - maxmaxttl;
  time_t timelimit_neg = time(0) - maxttl_negative;
  
  bool dodelete = false;
  std::map< DomeFileInfoParent, boost::shared_ptr<DomeFileInfo>,  DomeFileInfoParentComp>::iterator i_deleteme;
  
  
  for (std::map< DomeFileInfoParent, boost::shared_ptr<DomeFileInfo>,  DomeFileInfoParentComp>::iterator i = databyparent.begin();
       i != databyparent.end(); i++) {
    
    if (dodelete) {
      databyparent.erase(i_deleteme);
      dodelete = false;
    }
    dodelete = false;
  
  boost::shared_ptr<DomeFileInfo> fi = i->second;
  
  if (fi) {
    
    {
      boost::unique_lock<mutex> lck(*fi);
      
      time_t tl = timelimit;
      if ( (fi->status_statinfo == DomeFileInfo::NotFound) ||
        (fi->status_locations == DomeFileInfo::NotFound) ) {
        tl = timelimit_neg;
      }
      
      if ((fi->lastreftime < tl) || (fi->lastreftime < timelimit_max)) {
        // The item is old...
        
        
        if ( (fi->status_statinfo == DomeFileInfo::InProgress) ||
          (fi->status_locations == DomeFileInfo::InProgress) ) {
          Err(fname, "Found pending expired entry. Cannot purge parentfileid " << fi->statinfo.parent << "'" << fi->statinfo.name << "'");
          continue;
        }
        
        if (Logger::get()->getLevel() >= 4)
          Log(Logger::Lvl4, domelogmask, fname, "purging expired parentfileid " << fi->statinfo.parent <<
            " name: '" << fi->statinfo.name << "' status_statinfo: " << fi->status_statinfo <<
            " status_locations: " << fi->status_locations <<
            " lastreftime: " << fi->lastreftime << " timelimit: " << tl << " timelimit_max: " << timelimit_max);
        else
          Log(Logger::Lvl2, domelogmask, fname, "purging expired parentfileid " << fi->statinfo.parent << "'" << fi->statinfo.name << "'");
        
        lrudata.right.erase(fi->statinfo.stat.st_ino);
        
        DomeFileInfoParent k;
        k.name = fi->statinfo.name;
        k.parentfileid = fi->statinfo.parent;
        lrudata_parent.right.erase(k);
        
        dodelete = true;
        i_deleteme = i;
        
        d++;
        
      }
    }
    
    // The entry will disappear when there are no more references to it
    
  }
  
       }
       
       if (dodelete) {
         databyparent.erase(i_deleteme);
         dodelete = false;
       }
       
       if (d > 0)
         Log(Logger::Lvl1, domelogmask, fname, "purged " << d << " expired items.");
}



boost::shared_ptr <DomeFileInfo > DomeMetadataCache::getFileInfoOrCreateNewOne(DomeFileID fileid) {
  const char *fname = "DomeMetadataCache::getFileInfoOrCreateNewOne";
  boost::shared_ptr <DomeFileInfo > fi;
  bool hit = true;
  
  Log(Logger::Lvl4, domelogmask, fname, "fileid: " << fileid);
  
  {
    boost::lock_guard<DomeMetadataCache> l(*this);
    
    std::map< DomeFileID, boost::shared_ptr<DomeFileInfo> >::iterator p;
    
    p = databyfileid.find(fileid);
    if (p == databyfileid.end()) {
      
      
      // If we still have no space, try to garbage collect the old items
      if (databyfileid.size() > maxitems) {
        Log(Logger::Lvl4, domelogmask, fname, "Too many items " << databyfileid.size() << ">" << maxitems << ", running fileid garbage collection...");
        purgeExpired_fileid();
      }
      
      
      // If we reached the max number of items, delete as many as we need
      while (databyfileid.size() > maxitems) {
        Log(Logger::Lvl4, domelogmask, fname, "Too many items " << databyfileid.size() << ">" << maxitems << ", purging fileid LRU items...");
        if (purgeLRUitem_fileid()) break;
      }
      
      // If we still have no space, complain and do it anyway.
      if (databyfileid.size() > maxitems) {
        Log(Logger::Lvl4, domelogmask, fname, "Maximum fileid cache capacity exceeded. " << databyfileid.size() << ">" << maxitems);
      }
      
      
      // Create a new empty item
      fi.reset( new DomeFileInfo(fileid) );
      hit = false;
      // To disable the cache, set maxitems to 0
      if (maxitems > 0) {
        databyfileid[fileid] = boost::shared_ptr <DomeFileInfo >(fi);
        lrudata.insert(lrudataitem(++lrutick, fileid));
      }
      
    } else {
      // Promote the element to being the most recently used
      // Don't make it pending, not needed here
      lrudata.right.erase(fileid);
      lrudata.insert(lrudataitem(++lrutick, fileid));
      fi = p->second;
      fi->touch();
    }
  }
  
  // Here we have either
  //  - a new empty UgrFileInfo
  //  - an UgrFileInfo taken from the 1st level cache
  if (hit)
    Log(Logger::Lvl3, domelogmask, fname, "Exiting (hit). fileid: " << fileid);
  else
    Log(Logger::Lvl3, domelogmask, fname, "Exiting (miss). fileid: " << fileid);
  
  return fi;
  
}

boost::shared_ptr<DomeFileInfo> DomeMetadataCache::getFileInfoOrCreateNewOne(DomeFileID parentfileid, std::string name) {
  const char *fname = "DomeMetadataCache::getFileInfoOrCreateNewOne(parent)";
  boost::shared_ptr<DomeFileInfo> fi;
  bool hit = true;
  
  Log(Logger::Lvl4, domelogmask, fname, "parentfileid: " << parentfileid << " name: '" << name << "'");
  
  DomeFileInfoParent k;
  k.name = name;
  k.parentfileid = parentfileid;
  
  {
    boost::lock_guard<DomeMetadataCache> l(*this);
    
    std::map< DomeFileInfoParent, boost::shared_ptr<DomeFileInfo>,  DomeFileInfoParentComp>::iterator p;
    
    p = databyparent.find(k);
    if (p == databyparent.end()) {
      
      // If we still have no space, try to garbage collect the old items
      if (databyparent.size() > maxitems) {
        Log(Logger::Lvl4, domelogmask, fname, "Too many items " << databyparent.size() << ">" << maxitems << ", running parent garbage collection...");
        purgeExpired_parent();
      }
      
      // If we reached the max number of items, delete as many as we need
      while (databyparent.size() > maxitems) {
        Log(Logger::Lvl4, domelogmask, fname, "Too many items " << databyparent.size() << ">" << maxitems << ", purging parent LRU items...");
        if (purgeLRUitem_parent()) break;
      }
      

      
      // If we still have no space, complain and do it anyway.
      if (databyparent.size() > maxitems) {
        Log(Logger::Lvl4, domelogmask, fname, "Maximum parent cache capacity exceeded. " << databyparent.size() << ">" << maxitems);
      }
      
      
      // Create a new item
      fi.reset( new DomeFileInfo(parentfileid, name) );
      hit = false;
      
      // To disable the cache, set maxitems to 0
      if (maxitems > 0) {
        databyparent[k] = fi;
        lrudata_parent.insert(lrudataitem_parent(++lrutick, k));
      }
      
    } else {
      // Promote the element to being the most recently used
      // Don't make it pending, not needed here
      lrudata_parent.right.erase(k);
      lrudata_parent.insert(lrudataitem_parent(++lrutick, k));
      fi = p->second;     
      fi->touch();
    }
  }
  
  // Here we have either
  //  - a new empty UgrFileInfo, marked as pending for both statinfo and locations
  //  - an UgrFileInfo taken from the 1st level cache
  
  if (hit)
    Log(Logger::Lvl3, domelogmask, fname, "Exiting (hit). parentfileid: " << parentfileid << " name: '" << name << "'");
  else
    Log(Logger::Lvl3, domelogmask, fname, "Exiting (miss). parentfileid: " << parentfileid << " name: '" << name << "'");
  
  return fi;
  
}



void DomeMetadataCache::tick() {
  const char *fname = "DomeMetadataCache::tick";
  Log(Logger::Lvl4, domelogmask, fname, "tick...");
  
  boost::lock_guard<DomeMetadataCache> l(*this);
  
  purgeExpired();
  
  // If we reached the max number of items, delete as much as we can
  while (databyfileid.size() > maxitems) {
    if (purgeLRUitem_fileid()) break;
  }
  while (databyparent.size() > maxitems) {
    if (purgeLRUitem_parent()) break;
  }
  
  Log(Logger::Lvl4, domelogmask, fname, "Cache status by fileid. nItems:" << databyfileid.size() << " nLRUItems: " << lrudata.size());
  Log(Logger::Lvl4, domelogmask, fname, "Cache status by parentid+name. nItems:" << databyparent.size() << " nLRUItems: " << lrudata_parent.size());
  
}

/// Tag an entry so that it will be soon purged
void DomeMetadataCache::wipeEntry(dmlite::ExtendedStat xstat) {
  wipeEntry(xstat.stat.st_ino, xstat.parent, xstat.name);
}

/// Tag an entry so that it will be soon purged. More expensive version that does a stat inside
void DomeMetadataCache::wipeEntry(DomeFileID fileid) {
  const char *fname = "DomeMetadataCache::wipeEntry";
  Log(Logger::Lvl4, domelogmask, fname, "fileid: " << fileid);
  
  dmlite::ExtendedStat xstat;
  DomeMySql sql;
  dmlite::DmStatus ret;

  ret = sql.getStatbyFileid(xstat, fileid);
  if (ret.ok())
    wipeEntry(xstat.stat.st_ino, xstat.parent, xstat.name);
  
  
}

/// Tag an entry so that it will be soon purged
void DomeMetadataCache::wipeEntry(DomeFileID fileid, DomeFileID parentfileid, std::string name) {
  const char *fname = "DomeMetadataCache::wipeEntry";
  Log(Logger::Lvl4, domelogmask, fname, "fileid: " << fileid << " parentfileid: " << parentfileid << " name: '" << name << "'");
  
  boost::lock_guard<DomeMetadataCache> l(*this);
  
  {
    std::map< DomeFileID, boost::shared_ptr<DomeFileInfo> >::iterator p;
    
    // Fix the item got through the fileid
    p = databyfileid.find(fileid);
    if (p != databyfileid.end()) {
      Log(Logger::Lvl4, domelogmask, fname, "Found fileid: " << fileid );
      boost::shared_ptr<DomeFileInfo> fi;
      fi = p->second;
      
      boost::unique_lock<boost::mutex> l(*fi);
      fi->status_statinfo = DomeFileInfo::NoInfo;
      fi->signalSomeUpdate();
    }
  }
  
  {
    // Fix the item got through the parentfileid+name
    DomeFileInfoParent k;
    k.name = name;
    k.parentfileid = parentfileid;
    
    std::map< DomeFileInfoParent, boost::shared_ptr<DomeFileInfo> >::iterator p;
    p = databyparent.find(k);
    if (p != databyparent.end()) {
      Log(Logger::Lvl4, domelogmask, fname, "Found parentfileid: " << parentfileid << " name: '" << name << "'");
      boost::shared_ptr<DomeFileInfo> fi;
      fi = p->second;
      
      boost::unique_lock<boost::mutex> l(*fi);
      fi->status_statinfo = DomeFileInfo::NoInfo;
      fi->signalSomeUpdate();
    }
  }
  
  Log(Logger::Lvl3, domelogmask, fname, "Exiting. fileid: " << fileid << " parentfileid: " << parentfileid << " name: '" << name << "'");
  return;
}


int DomeMetadataCache::pushXstatInfo(dmlite::ExtendedStat xstat, DomeFileInfo::InfoStatus newstatus_statinfo) {
  const char *fname = "DomeMetadataCache::pushXstatInfo";
  
  Log(Logger::Lvl4, domelogmask, fname, "Adjusting fileid: " << xstat.stat.st_ino << " parentfileid: " <<
  xstat.parent << " name: '" << xstat.name << "'");
  
  boost::shared_ptr <DomeFileInfo > fi;
  
  boost::lock_guard<DomeMetadataCache> l(*this);  
  {
    // Fix the item got through the parentfileid+name
    DomeFileInfoParent k;
    k.name = xstat.name;
    k.parentfileid = xstat.parent;
    
    std::map< DomeFileInfoParent, boost::shared_ptr<DomeFileInfo> >::iterator p;
    p = databyparent.find(k);
    if (p != databyparent.end()) {
      Log(Logger::Lvl4, domelogmask, fname, "Adjusting parentfileid: " << xstat.parent << " name: '" << xstat.name << "'");
      
      fi = p->second;
      
      boost::unique_lock<boost::mutex> l(*fi);
      fi->statinfo = xstat;
      fi->status_statinfo = newstatus_statinfo;
      fi->parentfileid = xstat.parent;
      fi->signalSomeUpdate();
    }
    else {
      // Create a new item
      fi.reset( new DomeFileInfo(xstat.parent, xstat.name) );
      
      DomeFileInfoParent k;
      k.name = xstat.name;
      k.parentfileid = xstat.parent;
      
      fi->statinfo = xstat;
      fi->status_statinfo = DomeFileInfo::Ok;
      // To disable the cache, set maxitems to 0
      if (maxitems > 0) {
        databyparent[k] = fi;
        lrudata_parent.insert(lrudataitem_parent(++lrutick, k));
      }
    }
    
    
    
    {
      std::map< DomeFileID, boost::shared_ptr<DomeFileInfo> >::iterator p;
      
      // Fix the item got through the fileid
      p = databyfileid.find(xstat.stat.st_ino);
      if (p != databyfileid.end()) {
        Log(Logger::Lvl4, domelogmask, fname, "Adjusting fileid: " << xstat.stat.st_ino );
        
        fi = p->second;
        
        boost::unique_lock<boost::mutex> l(*fi);
        fi->statinfo = xstat;
        fi->status_statinfo = newstatus_statinfo;
        fi->fileid = xstat.stat.st_ino;
        fi->signalSomeUpdate();
      }
      else {
        // Create a new item if fi has not been set
        if (!fi)
          fi.reset( new DomeFileInfo(xstat.stat.st_ino) );
        
        fi->statinfo = xstat;
        fi->status_statinfo = DomeFileInfo::Ok;
        // To disable the cache, set maxitems to 0
        if (maxitems > 0) {
          databyfileid[xstat.stat.st_ino] = fi;
          lrudata.insert(lrudataitem(++lrutick, xstat.stat.st_ino));
        }
      }
    }
    
  }
  
  Log(Logger::Lvl3, domelogmask, fname, "Exiting. fileid: " << xstat.stat.st_ino << " parentfileid: " <<
  xstat.parent << " name: '" << xstat.name << "'");
  return 0;
}



