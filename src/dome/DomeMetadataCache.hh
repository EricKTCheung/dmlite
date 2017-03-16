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
 * @brief  In-memory cache of frequently accessed file information. For simplicity, this is a singleton
 * @author Fabrizio Furano
 * @date   Mar 2017
 */

#ifndef DOMEMETADATACACHE_HH
#define DOMEMETADATACACHE_HH

#include "status.h"
#include "inode.h"
#include "utils/logger.h"
#include "utils/DomeUtils.h"
#include "utils/Config.hh"
#include "DomeStatus.h"
#include "DomeLog.h"
#include "utils/urls.h"

#include <string>
#include <map>
#include <time.h>


#ifdef HAVE_ATOMIC
#include <atomic>

typedef std::atomic<int> IntAtomic;
#endif

#ifdef HAVE_CSTDATOMIC
#include <cstdatomic>

typedef atomic<int> IntAtomic;
#endif

#include <boost/thread.hpp>
#include <boost/bimap.hpp>

/// Typedef for the fileid used as key
typedef int64_t DomeFileID;

/// Also the couple (parent_fileid, name) is a key (sob)
struct DomeFileInfoParent {
  DomeFileID parentfileid;
  std::string name;
};
inline bool operator < (const DomeFileInfoParent &s1, const DomeFileInfoParent &s2) {
  if (s1.parentfileid < s2.parentfileid)
    return true;
  else {
    if (s1.parentfileid == s2.parentfileid)
      return (s1.name < s2.name);
    
    return false;
  }
}

/// Defines the information that is kept about a file
/// Note that this object is lockable. It's supposed to be locked (if necessary)
/// from outside its own code only
class DomeFileInfo : public boost::mutex {
protected:
  /// Threads waiting for result about this file will wait and synchronize here
  boost::condition_variable condvar;
public:
  
  /// Ctor
  /// @param fileid The fileid that univocally identifies a file or dir
  /// @param parentfileid The parent fileid that together with the name univocally identifies this file or dir
  /// @param name The name (not path!)  that together with the parentfileid univocally identifies this file or dir
  DomeFileInfo(DomeFileID file_id);
  DomeFileInfo(DomeFileID parentfileid, std::string fname);
  ~DomeFileInfo();
  
  /// The name of this file relative to its parent directory
  std::string locfilename;
  /// The fileid
  DomeFileID fileid;
  /// The parent fileid
  DomeFileID parentfileid;
  
  
  enum InfoStatus {
    Error = -2,
    NoInfo,
    Ok, // 0
    NotFound,
    InProgress
  };
  
  /// Status of this object with respect to the pending stat operations
  /// carried on by plugins
  /// If a request is in progress, the responsibility of
  /// performing it is of the plugin(s) that are treating it.
  InfoStatus status_statinfo;
  
  /// Status of this object with respect to the pending locate operations
  /// carried on by plugins
  /// If a request is in progress, the responsibility of
  /// performing it is of the plugin(s) that are treating it.
  InfoStatus status_locations;
  
  
  /// Count of the plugins that are gathering stat info
  /// for an instance
  int pending_statinfo;
  
  /// Count of the plugins that are gathering locate info
  /// for an instance
  int pending_locations;
  
  /// Tells if the stat information is ready
  InfoStatus getStatStatus() {
    // To stat successfully a file we just need one information source to answer positively
    // Hence, if we have the stat info here, we just return Ok, regardless
    // of how many plugins are still active
    if (!status_statinfo) return Ok;
    
    // If we have no stat info, then the file was not found or it's early to tell
    // Hence the info is inprogress if there are queries that are still active
    if (pending_statinfo > 0) return InProgress;
    
    
    return status_statinfo;
  }
  
  /// Tells if the locate information is ready
  InfoStatus getLocationStatus() {
    // In the case of a pending op that tries to find all the locations, we need to
    // wait for the result
    // Hence, this info is inprogress if there are still queries that are active on it
    
    if (status_locations == Ok) return Ok;
    return status_locations;
  }
  
  /// Builds a summary of the status
  /// If some info part is pending then the whole thing is pending
  InfoStatus getInfoStatus() {
    if ((pending_statinfo > 0) ||
      (pending_locations > 0))
      return InProgress;
    
    if ((status_statinfo == Ok) ||
      (status_locations == Ok))
      return Ok;
    
    if ((status_statinfo == NotFound) ||
      (status_locations == NotFound))
      return NotFound;
    
    return NoInfo;
  };
  
  /// Called by plugins when they start a query, to add 1 to the pending counter
  void notifyStatPending() {
    if (pending_statinfo >= 0) {
      // Set the file status to pending with respect to the stat op
      pending_statinfo++;
    }
  }
  
  /// Called by plugins when they end a search, to subtract 1 from the pending counter
  /// and wake up the clients that are waiting for something to happen to a file info
  void notifyStatNotPending() {
    if (pending_statinfo > 0) {
      // Decrease the pending count with respect to the stat op
      pending_statinfo--;
    } else
      Err("DomeFileInfo::notifyStatNotPending", "The fileinfo seemed not to be pending?!? fileid:" << fileid);
    
    signalSomeUpdate();
  }
  
  
  // Called by plugins when they start a search, to add 1 to the pending counter
  void notifyLocationPending() {
    if (pending_locations >= 0) {
      // Set the file status to pending with respect to the stat op
      pending_locations++;
    }
  }
  
  /// Called by plugins when they end a search, to subtract 1 from the pending counter
  /// and wake up the clients that are waiting for something to happen to a file info
  void notifyLocationNotPending() {

    if (pending_locations > 0) {
      // Decrease the pending count with respect to the stat op
      pending_locations--;
    } else
      Err("DomeFileInfo::notifyLocationNotPending", "The fileinfo seemed not to be pending?!? fileid:" << fileid);
    
    signalSomeUpdate();
  }
  
  /// The stat information about this entity, in its original data structure
  dmlite::ExtendedStat statinfo;
  
  /// The list of the replicas (if this is a file)
  std::vector<dmlite::Replica> replicas;
  
  
  /// The last time there was a request to gather info about this entry
  time_t lastupdreqtime;
  
  /// The last time there was an update to this entry
  time_t lastupdtime;
  
  /// The last time this entry was referenced
  time_t lastreftime;
  
  /// Update last reference time
  void touch() {
    // only update reference time if the entry exist, otherwise it may be stuck in internal cache 
    // until max ttl expires
    if(getInfoStatus() == DomeFileInfo::NotFound)
      return;
    lastreftime = time(0);
  }
  
  /// Wait until any notification update comes
  /// Useful to recheck if what came is what we were waiting for
  /// 0 if notif received, nonzero if tmout
  int waitForSomeUpdate(boost::unique_lock<boost::mutex> &l, int sectmout);
  
  /// Wait for the stat info to be available
  /// @param l lock to be held
  /// @param sectmout Wait timeout in seconds
  int waitStat(boost::unique_lock<boost::mutex> &l, int sectmout);
  
  /// Wait for the replica info to be available
  /// @param l lock to be held
  /// @param sectmout Wait timeout in seconds
  int waitLocations(boost::unique_lock<boost::mutex> &l, int sectmout);
    
  /// Signal that something changed here
  int signalSomeUpdate();
  
  
  // Useful for debugging
  void print(std::ostream &out);
    
  /// Clear an object, so that it seems that we know nothing about it.
  /// Useful instead of removing the item from the cache (which is deprecated)
  void setToNoInfo();
  
  /// Fill the fields from a stat struct
  /// @param st the stat struct to copy fields from
  void takeStat(const dmlite::ExtendedStat &st);
  
  /// Helper. add a replica to the replicas list
  /// @params replica struct to add
  void addReplica( const dmlite::Replica & replica );
  
  /// Helper. add a vector of replicas to the replicas list
  /// @params replica struct to add
  void addReplica( const std::vector<dmlite::Replica> &reps );
};

/// Instances of DomeFileInfo may be kept in a quasi-sorted way.
/// This is the compare functor that keeps them sorted by parent_id+name
class DomeFileInfoParentComp {
public:
  virtual ~DomeFileInfoParentComp(){};
  
  virtual bool operator()(DomeFileInfoParent s1, DomeFileInfoParent s2) {
    if (s1.parentfileid < s2.parentfileid)
      return true;
    else {
      if (s1.parentfileid == s2.parentfileid)
        return (s1.name < s2.name);
      
      return false;
    }
  }
};





/// This class acts like a repository of file locations/information that has to
/// be gathered on the fly and kept as in a cache.
class DomeMetadataCache : public boost::mutex {
private:
  
  
  
  /// Private ctor
  DomeMetadataCache() : lrutick(0) {  };
  
  /// Singleton instance
  static DomeMetadataCache *instance;
  
  /// @return the singleton instance
  static DomeMetadataCache *get()
  {
    if (instance == 0)
      instance = new DomeMetadataCache();
    return instance;
  }
  
  
  
  /// Counter for implementing a LRU buffer
  unsigned long long lrutick;
  /// Max number of items allowed
  unsigned long maxitems;
  /// Max life for an item that was not recently accessed
  unsigned int maxttl;
  /// Max life for an item that even if it was recently accessed
  unsigned int maxmaxttl;
  /// Max life for a NEGATIVE item (e.g. a not found) that was not recently accessed
  unsigned int maxttl_negative;
  
  /// A simple implementation of an lru queue, based on a bimap
  typedef boost::bimap< time_t, DomeFileID > lrudatarepo;
  /// A simple implementation of an lru queue, based on a bimap
  typedef lrudatarepo::value_type lrudataitem;
  /// A simple implementation of an lru queue, based on a bimap
  lrudatarepo lrudata;
  
  /// A simple implementation of an lru queue, based on a bimap
  typedef boost::bimap< time_t, DomeFileInfoParent > lrudatarepo_parent;
  /// A simple implementation of an lru queue, based on a bimap
  typedef lrudatarepo_parent::value_type lrudataitem_parent;
  /// A simple implementation of an lru queue, based on a bimap
  lrudatarepo_parent lrudata_parent;
  
  /// The information repo itself. Basically a map key->DomeFileInfo
  /// where the key is a numeric fileid.
  std::map< DomeFileID, boost::shared_ptr<DomeFileInfo> > databyfileid;
  /// Unfortunately we have to keep TWO indexes, because items in
  /// the lcgdm database have two keys. In this case the key
  /// is a couple (parent_fileid, filename)
  std::map< DomeFileInfoParent, boost::shared_ptr<DomeFileInfo> > databyparent;
  
  /// Purge an item from the buffer, to make space
  int purgeLRUitem_fileid();
  int purgeLRUitem_parent();
  
  /// Purge the old items from the buffer
  void purgeExpired_fileid();
  /// Purge the old items from the buffer
  void purgeExpired_parent();
  /// Purge the old items from the buffer
  void purgeExpired() {
    purgeExpired_fileid();
    purgeExpired_parent();
  }
  
public:
  
  void Init() {
    // Get the max capacity from the config
    maxitems = CFG->GetLong("mdcache.maxitems", 1000000);
    // Get the lifetime of an entry after the last reference
    maxttl = CFG->GetLong("mdcache.itemttl", 3600);
    // Get the maximum allowed lifetime of an entry
    maxmaxttl = CFG->GetLong("mdcache.itemmaxttl", 7200);
    maxttl_negative = CFG->GetLong("mdcache.itemttl_negative", 10);
  }
  
  
  //
  // Helper primitives to operate on the list of file locations
  //
  
  /// Get a pointer to a FileInfo, or create a new one, marked as pending, identified only with its fileid
  boost::shared_ptr <DomeFileInfo > getFileInfoOrCreateNewOne(DomeFileID fileid);
  /// Get a pointer to a FileInfo, or create a new one, marked as pending, identified only with its parentfileid+name
  boost::shared_ptr <DomeFileInfo > getFileInfoOrCreateNewOne(DomeFileID parentfileid, std::string name);

  
  /// Forcefully purge an entry using its fileid
  void purgeEntry(DomeFileID fileid);
  /// Forcefully purge an entry using its parentfileid+name
  void purgeEntry(DomeFileID parentfileid, std::string name);
  
  /// For DB compatibility reasons, this cache has two keys, so it may happen that
  /// two different objects get created with two different key types (sob)
  /// If we detect that two cache entries refer to the same file but through different types of key
  /// then we make sure that they are both filled with the same information
  int fixupFileInfoKeys(DomeFileID fileid, DomeFileID parentfileid, std::string name);
  
  
  /// Gives life to this obj, purges expired items, etc
  void tick();
};







#endif

