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



/** @file   DomeStatus.h
 * @brief  A helper class that describes the internal status of the storage system, pools, disks, etc
 * @author Fabrizio Furano
 * @date   Dec 2015
 */


#include <boost/thread.hpp>
#include <set>
#include "DomeGenQueue.h"
#include "utils/DavixPool.h"

/// We describe a filesystem where to put data. We also keep dynamic information here, e.g. the free/used space
class DomeFsInfo {

public:

  DomeFsInfo() {
    
    // A filesystem starts internally disabled
    // It will be anabled by the first good answer of the
    // server that holds it
    status = FsStaticDisabled;
    freespace = 0LL;
    physicalsize = 0LL;
  };
  
  
  /// The logical pool this fs belongs to.
  std::string poolname;

  /// The server that hosts this filesystem
  std::string server;

  /// The private path of the filesystem inside its server
  std::string fs;

  /// Static status about this filesystem (could be disabled or read only as set in the DB)
  /// I know, these values are ugly, because 0 should not be 'active'
  /// we cannot change them, as they come from the DB and are shared with the dpm daemon
  enum DomeFsStatus {
    FsStaticActive = 0,
    FsStaticDisabled,
    FsStaticReadOnly
  } status;
  
  /// Internal status of the filesystem. It can be broken or its server may be broken
  enum DomeFsActivityStatus {
    FsUnknown = 0, // Just after start
    FsOnline,      // Fs belongs to working server
    FsBroken       // The server that owns this fs seems to be broken.
  } activitystatus;
  
  /// Free space, in bytes
  long long freespace;
  /// Total size of this filesystem
  long long physicalsize;
  
  bool isGoodForWrite() {
    return ( (status == FsStaticActive) && (activitystatus == FsOnline) );
  }
  bool isGoodForRead() {
    return ( (status != FsStaticDisabled) && (activitystatus == FsOnline) );
  }
  
  bool canPullFile() {
    return ( ((pool_stype == 'V') || (pool_stype == 'v')) && (freespace > pool_defsize) );
  }
  
  // Default file size for blindly allocating a new file
  long pool_defsize;
  
  // Space type. V=Volatile, (P or anything else)=Permanent
  char pool_stype;
  
  // Predicate for sorting filesystems by decreasing freespace
  struct pred_decr_freespace {
    bool operator()(DomeFsInfo const & a, DomeFsInfo const & b) const {
        return a.freespace > b.freespace;
    }
    
  };

};





/// Information about a quota on a directory. This comes from the historical spacetoken information
class DomeQuotatoken {
public:
  /// An ID
  int64_t rowid;

  /// Spacetoken human name
  std::string u_token;
  
  /// Pool referred to by this quotatoken
  std::string poolname;

  /// Total space of this quota or spacetoken
  int64_t t_space;

  /// Path prefix this structure is assigned to, in the quotatoken use case
  /// No trailing slash !
  std::string path;

};




/// Class that contains the internal status of the storage system, pools, disks, etc
/// Accesses must lock/unlock it for operations where other threads may write
class DomeStatus: public boost::recursive_mutex {
public:
  
  DomeStatus();
  
  // Head node or disk server ?
  enum {
    roleHead,
    roleDisk
  } role;

  // The hostname of this machine
  std::string myhostname;
  
  // The hostname of the head node we refer to
  std::string headnodename;
  
  /// Trivial store for filesystems information
  std::vector <DomeFsInfo> fslist;
 
  /// Simple keyvalue store for prefix-based quotas, that
  /// represent a simplification of spacetokens
  /// The key is the prefix without trailing slashes
  std::multimap <std::string, DomeQuotatoken> quotas;

  /// List of all the servers that are involved. This list is built dynamically
  /// when populating the filesystems
  std::set <std::string> servers;
  
  

  /// Helper function that reloads all the filesystems from the DB
  int loadFilesystems();

  /// Helper function that reloads all the quotas from the DB
  int loadQuotatokens();

  /// Helper function that inserts a quotatoken or overwrites an existing one
  int insertQuotatoken(DomeQuotatoken &tk);
  
  /// Helper function that gets a quotatoken given the keys
  int getQuotatoken(const std::string &path, const std::string &poolname, DomeQuotatoken &tk);
  
  /// Helper function that deletes a quotatoken given the keys
  int delQuotatoken(const std::string &path, const std::string &poolname, DomeQuotatoken &tk);
  
  /// Calculates the total space for the given pool and the free space on the disks that belong to it
  /// Returns zero if pool was found, nonzero otherwise
  int getPoolSpaces(std::string &poolname, long long &total, long long &free, int &poolstatus);
  
  /// Tells if a pool with the given name exists
  bool existsPool(std::string &poolname);
  
  /// Removes a pool and all its filesystems
  int rmPoolfs(std::string &poolname);
  
  /// Adds a filesystem to an existing pool
  int addPoolfs(std::string &srv, std::string &fs, std::string &poolname);

  // Utility ------------------------------------
  bool LfnMatchesPool(std::string lfn, std::string pool);
  bool LfnMatchesAnyCanPullFS(std::string lfn, DomeFsInfo &fsinfo);
  bool PfnMatchesAnyFS(std::string &srv, std::string &pfn);
  bool PfnMatchesAnyFS(std::string &srv, std::string &pfn, DomeFsInfo &fsinfo);
  
  // head node trusts all the disk nodes that are registered in the filesystem table
  // disk node trusts head node as defined in the config file
  bool isDNaKnownServer(std::string dn);
  
  
  /// Atomically increment and returns the number of put requests that this server saw since the last restart
  /// Useful to compose damn unique replica pfns
  long getGlobalputcount();
  
  void checkDiskSpaces();
  
  // Tells if the given pfn belongs to the given filesystem root path
  bool PfnMatchesFS(std::string &server, std::string &pfn, DomeFsInfo &fs);
  // ---------------------------------
  
  
  /// The queue holding checksum requests
  GenPrioQueue *checksumq;
  /// Checks if I can send a checksum request to the disk
  void tickChecksums();
  /// Gives life to file pulls
  void tickFilepulls();

  /// The queue holding file pull requests
  GenPrioQueue *filepullq;

  /// The davix pool
  dmlite::DavixCtxPool *davixPool;
  void setDavixPool(dmlite::DavixCtxPool *pool);

  /// The status lives
  int tick(time_t timenow);
  
private:
  time_t lastreload, lastfscheck;
  long globalputcount;
};
