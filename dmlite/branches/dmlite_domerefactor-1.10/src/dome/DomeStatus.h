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
#include "DomeDmlitePool.h"
#include "dmlite/cpp/authn.h"
#include "status.h"

class DomeReq;
class DomeQuotatoken;
class DomeStatus;

class DomePoolInfo {

public:
  DomePoolInfo() {
    poolname="";
    defsize = 1024LL*1024*1024*4;
    stype = 'P';
  };

  std::string poolname;

  // Default file size for blindly allocating a new file
  int64_t defsize;

  // Space type. V=Volatile, (P or anything else)=Permanent
  char stype;

};


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
    activitystatus = FsUnknown;
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

  bool canPullFile(DomeStatus &st);

  /// Check if the given write request can go to the given quotatoken
  bool canwriteintoQuotatoken(DomeReq &req, DomeQuotatoken &token);

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

  /// Spacetoken uuid. Used for backward compatibility
  std::string s_token;

  /// Spacetoken human name
  std::string u_token;

  /// Pool referred to by this quotatoken
  std::string poolname;

  /// Total space of this quota or spacetoken
  int64_t t_space;

  /// Path prefix this structure is assigned to, in the quotatoken use case
  /// No trailing slash !
  std::string path;

  /// Groups that are allowed to write
  std::vector<std::string> groupsforwrite;

  std::string getGroupsString(bool putzeroifempty = false);

  /// uid/gid associated to this quotatoken
  /// Details are not very clear at this time
  int s_uid, s_gid;
};


/// Information about an user
class DomeUserInfo {
public:

  DomeUserInfo(): userid(-1), banned(false) {};

  dmlite::UserInfo getDmLiteUser();
  
  /// The user id
  int16_t userid;

  /// The username
  std::string username;

  /// Tha banned status
  bool banned;
  
  /// What's this?
  std::string ca;

  /// additional info
  std::string xattr;
};

/// Information about a group
class DomeGroupInfo {
public:

  DomeGroupInfo(): groupid(-1), banned(false) {};

  dmlite::GroupInfo getDmLiteGroup();
  
  /// The user id
  int16_t groupid;

  /// The username
  std::string groupname;

  /// Tha banned status
  bool banned;

  /// additional info
  std::string xattr;
};


/// Class that contains the internal status of the storage system, pools, disks, etc
/// Accesses must lock/unlock it for operations where other threads may write
class DomeStatus: public boost::recursive_mutex {
public:

  DomeStatus();
  ~DomeStatus() {

    //if(dmpool) {
    //  delete dmpool;
    //  dmpool = NULL;
    //}
  }

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

  /// Trivial store for pool information
  std::map <std::string, DomePoolInfo> poolslist;

  /// Simple keyvalue store for prefix-based quotas, that
  /// represent a simplification of spacetokens
  /// The key is the prefix without trailing slashes
  std::multimap <std::string, DomeQuotatoken> quotas;

  /// List of all the servers that are involved. This list is built dynamically
  /// when populating the filesystems
  std::set <std::string> servers;

  /// Tables to quick translate users and groups. To access this the status must be locked!
  std::map <int, DomeUserInfo> usersbyuid;
  std::map <std::string, DomeUserInfo> usersbyname;
  std::map <int, DomeGroupInfo> groupsbygid;
  std::map <std::string, DomeGroupInfo> groupsbyname;
  
  dmlite::DmStatus getIdMap(const std::string& userName,
                    const std::vector<std::string>& groupNames,
                    DomeUserInfo &user,
                    std::vector<DomeGroupInfo> &groups);
  
  /// Inserts/overwrites an user
  int insertUser(DomeUserInfo &ui);
  /// Inserts/overwrites a group
  int insertGroup(DomeGroupInfo &gi);
  /// Gets user info from uid. Returns 0 on failure
  int getUser(int uid, DomeUserInfo &ui);
  /// Gets user info from name. Returns 0 on failure
  int getUser(std::string username, DomeUserInfo &ui);
  /// Gets group info from gid. Returns 0 on failure
  int getGroup(int gid, DomeGroupInfo &gi);
  /// Gets group info from name. Returns 0 on failure
  int getGroup(std::string groupname, DomeGroupInfo &gi);

  /// A quick rendition of the grid mapfile, translating from user DN to VOMS group
  /// The implementation of getIdMap may use this
  std::multimap <std::string, std::string> gridmap;


  /// Helper function that reloads all the filesystems from the DB
  int loadFilesystems();

  /// Helper function that reloads all the quotas from the DB
  int loadQuotatokens();

  /// Helper function that reloads all the users from the DB
  int loadUsersGroups();

  /// Helper function that updates all quotatokens
  void updateQuotatokens(const std::vector<DomeQuotatoken> &tokens);

  /// Helper function that gets a quotatoken given its s_token
  bool getQuotatoken(const std::string &s_token, DomeQuotatoken &tk);

  /// Helper function that gets a quotatoken given the keys
  int getQuotatoken(const std::string &path, const std::string &poolname, DomeQuotatoken &tk);

  /// Helper function that deletes a quotatoken given the keys
  int delQuotatoken(const std::string &path, const std::string &poolname, DomeQuotatoken &tk);

  /// Calculates the total space for the given pool and the free space on the disks that belong to it
  /// Returns zero if pool was found, nonzero otherwise
  int getPoolSpaces(std::string &poolname, long long &total, long long &free, int &poolstatus);

  /// Tells if a pool with the given name exists
  bool existsPool(std::string &poolname);

  /// Retrieves basic info about a named pool
  bool getPoolInfo(std::string &poolname, int64_t &pool_defsize, char &pool_stype);

  // Utility ------------------------------------
  bool LfnMatchesAnyCanPullFS(std::string lfn, DomeFsInfo &fsinfo);
  bool PfnMatchesAnyFS(std::string &srv, std::string &pfn);
  bool PfnMatchesAnyFS(std::string &srv, std::string &pfn, DomeFsInfo &fsinfo);

  // head node trusts all the disk nodes that are registered in the filesystem table
  // disk node trusts head node as defined in the config file
  bool isDNaKnownServer(std::string dn);

  // which quotatoken should apply to lfn?
  // return true and set the token, or return false if no tokens match
  // If more than one quotatokens match then the first one is selected
  // TODO: honour the fact that multiple quotatokens may match
  bool whichQuotatokenForLfn(const std::string &lfn, DomeQuotatoken &token);

  // calculate used space of a quotatoken
  long long getQuotatokenUsedSpace(const DomeQuotatoken &token);
  long long getDirUsedSpace(const std::string &path);

  // does this file fit in our quotatoken?
  bool fitsInQuotatoken(const DomeQuotatoken &token, const int64_t size);

  bool canwriteintoQuotatoken(DomeReq &req, DomeQuotatoken &token);

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



  void notifyQueues();
  void waitQueues();

  /// The queue holding file pull requests
  GenPrioQueue *filepullq;

  /// The davix pool
  dmlite::DavixCtxPool *davixPool;
  void setDavixPool(dmlite::DavixCtxPool *pool);

  /// The status lives
  int tick(time_t timenow);
  int tickQueues(time_t timenow);

  //DmlitePool *dmpool;
private:
  time_t lastreload, lastfscheck, lastreloadusersgroups;
  long globalputcount;

  // For the queue ticker
  boost::condition_variable queue_cond;
  boost::mutex queue_mtx;
};
