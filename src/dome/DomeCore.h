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



/** @file   DomeCore.h
 * @brief  The main class where operation requests are applied to the status
 * @author Fabrizio Furano
 * @date   Dec 2015
 */

#ifndef DOMECORE_H
#define DOMECORE_H


#include "DomeStatus.h"
#include "DomeTaskExec.h"
#include "DomeReq.h"
#include "DomeLog.h"
#include "DomeDmlitePool.h"
#include "utils/mysqlpools.h"
#include "utils/DavixPool.h"
#include <fcgiapp.h>
#include <fcgio.h>
#include "utils/Config.hh"
#include "DomeMysql.h"
#include <string>
#include <vector>
#include <map>
#include <functional>
#include <bitset>
#include <boost/thread.hpp>
#include <dmlite/cpp/catalog.h>

/// Data associated to a running checksum calculation
struct PendingChecksum {
  std::string lfn;
  std::string server;
  std::string pfn;
  std::string chksumtype;
  bool updateLfnChecksum;
  std::string remoteclientdn;
  std::string remoteclienthost;

  PendingChecksum(std::string _lfn, std::string _server, std::string _pfn, std::string _chksumtype, bool _updateLfnChecksum)
  : lfn(_lfn), server(_server), pfn(_pfn), chksumtype(_chksumtype), updateLfnChecksum(_updateLfnChecksum) {} 

  PendingChecksum() {}
};

/// Data associated to a pending file pull
struct PendingPull {
  std::string lfn;
  std::string server;
  std::string pfn;
  std::string chksumtype;

  std::string remoteclientdn;
  std::string remoteclienthost;

  PendingPull(std::string _lfn, std::string _server, std::string _pfn, std::string _chksumtype)
  : lfn(_lfn), server(_server), pfn(_pfn), chksumtype(_chksumtype) {} 

  PendingPull() {}
};
// Remember: A DMLite connection pool to the db is just a singleton, always accessible

class DomeCore: public DomeTaskExec {

public:

  DomeCore();
  virtual ~DomeCore();

  /// Reads the config file and initializes all the subsystems
  /// After this call everything has to be operative
  /// To be called after the ctor to initialize the object.
  /// @param cfgfile Path to the config file to be loaded
  int init(const char *cfgfile = 0);

  /// The app status, plus functions that modify it
  DomeStatus status;


  /// Ticking this gives life to the objects belonging to this class
  /// This is useful for managing things that expire, pings or periodic checks
  virtual void tick(int parm);



  /// Requests calls. These parse the request, do actions and send the response, using the original fastcgi func
  int dome_put(DomeReq &req, FCGX_Request &request, struct DomeFsInfo *destfs = 0, std::string *destrfn = 0);
  int dome_putdone_head(DomeReq &req, FCGX_Request &request);
  int dome_putdone_disk(DomeReq &req, FCGX_Request &request);
  int dome_getspaceinfo(DomeReq &req, FCGX_Request &request);
  int dome_getquotatoken(DomeReq &req, FCGX_Request &request);
  int dome_setquotatoken(DomeReq &req, FCGX_Request &request);
  int dome_delquotatoken(DomeReq &req, FCGX_Request &request);
  int dome_chksum(DomeReq &req, FCGX_Request &request);
  int dome_chksumstatus(DomeReq &req, FCGX_Request &request);

  int dome_get(DomeReq &req, FCGX_Request &request);
  int dome_pullstatus(DomeReq &req, FCGX_Request &request);
  int dome_statpool(DomeReq &req, FCGX_Request &request);
  int dome_ping(DomeReq &req, FCGX_Request &request) {
    int r = FCGX_FPrintF(request.out,
                 "Content-type: text/html\r\n"
                 "\r\n"
                 "Have a nice day.\r\n");

    return (r);
  };

  int dome_pull(DomeReq &req, FCGX_Request &request);
  int dome_dochksum(DomeReq &req, FCGX_Request &request);
  int dome_statpfn(DomeReq &req, FCGX_Request &request);

  int dome_getdirspaces(DomeReq &req, FCGX_Request &request);

  /// Disk server only. Removes a physical file
  int dome_pfnrm(DomeReq &req, FCGX_Request &request);
  /// Disk server only. Removes a replica, both from the catalog and the disk
  int dome_delreplica(DomeReq &req, FCGX_Request &request);
  
  /// Adds a new pool
  int dome_addpool(DomeReq &req, FCGX_Request &request);
  /// Removes a pool and all the related filesystems
  int dome_rmpool(DomeReq &req, FCGX_Request &request);
  /// Adds a filesystem to an existing pool. This implicitly creates a pool
  int dome_addfstopool(DomeReq &req, FCGX_Request &request);
  /// Removes a filesystem, no matter to which pool it was attached
  int dome_rmfs(DomeReq &req, FCGX_Request &request);

   
  
  /// Fecthes logical stat information for an LFN or file ID or a pfn
  int dome_getstatinfo(DomeReq &req, FCGX_Request &request);
  /// Like an HTTP GET on a directory, gets all the content
  int dome_getdir(DomeReq &req, FCGX_Request &request);
  /// Get user information
  int dome_getuser(DomeReq &req, FCGX_Request &request);
  /// Get group information
  int dome_getgroup(DomeReq &req, FCGX_Request &request);
  /// Get id mapping
  int dome_getidmap(DomeReq &req, FCGX_Request &request);
  
private:
  bool initdone, terminationrequested;
  boost::recursive_mutex mtx;
  boost::mutex accept_mutex;
  int fcgi_listenSocket;
  DmlitePool *dmpool;

  dmlite::DavixCtxFactory *davixFactory;
  dmlite::DavixCtxPool *davixPool;

  /// Easy way to get threaded life
  std::vector< boost::thread * > workers;
  friend void workerFunc(DomeCore *core, int myidx);

  /// The thread that ticks
  boost::thread *ticker;

  /// Atomically increment and returns the number of put requests that this server saw since the last restart
  long getGlobalputcount();

  /// Pending disknode checksums
  std::map<int, PendingChecksum> diskPendingChecksums;
  
    /// Pending disknode file pulls
  std::map<int, PendingPull> diskPendingPulls;
  
protected:

  // In the case of a disk server, checks the free/used space in the mountpoints
  void checkDiskSpaces();

  /// Send a notification to the head node about the completion of this task
  virtual void onTaskCompleted(DomeTask &task);

  /// Send a notification to the head node about a task that is running
  virtual void onTaskRunning(DomeTask &task);

  // helper functions for checksums
  int calculateChecksum(DomeReq &req, FCGX_Request &request, std::string lfn, dmlite::Replica replica, std::string checksumtype, bool updateLfnChecksum);
  void sendChecksumStatus(const PendingChecksum &pending, const DomeTask &task, bool completed);
  
  // Helper for file pulls
  int enqfilepull(DomeReq &req, FCGX_Request &request, std::string lfn);
  void sendFilepullStatus(const PendingPull &pending, const DomeTask &task, bool completed);
};




#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()



#endif
