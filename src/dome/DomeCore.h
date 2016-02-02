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
  int dome_put(DomeReq &req, FCGX_Request &request);
  int dome_putdone(DomeReq &req, FCGX_Request &request);
  int dome_getspaceinfo(DomeReq &req, FCGX_Request &request);
  int dome_getquotatoken(DomeReq &req, FCGX_Request &request);
  int dome_setquotatoken(DomeReq &req, FCGX_Request &request);
  int dome_delquotatoken(DomeReq &req, FCGX_Request &request);
  int dome_chksum(DomeReq &req, FCGX_Request &request);
  int dome_chksumstatus(DomeReq &req, FCGX_Request &request);
  int dome_ispullable(DomeReq &req, FCGX_Request &request);
  int dome_get(DomeReq &req, FCGX_Request &request);
  int dome_pulldone(DomeReq &req, FCGX_Request &request);
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
  int dome_statfs(DomeReq &req, FCGX_Request &request);

  int dome_getdirspaces(DomeReq &req, FCGX_Request &request);
  
private:
  bool initdone, terminationrequested;
  boost::recursive_mutex mtx;
  boost::mutex accept_mutex;
  int fcgi_listenSocket;
  DmlitePool *dmpool;

  /// Easy way to get threaded life
  std::vector< boost::thread * > workers;
  friend void workerFunc(DomeCore *core, int myidx);

  /// The thread that ticks
  boost::thread *ticker;


protected:

  // In the case of a disk server, checks the free/used space in the mountpoints
  void checkDiskSpaces();

  /// Send a notification to the head node about the completion of this task
  virtual void onTaskCompleted(DomeTask &task);

  /// Send a notification to the head node about a task that is running
  virtual void onTaskRunning(DomeTask &task);

  // unconditionally schedule the calculation of a checksum
  void calculateChecksum(std::string lfn, std::string pfn, bool replace);
};








#endif
