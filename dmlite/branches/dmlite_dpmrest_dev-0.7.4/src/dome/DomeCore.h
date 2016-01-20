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



/** @file   DpmrCore.h
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
#include "utils/mysqlpools.h"
#include <fcgiapp.h>
#include <fcgio.h>
#include "utils/Config.hh"
#include "DomeMysql.h"

// Remember: A DMLite connection pool to the db is just a singleton, always accessible
  

class DpmrCore: public DpmrTaskExec {

public:
  
  DpmrCore() {
      const char *fname = "DpmrCore::ctor";
      domelogmask = Logger::get()->getMask(domelogname);
      //Info(Logger::Lvl1, fname, "Ctor " << dmlite_MAJOR <<"." << dmlite_MINOR << "." << dmlite_PATCH);
      initdone = false;
      
  }
  

  /// Reads the config file and initializes all the subsystems
  /// After this call everything has to be operative
  /// To be called after the ctor to initialize the object.
  /// @param cfgfile Path to the config file to be loaded
  int init(char *cfgfile = 0);
  
  /// The app status, plus functions that modify it
  DpmrStatus status;
  

  /// Ticking this gives life to the objects belonging to this class
  /// This is useful for managing things that expire, pings or periodic checks
  int tick();
  
  
  
  /// Requests calls. These parse the request, do actions and send the response, using the original fastcgi func
  int dpmr_put(DpmrReq &req, FCGX_Request &request);
  int dpmr_putdone(DpmrReq &req, FCGX_Request &request);
  int dpmr_getspaceinfo(DpmrReq &req, FCGX_Request &request);
  int dpmr_getquotatoken(DpmrReq &req, FCGX_Request &request);
  int dpmr_setquotatoken(DpmrReq &req, FCGX_Request &request);
  int dpmr_delquotatoken(DpmrReq &req, FCGX_Request &request);
  int dpmr_chksum(DpmrReq &req, FCGX_Request &request);
  int dpmr_chksumdone(DpmrReq &req, FCGX_Request &request);
  int dpmr_ispullable(DpmrReq &req, FCGX_Request &request);
  int dpmr_get(DpmrReq &req, FCGX_Request &request);
  int dpmr_pulldone(DpmrReq &req, FCGX_Request &request);
  int dpmr_statpool(DpmrReq &req, FCGX_Request &request);
  int dpmr_ping(DpmrReq &req, FCGX_Request &request) {
    int r = FCGX_FPrintF(request.out, 
                 "Content-type: text/html\r\n"
                 "\r\n"
                 "Have a nice day.\r\n");
    
    return (r);
  };
  
  int dpmr_pull(DpmrReq &req, FCGX_Request &request);
  int dpmr_dochksum(DpmrReq &req, FCGX_Request &request);
  int dpmr_statfs(DpmrReq &req, FCGX_Request &request);
  
private:
  bool initdone;
  boost::recursive_mutex mtx;
  
protected:

  
  /// Send a notification to the head node about the completion of this task
  virtual void onTaskCompleted(DpmrTask &task) {}
  
  /// Send a notification to the head node about a task that is running
  virtual void onTaskRunning(DpmrTask &task) {}
};








#endif