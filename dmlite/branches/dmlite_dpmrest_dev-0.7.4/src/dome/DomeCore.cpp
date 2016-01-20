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



/** @file   DpmrCore.cpp
 * @brief  The main class where operation requests are applied to the status
 * @author Fabrizio Furano
 * @date   Dec 2015
 */


#include "DomeCore.h"
#include "DomeLog.h"







int DpmrCore::init(char *cfgfile) {
  const char *fname = "UgrConnector::init";
  {
    boost::lock_guard<boost::recursive_mutex> l(mtx);
    if (initdone) return -1;
    
    Logger::get()->setLevel(Logger::Lvl4);
    
    // Process the config file
    Log(Logger::Lvl1, domelogmask, domelogname, "------------------------------------------");
    Log(Logger::Lvl1, domelogmask, domelogname, "------------ Starting. Config: " << cfgfile);
    
    if (!cfgfile || !strlen(cfgfile)) {
      Err(fname, "No config file given." << cfgfile);
      return -1;
    }
    
    if (CFG->ProcessFile(cfgfile)) {
      Err(fname, "Error processing config file." << cfgfile << std::endl);
      return 1;
    }
    
    // Initialize the logger
    long debuglevel = CFG->GetLong("glb.debug", 1);
    Logger::get()->setLevel((Logger::Level)debuglevel);
    
    
    // The limits for the prio queues, get them from the cfg 
    std::vector<int> limits;
    limits.push_back( CFG->GetLong("head.maxchecksums", 10) );
    limits.push_back( CFG->GetLong("head.maxchecksumspernode", 10) );
    
    // Create the chksum queue
    status.checksumq = new GenPrioQueue(30, limits);
    
    // Create the queue for the callouts
    limits.clear();
    limits.push_back( CFG->GetLong("head.maxcallouts", 10) );
    limits.push_back( CFG->GetLong("head.maxcalloutspernode", 10) );
    status.filepullq = new GenPrioQueue(30, limits);
    
    // Allocate the mysql factory and configure it
    DomeMySql::configure( CFG->GetString("glb.db.host",     (char *)"localhost"),
                          CFG->GetString("glb.db.user",     (char *)"guest"),
                          CFG->GetString("glb.db.password", (char *)"none"),
                          CFG->GetLong  ("glb.db.port",     0),
                          CFG->GetLong  ("glb.db.poolsz",   10) );
    
    // Try getting a db connection and use it. If it does not work
    // an exception will just kill us, which is what we want
    DomeMySql sql;
    status.loadQuotatokens();
    status.loadFilesystems();
    
    return 0;
  }
}
  
int DpmrCore::tick() {

  // At regular intervals, one minute or so,
  // reloading the filesystems and the quotatokens is a good idea
  status.loadQuotatokens();
  status.loadFilesystems();

}
  
  
  
  
  
  
  
  int DpmrCore::dpmr_put(DpmrReq &req, FCGX_Request &request) {
    
    
  };
  int DpmrCore::dpmr_putdone(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_getspaceinfo(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_getquotatoken(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_setquotatoken(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_delquotatoken(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_chksum(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_chksumdone(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_ispullable(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_get(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_pulldone(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_statpool(DpmrReq &req, FCGX_Request &request) {};
  
  
  int DpmrCore::dpmr_pull(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_dochksum(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_statfs(DpmrReq &req, FCGX_Request &request) {};
  
  
  
  
  
  
  
  
  
  
  
  