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


DpmrCore::DpmrCore() {
  const char *fname = "DpmrCore::ctor";
  domelogmask = Logger::get()->getMask(domelogname);
  //Info(Logger::Lvl1, fname, "Ctor " << dmlite_MAJOR <<"." << dmlite_MINOR << "." << dmlite_PATCH);
  initdone = false;
  terminationrequested = false;
}

DpmrCore::~DpmrCore() {
  // Interrupt accept and worker threads
  for (int i = 0; i < workers.size(); i++) {
    workers[i]->interrupt();
  }
  
  // Shutdown fgci
  FCGX_ShutdownPending();
  
  // Join workers
  for (int i = 0; i < workers.size(); i++) {
    Log(Logger::Lvl1, domelogmask, domelogname, "Joining worker " << i);
    workers[i]->join();
  }
  
  Log(Logger::Lvl1, domelogmask, domelogname, "Stopping ticker.");
  
  if (ticker) {
    Log(Logger::Lvl1, domelogmask, domelogname, "Joining ticker.");
    ticker->interrupt();
    ticker->join();
    delete ticker;
    ticker = 0;
    Log(Logger::Lvl1, domelogmask, domelogname, "Joined ticker.");
    
  }
  
  
}


// entry point for worker threads, endless loop that wait for requests from apache
// pass on processing to handlers depends on (not yet) defined REST methods
void workerFunc(DpmrCore *core, int myidx) {
  
  Log(Logger::Lvl4, domelogmask, domelogname, "Worker: " << myidx << " started");
  
  int rc, i = 0;
  
  
  FCGX_Request request;
  FCGX_InitRequest(&request, core->fcgi_listenSocket, 0);
  
  while( !core->terminationrequested )
  {
    // thread safety seems to be platform dependant... serialise the accept loop just in case
    // NOTE: although we are multithreaded, this is a very naive way of dealing with this. The future, proper
    // implementation should keep 2 threads doing only accept/enqueue, and all the others dequeue/working
    
    {
      boost::lock_guard<boost::mutex> l(core->accept_mutex);
      
      rc = FCGX_Accept_r(&request);
    }
    
    if (rc < 0) {// Something broke in fcgi... maybe we have to exit ? MAH ?
      Err("workerFunc", "Accept returned " << rc);
      break;
    }
    
    if (Logger::get()->getLevel() >= Logger::Lvl4)
      for (char **envp = request.envp ; *envp; ++envp) {
        Log(Logger::Lvl4, domelogmask, domelogname, "Worker: " << myidx << " FCGI env: " << *envp);
      }
    
    std::string request_method = FCGX_GetParam("REQUEST_METHOD", request.envp);
    std::string query = FCGX_GetParam("PATH_INFO", request.envp);
    
    Log(Logger::Lvl4, domelogmask, domelogname, "Worker: " << myidx << " req:" << request_method << " query:" << query);
    
    // split query string first before comparing
    //queryMap query_map;
    //queryToMap(query_map, query);       
    
    if(request_method == "GET") {
      
      
      if (query == "/info") {
        FCGX_FPrintF(request.out, 
                   "Content-type: text\r\n"
                   "\r\n"
                   "Hi, This is a GET\r\n");
        
        FCGX_FPrintF(request.out, "Server PID: %d - Thread Index: %d \r\n\r\n", getpid(), myidx);
        for (char **envp = request.envp ; *envp; ++envp)
        {
          FCGX_FPrintF(request.out, "%s \r\n", *envp);
        }
        
      }
      
    } else if(request_method == "HEAD"){ // meaningless placeholder
      FCGX_FPrintF(request.out, 
                   "Content-type: text/html\r\n"
                   "\r\n"
                   "This is a HEAD\r\n");
      
    } else if(request_method == "PUT"){ // meaningless placeholder
      FCGX_FPrintF(request.out, 
                   "Content-type: text/html\r\n"
                   "\r\n"
                   "This is a PUT\r\n");
    }
    
    FCGX_Finish_r(&request);
  }
  
  Log(Logger::Lvl4, domelogmask, domelogname, "Worker: " << myidx << " finished");
  
  
}




int DpmrCore::init(const char *cfgfile) {
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
    
    if (CFG->ProcessFile((char *)cfgfile)) {
      Err(fname, "Error processing config file." << cfgfile << std::endl);
      return 1;
    }
    
    // Initialize the logger
    long debuglevel = CFG->GetLong("glb.debug", 1);
    Logger::get()->setLevel((Logger::Level)debuglevel);
    
    std::string r = CFG->GetString("glb.role", (char *)"head");
    if (r == "head") role = roleHead;
    else if (r == "disk") role = roleDisk;
    else {
      Err(fname, "Invalid role: '" << r << "'");
      return -1;
    }
    
    // The limits for the prio queues, get them from the cfg 
    std::vector<size_t> limits;
    limits.push_back( CFG->GetLong("head.maxchecksums", 10) );
    limits.push_back( CFG->GetLong("head.maxchecksumspernode", 2) );
    
    // Create the chksum queue
    status.checksumq = new GenPrioQueue(30, limits);
    
    // Create the queue for the callouts
    limits.clear();
    limits.push_back( CFG->GetLong("head.maxcallouts", 10) );
    limits.push_back( CFG->GetLong("head.maxcalloutspernode", 2) );
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
    
    
    // Startup the FCGI mechanics
    // init must be called for multithreaded applications
    FCGX_Init();
    
    // Standalone external servers have to specify a port number and must be run
    // by a proper script (mod_fcgi provides one)
    // If the port number is 0 then we assume that the lifetime of this daemon
    // will be managed by Apache
    int portnum = CFG->GetLong("glb.fcgi.listenport", 0);
    fcgi_listenSocket = 0;
    if (portnum) {
      char buf[32];
      sprintf(buf, ":%d", portnum);
      Log(Logger::Lvl1, domelogmask, domelogname, "Setting fcgi listen port to '" << buf << "'");
      fcgi_listenSocket = FCGX_OpenSocket( buf, 100 );
    }
    
    if( fcgi_listenSocket < 0 ) {
      Err(fname, "FCGX_OpenSocket() error " << fcgi_listenSocket);
      
      return -1;
    }
    
    // Create our pool of threads. Please note tha this approach may have some limitations.
    // Best would be a couple of threads that do only accept and enqueue
    // and a larger pool of workers
    Log(Logger::Lvl1, domelogmask, domelogname, "Creating " << CFG->GetLong("glb.workers", 300) << " workers.");
    
    for (int i = 0; i < CFG->GetLong("glb.workers", 300); i++) {
      workers.push_back(new boost::thread(workerFunc, this, i));
    }
    
    // Start the ticker
    Log(Logger::Lvl1, domelogmask, domelogname, "Starting ticker.");
    ticker = new boost::thread(boost::bind(&DpmrCore::tick, this, 0));
    
    return 0;
  }
}

void DpmrCore::tick(int parm) {
  
  time_t lastreload = time(0);
  while (! this->terminationrequested ) {
    time_t timenow = time(0);
    
    Log(Logger::Lvl4, domelogmask, domelogname, "Tick");
  
    if ( timenow - lastreload >= CFG->GetLong("glb.reloadfsquotas", 60)) {
      // At regular intervals, one minute or so,
      // reloading the filesystems and the quotatokens is a good idea
      Log(Logger::Lvl4, domelogmask, domelogname, "Reloading quotas and filesystems");
      status.loadQuotatokens();
      status.loadFilesystems();
      lastreload = time(0);
    }
  
    status.tick();
    
    
    sleep(CFG->GetLong("glb.tickfreq", 10));
  }
  
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


/// Send a notification to the head node about the completion of this task
void DpmrCore::onTaskCompleted(DomeTask &task) {
  
}

/// Send a notification to the head node about a task that is running
void DpmrCore::onTaskRunning(DomeTask &task) {
  
}








