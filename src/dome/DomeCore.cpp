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



/** @file   DomeCore.cpp
 * @brief  The main class where operation requests are applied to the status
 * @author Fabrizio Furano
 * @date   Dec 2015
 */


#include "DomeCore.h"
#include "DomeLog.h"
#include "DomeDmlitePool.h"
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <sys/vfs.h>
#include <unistd.h>

DomeCore::DomeCore() {
  const char *fname = "DomeCore::ctor";
  domelogmask = Logger::get()->getMask(domelogname);
  //Info(Logger::Lvl1, fname, "Ctor " << dmlite_MAJOR <<"." << dmlite_MINOR << "." << dmlite_PATCH);
  initdone = false;
  terminationrequested = false;
}

DomeCore::~DomeCore() {
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
void workerFunc(DomeCore *core, int myidx) {

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

    DomeReq dreq(request);

    Log(Logger::Lvl4, domelogmask, domelogname, "Worker: " << myidx << " req:" << dreq.verb << " cmd:" << dreq.domecmd << " query:" << dreq.object << " bodyitems:" << dreq.bodyfields.size());

    // -------------------------
    // Command dispatching
    // -------------------------
    
    // First discriminate on the HTTP request: GET/POST, etc..
    if(dreq.verb == "GET") {

      // Now dispatch based on the actual command name
      if ( dreq.domecmd == "dome_getspaceinfo" ) {
        core->dome_getspaceinfo(dreq, request);
      } else if(dreq.domecmd == "dome_chksum") {
        core->dome_chksum(dreq, request);
      }
      else
      if ( dreq.domecmd == "dome_statpool" ) {
        core->dome_statpool(dreq, request);
      }
      else
        // Very useful sort of echo service for FastCGI.
        // Will return to the client a detailed summary of his request
        if (dreq.object == "/info") {
          FCGX_FPrintF(request.out,
                   "Content-type: text\r\n"
                   "\r\n"
                   "Hi, This is a GET, and you may like it.\r\n");

        FCGX_FPrintF(request.out, "Server PID: %d - Thread Index: %d \r\n\r\n", getpid(), myidx);
        for (char **envp = request.envp ; *envp; ++envp)
        {
          FCGX_FPrintF(request.out, "%s \r\n", *envp);

        }

      }

    } else if(dreq.verb == "HEAD"){ // meaningless placeholder
      FCGX_FPrintF(request.out,
                   "Content-type: text/html\r\n"
                   "\r\n"
                   "You sent me a HEAD request. Nice, eh ?\r\n");

    } else if(dreq.verb == "PUT"){ // meaningless placeholder
      FCGX_FPrintF(request.out,
                   "Content-type: text/html\r\n"
                   "\r\n"
                   "This is a PUT\r\n");
    }

    FCGX_Finish_r(&request);
  }

  Log(Logger::Lvl4, domelogmask, domelogname, "Worker: " << myidx << " finished");


}




int DomeCore::init(const char *cfgfile) {
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
    if (r == "head") status.role = status.roleHead;
    else if (r == "disk") status.role = status.roleDisk;
    else {
      Err(fname, "Invalid role: '" << r << "'");
      return -1;
    }

    // The limits for the prio queues, get them from the cfg
    std::vector<size_t> limits;
    limits.push_back( CFG->GetLong("head.checksum.maxtotal", 10) );
    limits.push_back( CFG->GetLong("head.checksum.maxpernode", 2) );

    // Create the chksum queue
    status.checksumq = new GenPrioQueue(CFG->GetLong("head.checksum.qtmout", 30), limits);

    // Create the queue for the callouts
    limits.clear();
    limits.push_back( CFG->GetLong("head.filepulls.maxtotal", 10) );
    limits.push_back( CFG->GetLong("head.filepulls.maxpernode", 2) );
    status.filepullq = new GenPrioQueue(CFG->GetLong("head.filepulls.qtmout", 30), limits);

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
    ticker = new boost::thread(boost::bind(&DomeCore::tick, this, 0));

    return 0;
  }
}

void DomeCore::tick(int parm) {

  while (! this->terminationrequested ) {
    time_t timenow = time(0);

    Log(Logger::Lvl4, domelogmask, domelogname, "Tick");



    status.tick(timenow);


    sleep(CFG->GetLong("glb.tickfreq", 10));
  }

}






/// Send a notification to the head node about the completion of this task
void DomeCore::onTaskCompleted(DomeTask &task) {

}

/// Send a notification to the head node about a task that is running
void DomeCore::onTaskRunning(DomeTask &task) {

}
