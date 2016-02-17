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
  
  if(dmpool) {
    delete dmpool;
    dmpool = NULL;
  }
  
  if(davixPool) {
    delete davixPool;
    davixPool = NULL;
  }

  if(davixFactory) {
    delete davixFactory;
    davixFactory = NULL;
  }

  if (ticker) {
    Log(Logger::Lvl1, domelogmask, domelogname, "Joining ticker.");
    ticker->interrupt();
    ticker->join();
    delete ticker;
    ticker = 0;
    Log(Logger::Lvl1, domelogmask, domelogname, "Joined ticker.");
    
  }
  
  
}



bool DomeCore::LfnMatchesPool(std::string lfn, std::string pool) {
  
  // Lock status!
  boost::unique_lock<boost::recursive_mutex> l(status);
  std::string lfn1(lfn);
  
  while (lfn1.length() > 0) {
    
    Log(Logger::Lvl4, domelogmask, domelogname, "Processing: '" << lfn1 << "'");
    // Check if any matching quotatoken exists
    std::pair <std::multimap<std::string, DomeQuotatoken>::iterator, std::multimap<std::string, DomeQuotatoken>::iterator> myintv;
    myintv = status.quotas.equal_range(lfn1);
    
    if (myintv.first != status.quotas.end()) {
      for (std::multimap<std::string, DomeQuotatoken>::iterator it = myintv.first; it != myintv.second; ++it) {
        if (it->second.poolname == pool) {
          
          Log(Logger::Lvl1, domelogmask, domelogname, "pool: '" << it->second.poolname << "' matches path '" << lfn);    
          
          return true;
        }
      }
    }
    
    // No match found, look upwards by trimming the last token from absPath
    size_t pos = lfn1.rfind("/");
    lfn1.erase(pos);
  }
  return true;
  
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
    
    if (Logger::get()->getLevel() >= Logger::Lvl4) {
      
      for (char **envp = request.envp ; *envp; ++envp) {
        Log(Logger::Lvl4, domelogmask, domelogname, "Worker: " << myidx << " FCGI env: " << *envp);
      }
    }
    
    DomeReq dreq(request);
    Log(Logger::Lvl4, domelogmask, domelogname, "--------- New request. Worker: " << myidx <<
    " clientdn: '" << dreq.clientdn << "' clienthost: '" << dreq.clienthost <<
    "' remoteclient: '" << dreq.remoteclientdn << "' remoteclienthost: '" << dreq.remoteclienthost);
    
    Log(Logger::Lvl4, domelogmask, domelogname, "Worker: " << myidx << " req:" << dreq.verb << " cmd:" << dreq.domecmd << " query:" << dreq.object << " bodyitems:" << dreq.bodyfields.size());
    
    
    // -------------------------
    // Generic authorization
    // Please note that authentication must be configured in the web server, not in DOME
    // -------------------------
    
    int i = 0;
    bool authorize;
    while (true) {
      
      char buf[1024];
      char *dn = buf;
      CFG->ArrayGetString("glb.auth.authorizeDN", buf, i);
      if ( !buf[0] ) {
        // If there ar eno directives at all then this service is open to every dn
        if (i == 0) authorize = true;
        break;
      }
      
      if (buf[0] == '"') {
        
        if (buf[strlen(buf)-1] != '"') {
          Err("workerFunc", "Mismatched quotes in authorizeDN directive. Can't authorize DN " << dreq.clientdn);
          continue;
        }
        
        buf[strlen(buf)-1] = '\0';
        dn = buf+1;
        
      }
      
      if ( !strncmp(dn, dreq.clientdn.c_str(), sizeof(buf)) ) {
        // Authorize if the client DN can be found in the config whitelist
        Log(Logger::Lvl1, domelogmask, domelogname, "DN '" << dn << "' authorized by whitelist.");
        authorize = true;
        break;
      }
      
      i++;
    }
    
    if (!authorize) {
      // The whitelist in the config file did not authorize
      // Anyway this call may come from a server that was implicitly known, e.g.
      // head node trusts all the disk nodes that are registered in the filesystem table
      // disk node trusts head node as defined in the config file
      
      authorize = core->isDNaKnownServer(dreq.clientdn);
      if (authorize)
        Log(Logger::Lvl2, domelogmask, domelogname, "DN '" << dreq.clientdn << "' is a known server of this cluster.");
    }
    
    // -------------------------
    // Command dispatching
    // -------------------------
    
    if (authorize) {
      // First discriminate on the HTTP request: GET/POST, etc..
      if(dreq.verb == "GET") {
        
        // Now dispatch based on the actual command name
        if ( dreq.domecmd == "dome_getspaceinfo" ) {
          core->dome_getspaceinfo(dreq, request);
        } else if(dreq.domecmd == "dome_chksum") {
          core->dome_chksum(dreq, request);
        } else if(dreq.domecmd == "dome_chksumstatus") {
          core->dome_chksumstatus(dreq, request);
        } else if(dreq.domecmd == "dome_getdirspaces") {
          core->dome_getdirspaces(dreq, request);
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
        
      } else if(dreq.verb == "PUT"){ 
        
        core->dome_put(dreq, request);
        
        
      } else if(dreq.verb == "POST"){ 
        if ( dreq.domecmd == "dome_putdone" ) {
          
          if(core->status.role == core->status.roleHead)
            core->dome_putdone_head(dreq, request);
          else
            core->dome_putdone_disk(dreq, request);
          
        }
        else {
          FCGX_FPrintF(request.out,
                       "Content-type: text/html\r\n"
                       "\r\n"
                       "A POST request without a DOME command. Nice joke, eh ?\r\n");
        }
      }
      
    } // if authorized
    else
      Err(domelogname, "DN '" << dreq.clientdn << " has NOT been authorized.");
    
    
    FCGX_Finish_r(&request);
  }
  
  Log(Logger::Lvl4, domelogmask, domelogname, "Worker: " << myidx << " finished");
  
  
}

static Davix::RequestParams getDavixParams() {
  Davix::RequestParams params;

  // set timeouts, etc
  long conn_timeout = CFG->GetLong("glb.restclient.conn_timeout", 15);
  struct timespec spec_timeout;
  spec_timeout.tv_sec = conn_timeout;
  spec_timeout.tv_nsec = 0;
  params.setConnectionTimeout(&spec_timeout);
  Log(Logger::Lvl1, domelogmask, domelogname, "Davix: Connection timeout is set to : " << conn_timeout);

  long ops_timeout = CFG->GetLong("glb.restclient.ops_timeout", 15);
  spec_timeout.tv_sec = ops_timeout;
  spec_timeout.tv_nsec = 0;
  params.setOperationTimeout(&spec_timeout);
  Log(Logger::Lvl1, domelogmask, domelogname, "Davix: Operation timeout is set to : " << ops_timeout);
    
  // get ssl check
  bool ssl_check = CFG->GetBool("glb.restclient.ssl_check", true);
  params.setSSLCAcheck(ssl_check);
  Log(Logger::Lvl1, domelogmask, domelogname, "SSL CA check for davix is set to  " + std::string((ssl_check) ? "TRUE" : "FALSE"));

  // ca check
  std::string ca_path = CFG->GetString("glb.restclient.ca_path", (char *)"/etc/grid/security/certificates");
  if( !ca_path.empty()) {
    Log(Logger::Lvl1, domelogmask, domelogname, "CA Path :  " << ca_path);
    params.addCertificateAuthorityPath(ca_path);
  }

  // credentials
  Davix::X509Credential cred;
  Davix::DavixError* tmp_err = NULL;

  cred.loadFromFilePEM(CFG->GetString("glb.restclient.cli_private_key", (char *)""), CFG->GetString("glb.restclient.cli_certificate", (char *)""), "", &tmp_err);
  if( tmp_err ){
      std::ostringstream os;
      os << "Cannot load cert-privkey " << CFG->GetString("glb.restclient.cli_certificate", (char *)"") << "-" <<
            CFG->GetString("glb.restclient.cli_private_key", (char *)"") << ", Error: "<< tmp_err->getErrMsg();
        
      throw dmlite::DmException(EPERM, os.str());
  }
  params.setClientCertX509(cred);
  return params;
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
    
    if (status.role == status.roleHead) 
      status.headnodename = status.myhostname;
    else {
      // Now get the host name of the head node
      Davix::Uri uri(CFG->GetString("disk.headnode.domeurl", (char *)""));
      status.headnodename = uri.getHost();
    }
  
    Log(Logger::Lvl1, domelogmask, domelogname, "My head node hostname is: " << status.headnodename);
    
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
    
    dmpool = new DmlitePool(CFG->GetString("glb.dmlite.configfile", (char *)"/etc/dmlite.conf"));
    
    // Configure the davix pool
    davixFactory = new dmlite::DavixCtxFactory();
    davixFactory->setRequestParams(getDavixParams());
    davixPool = new dmlite::DavixCtxPool(davixFactory, CFG->GetLong("glb.restclient.poolsize", 15));
    status.setDavixPool(davixPool);

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

bool DNMatchesHost(std::string dn, std::string host) {
  std::string s = "/CN="+host;
  
  // Simple version, if the common name appears in the dn then we are right
  if (dn.find(s) != std::string::npos) return true;
  
  return false;
}

bool DomeCore::isDNaKnownServer(std::string dn) {
  // We know this server if its DN matches our own hostname, it's us !
  if (DNMatchesHost(dn, status.myhostname)) return true;
  
  // We know this server if its DN matches the DN of the head node
  if (DNMatchesHost(dn, CFG->GetString("disk.headnode.domeurl", (char *)""))) return true;
  
  // We know this server if its DN matches the hostname of a disk server
  for (std::set<std::string>::iterator i = status.servers.begin() ; i != status.servers.end(); i++) {
    if (DNMatchesHost(dn, *i)) return true;
  }
  
  // We don't know this server
  return false;
}



/// Send a notification to the head node about the completion of this task
void DomeCore::onTaskCompleted(DomeTask &task) {
  
}

/// Send a notification to the head node about a task that is running
void DomeCore::onTaskRunning(DomeTask &task) {
  
}
