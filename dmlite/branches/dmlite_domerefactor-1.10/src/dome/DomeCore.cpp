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
  domelogmask = Logger::get()->getMask(domelogname);
  //Info(Logger::Lvl1, fname, "Ctor " << dmlite_MAJOR <<"." << dmlite_MINOR << "." << dmlite_PATCH);
  initdone = false;
  terminationrequested = false;
}

DomeCore::~DomeCore() {
  // Interrupt accept and worker threads
  for (unsigned int i = 0; i < workers.size(); i++) {
    workers[i]->interrupt();
  }

  // Shutdown fgci
  FCGX_ShutdownPending();

  // Join workers
  for (unsigned int i = 0; i < workers.size(); i++) {
    Log(Logger::Lvl1, domelogmask, domelogname, "Joining worker " << i);
    workers[i]->join();
  }

  Log(Logger::Lvl1, domelogmask, domelogname, "Stopping ticker.");


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




// entry point for worker threads, endless loop that wait for requests from apache
// pass on processing to handlers depends on (not yet) defined REST methods
void workerFunc(DomeCore *core, int myidx) {

  Log(Logger::Lvl4, domelogmask, domelogname, "Worker: " << myidx << " started");

  int rc;


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
    Log(Logger::Lvl4, domelogmask, domelogname, "clientdn: '" << dreq.clientdn << "' clienthost: '" << dreq.clienthost <<
    "' remoteclient: '" << dreq.creds.clientName << "' remoteclienthost: '" << dreq.creds.remoteAddress);

    Log(Logger::Lvl4, domelogmask, domelogname, "req:" << dreq.verb << " cmd:" << dreq.domecmd << " query:" << dreq.object << " body: " << dreq.bodyfields.size() );


    // -------------------------
    // Generic authorization
    // Please note that authentication must be configured in the web server, not in DOME
    // -------------------------

    int i = 0;
    bool authorize = false;
    while (true) {

      char buf[1024];
      char *dn = buf;
      CFG->ArrayGetString("glb.auth.authorizeDN", buf, i);
      if ( !buf[0] ) {
        // If there ar eno directives at all then this service is closed
        if (i == 0) authorize = false;
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
        Log(Logger::Lvl2, domelogmask, domelogname, "DN '" << dn << "' authorized by whitelist.");
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

      authorize = core->status.isDNaKnownServer(dreq.clientdn);
      if (authorize)
        Log(Logger::Lvl2, domelogmask, domelogname, "DN '" << dreq.clientdn << "' is authorized as a known server of this cluster.");
    }

    // -------------------------
    // Command dispatching
    // -------------------------

    if (authorize) {

      // Client was authorized. We log the request
      Log(Logger::Lvl1, domelogmask, domelogname, "clientdn: '" << dreq.clientdn << "' clienthost: '" << dreq.clienthost <<
          "' remoteclient: '" << dreq.creds.clientName << "' remoteclienthost: '" << dreq.creds.remoteAddress << "'");

      Log(Logger::Lvl1, domelogmask, domelogname, "req:" << dreq.verb << " cmd:" << dreq.domecmd << " query:" << dreq.object << " bodyitems: " << dreq.bodyfields.size());


      // First discriminate on the HTTP request: GET/POST, etc..
      if(dreq.verb == "GET") {

        // Now dispatch based on the actual command name
        if ( dreq.domecmd == "dome_access" ) {
          core->dome_access(dreq, request);
        } else if ( dreq.domecmd == "dome_accessreplica" ) {
          core->dome_accessreplica(dreq, request);
        } else if ( dreq.domecmd == "dome_getspaceinfo" ) {
          core->dome_getspaceinfo(dreq, request);
        } else if(dreq.domecmd == "dome_chksum") {
          core->dome_chksum(dreq, request);
        } else if(dreq.domecmd == "dome_getdirspaces") {
          core->dome_getdirspaces(dreq, request);
        }else if(dreq.domecmd == "dome_getquotatoken") {
          core->dome_getquotatoken(dreq, request);
        }else if(dreq.domecmd == "dome_get") {
          core->dome_get(dreq, request);
        } else if ( dreq.domecmd == "dome_statpool" ) {
            core->dome_statpool(dreq, request);
        } else if ( dreq.domecmd == "dome_statpfn" ) {
            core->dome_statpfn(dreq, request);
        } else if ( dreq.domecmd == "dome_getstatinfo" ) {
            core->dome_getstatinfo(dreq, request);
        } else if ( dreq.domecmd == "dome_getreplicainfo" ) {
          core->dome_getreplicainfo(dreq, request);
        } else if ( dreq.domecmd == "dome_getdir" ) {
            core->dome_getdir(dreq, request);
        } else if ( dreq.domecmd == "dome_getuser" ) {
            core->dome_getuser(dreq, request);
        } else if ( dreq.domecmd == "dome_getusersvec" ) {
          core->dome_getusersvec(dreq, request);
        } else if ( dreq.domecmd == "dome_getidmap" ) {
            core->dome_getidmap(dreq, request);
        } else if (dreq.domecmd == "dome_info") {
            core->dome_info(dreq, request, myidx, authorize);
        } else if (dreq.domecmd == "dome_getcomment") {
          core->dome_getcomment(dreq, request);
        } else if (dreq.domecmd == "dome_getgroup") {
          core->dome_getgroup(dreq, request);
        } else if (dreq.domecmd == "dome_getgroupsvec") {
          core->dome_getgroupsvec(dreq, request);
        } else if (dreq.domecmd == "dome_getreplicavec") {
          core->dome_getreplicavec(dreq, request);
        } else if (dreq.domecmd == "dome_readlink") {
          core->dome_readlink(dreq, request);
        } else {
          DomeReq::SendSimpleResp(request, 418, SSTR("Command '" << dreq.object << "' unknown for a GET request. I like your style."));
        }

      } else if(dreq.verb == "HEAD"){ // meaningless placeholder
        FCGX_FPrintF(request.out,
                     "Content-type: text/html\r\n"
                     "\r\n"
                     "You sent me a HEAD request. Nice, eh ?\r\n");

      } else if(dreq.verb == "POST"){
        if ( dreq.domecmd == "dome_put" ) {
          bool success;
          core->dome_put(dreq, request, success);
        }
        else if ( dreq.domecmd == "dome_putdone" ) {

          if(core->status.role == core->status.roleHead)
            core->dome_putdone_head(dreq, request);
          else
            core->dome_putdone_disk(dreq, request);

        }
        else if ( dreq.domecmd == "dome_setquotatoken" ) {
          core->dome_setquotatoken(dreq, request);
        }
        else if ( dreq.domecmd == "dome_delreplica" ) {
          core->dome_delreplica(dreq, request);
        }
        else if ( dreq.domecmd == "dome_pfnrm" ) {
          core->dome_pfnrm(dreq, request);
        }
        else if ( dreq.domecmd == "dome_addfstopool" ) {
          core->dome_addfstopool(dreq, request);
        }
        else if ( dreq.domecmd == "dome_modifyfs" ) {
          core->dome_modifyfs(dreq, request);
        }
        else if ( dreq.domecmd == "dome_rmfs" ) {
          core->dome_rmfs(dreq, request);
        }
        else if ( dreq.domecmd == "dome_delquotatoken" ) {
          core->dome_delquotatoken(dreq, request);
        }
        else if(dreq.domecmd == "dome_chksumstatus") {
          core->dome_chksumstatus(dreq, request);
        }
        else if(dreq.domecmd == "dome_dochksum") {
          core->dome_dochksum(dreq, request);
        }
        else if(dreq.domecmd == "dome_addfstopool") {
          core->dome_addfstopool(dreq, request);
        }
        else if(dreq.domecmd == "dome_rmpool") {
          core->dome_rmpool(dreq, request);
        }
        else if(dreq.domecmd == "dome_addpool") {
          core->dome_addpool(dreq, request);
        }
        else if(dreq.domecmd == "dome_modifypool") {
          core->dome_modifypool(dreq, request);
        }
        else if(dreq.domecmd == "dome_pull") {
          core->dome_pull(dreq, request);
        }
        else if(dreq.domecmd == "dome_pullstatus") {
          core->dome_pullstatus(dreq, request);
        }
        else if(dreq.domecmd == "dome_updatexattr") {
          core->dome_updatexattr(dreq, request);
        }
        else if(dreq.domecmd == "dome_makespace") {
          core->dome_makespace(dreq, request);
        }
        else if(dreq.domecmd == "dome_modquotatoken") {
          core->dome_modquotatoken(dreq, request);
        }
        else if(dreq.domecmd == "dome_create") {
          core->dome_create(dreq, request); 
        }
        else if(dreq.domecmd == "dome_makedir") {
          core->dome_makedir(dreq, request); 
        }
        else if(dreq.domecmd == "dome_deleteuser") {
          core->dome_deleteuser(dreq, request);
        }
        else if(dreq.domecmd == "dome_newuser") {
          core->dome_newuser(dreq, request);
        }
        else if(dreq.domecmd == "dome_deletegroup") {
          core->dome_deletegroup(dreq, request);
        }
        else if(dreq.domecmd == "dome_newgroup") {
          core->dome_newgroup(dreq, request);
        }
        else if (dreq.domecmd == "dome_removedir") {
          core->dome_removedir(dreq, request);
        }
        else if (dreq.domecmd == "dome_unlink") {
          core->dome_unlink(dreq, request);
        }
        else {
          DomeReq::SendSimpleResp(request, 418, SSTR("Command '" << dreq.domecmd << "' unknown for a POST request.  Nice joke, eh ?"));

        }
      }

    } // if authorized
    else {
      // only possible to run info when unauthorized
      if(dreq.domecmd == "dome_info") {
        core->dome_info(dreq, request, myidx, authorize);
      }
      else {
        Err(domelogname, "DN '" << dreq.clientdn << " has NOT been authorized.");
        DomeReq::SendSimpleResp(request, 403, SSTR(dreq.clientdn << " is unauthorized. Sorry :-)"));
      }
    }


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
  std::string ca_path = CFG->GetString("glb.restclient.ca_path", (char *)"/etc/grid-security/certificates");
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

      Davix::DavixError::clearError(&tmp_err);
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
    status.checksumq = new GenPrioQueue(CFG->GetLong("head.checksum.qtmout", 180), limits);

    // Create the queue for the callouts
    limits.clear();
    limits.push_back( CFG->GetLong("head.filepulls.maxtotal", 10) );
    limits.push_back( CFG->GetLong("head.filepulls.maxpernode", 2) );
    status.filepullq = new GenPrioQueue(CFG->GetLong("head.filepulls.qtmout", 180), limits);

    // Allocate the mysql factory and configure it
    if(status.role == status.roleHead) {
      DomeMySql::configure( CFG->GetString("head.db.host",     (char *)"localhost"),
                            CFG->GetString("head.db.user",     (char *)"guest"),
                            CFG->GetString("head.db.password", (char *)"none"),
                            CFG->GetLong  ("head.db.port",     0),
                            CFG->GetLong  ("head.db.poolsz",   128) );

      // Try getting a db connection and use it. If it does not work
      // an exception will just kill us, which is what we want
      DomeMySql sql;

      // Only load quotatokens on the head node
      status.loadQuotatokens();
    }

    // Configure the davix pool
    davixFactory = new dmlite::DavixCtxFactory();
    davixFactory->setRequestParams(getDavixParams());
    davixPool = new dmlite::DavixCtxPool(davixFactory, CFG->GetLong("glb.restclient.poolsize", 64));
    status.setDavixPool(davixPool);

    // Load filesystems
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
    queueTicker = new boost::thread(boost::bind(&DomeCore::queueTick, this, 0));

    return 0;
  }
}



void DomeCore::tick(int parm) {

  while (! this->terminationrequested ) {
    time_t timenow = time(0);

    Log(Logger::Lvl4, domelogmask, domelogname, "Tick");

    status.tick(timenow);
    DomeTaskExec::tick();

    sleep(CFG->GetLong("glb.tickfreq", 10));
  }

}

void DomeCore::queueTick(int parm) {

  while(! this->terminationrequested) {
    time_t timenow = time(0);
    status.waitQueues();

    Log(Logger::Lvl4, domelogmask, domelogname, "Tick");
    status.tickQueues(timenow);

  }
}

/// Send a notification to the head node about the completion of this task
void DomeCore::onTaskCompleted(DomeTask &task) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. key: " << task.key);
  int key = task.key;

  PendingChecksum pending;
  PendingPull pendingpull;

  {
    boost::lock_guard<boost::recursive_mutex> l(mtx);
    std::map<int, PendingChecksum>::iterator it = diskPendingChecksums.find(key);

    if(it != diskPendingChecksums.end()) {
      Log(Logger::Lvl4, domelogmask, domelogname, "Found pending checksum. key: " << task.key);
      pending = it->second;
      diskPendingChecksums.erase(it);
      sendChecksumStatus(pending, task, true);
      Log(Logger::Lvl4, domelogmask, domelogname, "Entering. key: " << task.key);
      return;
    }

    std::map<int, PendingPull>::iterator pit = diskPendingPulls.find(key);
    if(pit != diskPendingPulls.end()) {
      pendingpull = pit->second;
      Log(Logger::Lvl4, domelogmask, domelogname, "Found pending file pull. key: " << task.key);
      diskPendingPulls.erase(pit);
      sendFilepullStatus(pendingpull, task, true);
      return;
    }
  }

  // This may be a stat
  // This would be an internal error!!!!
  Err(domelogname, "Cannot match task notification. key: " << task.key);
}

/// Send a notification to the head node about a task that is running
void DomeCore::onTaskRunning(DomeTask &task) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. key: " << task.key);
  int key = task.key;
  PendingChecksum pending;
  PendingPull pendingpull;

  {
    boost::lock_guard<boost::recursive_mutex> l(mtx);
    std::map<int, PendingChecksum>::iterator it = diskPendingChecksums.find(key);

    if(it != diskPendingChecksums.end()) {
      pending = it->second;
      Log(Logger::Lvl4, domelogmask, domelogname, "Found pending checksum. key: " << task.key);
      sendChecksumStatus(pending, task, false);
      return;
    }

    std::map<int, PendingPull>::iterator pit = diskPendingPulls.find(key);

    if(pit != diskPendingPulls.end()) {
      pendingpull = pit->second;
      Log(Logger::Lvl4, domelogmask, domelogname, "Found pending file pull. key: " << task.key);
      sendFilepullStatus(pendingpull, task, false);
      return;
    }
  }

  // This would be an internal error or just a stat request.
  Err(domelogname, "Cannot match task notification. key: " << task.key);
}


void DomeCore::fillSecurityContext(dmlite::SecurityContext &ctx, DomeReq &req) {
  
  // Take the info coming from the request
  req.fillSecurityContext(ctx);
  
  Log(Logger::Lvl4, domelogmask, domelogname,
      "clientdn: '" << ctx.credentials.clientName << "' " <<
      "clienthost: '" << ctx.credentials.remoteAddress << "' " <<
      "ctx.user.name: '" << ctx.user.name << "' " <<
      "ctx.groups: " << ctx.groups.size() << "(size) "
  );
  
  
  
  
  // Now map uid and gids into the spooky extensible
  
  if (ctx.user.name == "") {
    // This is a rotten legacy from lcg-dm
    // A client can connect ONLY if it has the right identity, i.e.
    // it's a machine in the cluster
    // If such a client claims to be root by not giving any remote identity then he is root
    ctx.user["uid"] = 0;
    ctx.user["banned"] = false;
  }
  else {
    DomeUserInfo u;
    if (status.getUser(ctx.user.name, u)) {
      ctx.user["uid"] = u.userid;
      ctx.user["banned"] = u.banned;
    } else {
      // Maybe we have to do something if the user was unknown?
      DomeMySql sql;
      if ((ctx.user.name.length() > 0) && sql.newUser(u, ctx.user.name).ok()) {
        ctx.user["uid"] = u.userid;
        ctx.user["banned"] = u.banned;
      }
      else
        Err(domelogname, "Cannot add unknown user '" << ctx.user.name << "'");
    }
    
    DomeGroupInfo g;
    for(size_t i = 0; i < ctx.groups.size(); i++) {
      if (status.getGroup(ctx.groups[i].name, g)) {
        ctx.groups[i]["gid"] = g.groupid;
        ctx.groups[i]["banned"] = g.banned;
      } else {
        // Maybe we have to do something if the group was unknown?
        DomeMySql sql;
        if ((ctx.groups[i].name.length() > 0) && sql.newGroup(g, ctx.groups[i].name).ok()) {
          ctx.groups[i]["gid"] = g.groupid;
          ctx.groups[i]["banned"] = g.banned;
        }
        else
          Err(domelogname, "Cannot add unknown group '" << ctx.groups[i].name << "'");
      }
      
      
    }
  }
  
}

