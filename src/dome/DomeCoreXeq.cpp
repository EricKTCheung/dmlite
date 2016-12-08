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



/** @file   DomeCoreXeq.cpp
 * @brief  A part of the implementation of DomeCore. Functions implementing commands
 * @author Fabrizio Furano
 * @date   Feb 2016
 */


#include "DomeCore.h"
#include "DomeLog.h"
#include "utils/DomeUtils.h"
#include "DomeDmlitePool.h"
#include <sys/vfs.h>
#include <unistd.h>
#include <time.h>
#include <sys/param.h>
#include <stdio.h>
#include <algorithm>
#include <functional>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/optional/optional.hpp>

#include "cpp/authn.h"
#include "cpp/dmlite.h"
#include "cpp/catalog.h"
#include "cpp/utils/urls.h"
#include "utils/checksums.h"
#include "utils/DomeTalker.h"




using namespace dmlite;

// Creates a new logical file entry, and all its parent directories

int mkdirminuspandcreate(dmlite::Catalog *catalog,
                         const std::string& filepath,
                         std::string  &parentpath,
                         ExtendedStat &parentstat,
                         ExtendedStat &statinfo) throw (DmException) {

  if (filepath.empty())
    throw DmException(EINVAL, "Empty path. Internal error ?");

  // Get the absolute path
  std::string path = filepath;
  if ( filepath[0] != '/' )
    path = catalog->getWorkingDir() + "/" + filepath;

  if ( path[0] != '/' )
    path.insert(0, "/");

  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. Absolute path: '" << path << "'");

  std::vector<std::string> components = Url::splitPath(path);
  std::vector<std::string> todo;
  std::string name;
  bool parentok = false;

  components.pop_back();

  // Make sure that all the parent dirs exist
  DomeMySql sql;

  do {

    std::string ppath = Url::joinPath(components);
    ExtendedStat st;

    // Try to get the stat of the parent
      DmStatus ret = sql.getStatbyLFN(st, ppath);
      if (!ret.ok()) {
        // No parent means that we have to create it later
        Log(Logger::Lvl4, domelogmask, domelogname, "Path to create: '" << ppath << "'");
        name = components.back();
        components.pop_back();

        todo.push_back(ppath);
      }

      if (!parentok) {
        parentstat = st;
        parentpath = ppath;
        // This means that we successfully processed at least the parent dir
        // that we have to return
        parentok = true;
        break;
      }




  } while ( !components.empty() );


  // Here we have a todo list of directories that we have to create
  // .... so we do it
  while (!todo.empty()) {
    std::string p = todo.back();
    todo.pop_back();

    // Try to get the stat of the parent
    try {
      catalog->makeDir(p, 0774);
    } catch (DmException e) {
      // If we can't create the dir then this is a serious error, unless it already exists
      Err(domelogname, "Cannot create path '" << p << "' err: " << e.code() << "-" << e.what());
      if (e.code() != EEXIST) throw;
    }

    // Set proper ownership
    //catalog->setOwner(parentpath, parentstat.stat.st_uid, parentstat.stat.st_gid);
  }

  // If a miracle took us here, we only miss to create the final file
  try {
    catalog->create(filepath, 0664);
    catalog->setMode(filepath, 0664); // horrible workaround to make sure the memcached plugin
                                      // invalidates its previous contents. Necessary during the file
                                      // pulling workflow.
    DmStatus st = sql.getStatbyLFN(statinfo, filepath);
    if (!st.ok())
      throw st.exception();



  } catch (DmException e) {
    // If we can't create the file then this is a serious error
    Err(domelogname, "Cannot create file '" << filepath << "'");
    throw;
  }

  return 0;
}

// pick from the list of appropriate filesystems, given the hints
std::vector<DomeFsInfo> DomeCore::pickFilesystems(const std::string &pool,
                                       const std::string &host,
                                       const std::string &fs) {
  std::vector<DomeFsInfo> selected;

  boost::unique_lock<boost::recursive_mutex> l(status);
  Log(Logger::Lvl2, domelogmask, domelogname, "Picking from a list of " << status.fslist.size() << " filesystems to write into");

  for(unsigned int i = 0; i < status.fslist.size(); i++) {
    std::string fsname = SSTR(status.fslist[i].server << ":" << status.fslist[i].fs);
    Log(Logger::Lvl3, domelogmask, domelogname, "Checking '" << fsname << "' of pool '" << status.fslist[i].poolname << "'");

    if(!status.fslist[i].isGoodForWrite()) {
      Log(Logger::Lvl3, domelogmask, domelogname, fsname << " ruled out - not good for write");
      continue;
    }

    if(!pool.empty() && status.fslist[i].poolname != pool) {
      Log(Logger::Lvl3, domelogmask, domelogname, fsname << " ruled out - does not match pool hint");
      continue;
    }

    if(!host.empty() && status.fslist[i].server != host) {
      Log(Logger::Lvl3, domelogmask, domelogname, fsname << " ruled out - does not match host hint");
      continue;
    }

    if(!fs.empty() && status.fslist[i].fs != fs) {
      Log(Logger::Lvl3, domelogmask, domelogname, fsname << " ruled out - does not match fs hint");
      continue;
    }

    // fslist[i], you win
    Log(Logger::Lvl3, domelogmask, domelogname, fsname << " has become a candidate for writing.");
    selected.push_back(status.fslist[i]);
  }
  return selected;
}

int DomeCore::dome_put(DomeReq &req, FCGX_Request &request, bool &success, struct DomeFsInfo *dest, std::string *destrfn, bool dontsendok) {

  success = false;
  DomeQuotatoken token;

  // fetch the parameters, lfn and placement suggestions
  std::string lfn = req.bodyfields.get<std::string>("lfn", "");
  std::string addreplica_ = req.bodyfields.get<std::string>("additionalreplica", "");
  std::string pool = req.bodyfields.get<std::string>("pool", "");
  std::string host = req.bodyfields.get<std::string>("host", "");
  std::string fs = req.bodyfields.get<std::string>("fs", "");

  bool addreplica = false;
  if ( (addreplica_ == "true") || (addreplica_ == "yes") || (addreplica_ == "1") || (addreplica_ == "on") )
    addreplica = true;

  // Log the parameters, level 1
  Log(Logger::Lvl1, domelogmask, domelogname, "Entering. lfn: '" << lfn <<
    "' addreplica: " << addreplica << " pool: '" << pool <<
    "' host: '" << host << "' fs: '" << fs << "'");


  // if(!req.remoteclientdn.size() || !req.remoteclienthost.size()) {
  //   return DomeReq::SendSimpleResp(request, 501, SSTR("Invalid remote client or remote host credentials: " << req.remoteclientdn << " - " << req.remoteclienthost));
  // }

  // Give errors for combinations of the parameters that are obviously wrong
  if ( (host != "") && (pool != "") ) {
    // Error! Log it as such!, level1
    return DomeReq::SendSimpleResp(request, DOME_HTTP_BAD_REQUEST, "The pool hint and the host hint are mutually exclusive.");
  }

  // default minfreespace is 4GB. Gets overridden by the individual pool's value
  int64_t minfreespace_bytes = CFG->GetLong("head.put.minfreespace_mb", 1024*4) * 1024*1024;

  // use quotatokens?
  // TODO: more than quotatoken may match, they all should be considered
  if(pool.empty() && host.empty() && fs.empty()) {

    if(!status.whichQuotatokenForLfn(lfn, token)) {
      return DomeReq::SendSimpleResp(request, DOME_HTTP_BAD_REQUEST, "No quotatokens match lfn, and no hints were given.");
    }

    // Check if the quotatoken admits writes for the groups the client belongs to
    // Please note that the clients' groups come through fastcgi,
    // instead the groups in the quotatoken are in the form of gids
    if(!status.canwriteintoQuotatoken(req, token)) {

      // Prepare a complete error message that describes why the user can't write


      std::string userfqans, tokengroups, s;
      DomeGroupInfo gi;

      // Start prettyprinting the groups the user belongs to
      for (unsigned int i = 0; i < req.creds.groups.size(); i++) {
        userfqans += req.creds.groups[i];
        if (status.getGroup(req.creds.groups[i], gi)) {
          userfqans += SSTR( "(" << gi.groupid << ")" );

        }
        else
          userfqans += "(<unknown group>)";


        if (i < req.creds.groups.size()-1) userfqans += ",";
      }

      // Then prettyprint the gids of the selected token
      for (unsigned int i = 0; i < token.groupsforwrite.size(); i++) {
        int g = atoi(token.groupsforwrite[i].c_str());
        if (status.getGroup(g, gi)) {
          tokengroups += SSTR( gi.groupname << "(" << gi.groupid << ")" );

        }
        else {
          tokengroups += SSTR( "<unknown group>(" << token.groupsforwrite[i] << ")" );
        }

        if (i < token.groupsforwrite.size()-1) tokengroups += ",";
      }

      std::string err = SSTR("User '" << req.creds.clientName << " with fqans '" << userfqans <<
        "' cannot write to quotatoken '" << token.s_token << "(" << token.u_token <<
        ")' with gids: '" << tokengroups);

      Log(Logger::Lvl1, domelogmask, domelogname, err);
      return DomeReq::SendSimpleResp(request, DOME_HTTP_DENIED, err);
    }

    char pooltype;

    // Eventually override the default size
    status.getPoolInfo(token.poolname, minfreespace_bytes, pooltype);

    if(!status.fitsInQuotatoken(token, minfreespace_bytes)) {
      std::string err = SSTR("Unable to complete put for '" << lfn << "' - quotatoken '" << token.u_token << "' has insufficient free space. minfreespace_bytes: " << minfreespace_bytes);
      Log(Logger::Lvl1, domelogmask, domelogname, err);
      return DomeReq::SendSimpleResp(request, DOME_HTTP_INSUFFICIENT_STORAGE, err);
    }
    pool = token.poolname;
  }

  // populate the list of candidate filesystems
  std::vector<DomeFsInfo> selectedfss = pickFilesystems(pool, host, fs);

  // no filesystems match? return error
  if (selectedfss.empty()) {
    return DomeReq::SendSimpleResp(request, DOME_HTTP_BAD_REQUEST,
           SSTR("No filesystems match the given logical path and placement hints. "
                "HINT: make sure that the correct pools are associated to the LFN, and that they are writable and online. "
                "Selected pool: '" << pool << "'. Selected host: '" << host << "'. Selected fs: '" << fs << "'"));
  }


  // If we are replicating an existing file, the new replica must go into a filesystem that
  // does not contain it already
  DmlitePoolHandler stack(status.dmpool);
  try {
    if (addreplica) {
      // Get the list of the replicas of this lfn
      std::vector<Replica> replicas = stack->getCatalog()->getReplicas(lfn);
      // remove from the fslist the filesystems that match with any replica
      for (int i = selectedfss.size()-1; i >= 0; i--) {
        bool dropfs = false;

        // Loop on the replicas
        for(size_t j = 0; j < replicas.size(); j++) {
          std::string rfn = replicas[j].rfn;
          std::string pfn;
          size_t pos = rfn.find(":");
          if (pos == std::string::npos) pfn = rfn;
          else
            pfn = rfn.substr(rfn.find(":")+1, rfn.size());

          if (status.PfnMatchesFS(replicas[j].server, pfn, selectedfss[i])) {
            dropfs = true;
            break;
          }
        }

        if (dropfs) {
          Log(Logger::Lvl4, domelogmask, domelogname, "Filesystem: '" << selectedfss[i].server << ":" << selectedfss[i].fs <<
          "' already has a replica of '" << lfn << "', skipping");
          selectedfss.erase(selectedfss.begin()+i);
        }
      }

    }
  } catch (DmException e) {


  }


  // If no filesystems remain, return error "filesystems full for path ..."
  if ( !selectedfss.size() ) {
    // Error!
    return DomeReq::SendSimpleResp(request, DOME_HTTP_INSUFFICIENT_STORAGE,
           SSTR("No filesystems can host an additional replica for lfn:'" << lfn));
  }

  // Remove the filesystems that have less than the minimum free space available
  for (int i = selectedfss.size()-1; i >= 0; i--) {
    if (selectedfss[i].freespace < minfreespace_bytes) {
        Log(Logger::Lvl2, domelogmask, domelogname, "Filesystem: '" << selectedfss[i].server << ":" << selectedfss[i].fs <<
          "' has less than " << minfreespace_bytes << "bytes free");
        selectedfss.erase(selectedfss.begin()+i);
    }
  }

  // If no filesystems remain, return error "filesystems full for path ..."
  if ( !selectedfss.size() ) {
    // Error!
    return DomeReq::SendSimpleResp(request, DOME_HTTP_INSUFFICIENT_STORAGE, "All matching filesystems are full.");
  }


  // Sort the selected filesystems by decreasing free space
  std::sort(selectedfss.begin(), selectedfss.end(), DomeFsInfo::pred_decr_freespace());

  // Use the free space as weight for a random choice among the filesystems
  // Nice algorithm taken from http://stackoverflow.com/questions/1761626/weighted-random-numbers#1761646
  long sum_of_weight = 0;
  int fspos = 0;
  for (unsigned int i = 0; i < selectedfss.size(); i++) {
    sum_of_weight += (selectedfss[i].freespace >> 20);
  }
  // RAND_MAX is sufficiently big for this purpose
  int rnd = random() % sum_of_weight;
  for(unsigned int i=0; i < selectedfss.size(); i++) {
    if(rnd < (selectedfss[i].freespace >> 20)) {
      fspos = i;
      break;
    }
    rnd -= (selectedfss[i].freespace >> 20);
  }

  // We have the fs, build the final pfn for the file
  //  fs/group/date/basename.r_ordinal.f_ordinal
  Log(Logger::Lvl1, domelogmask, domelogname, "Selected fs: '" << selectedfss[fspos].server << ":" << selectedfss[fspos].fs <<
        " from " << selectedfss.size() << " matchings for lfn: '" << lfn << "'");

  // Fetch the time
  time_t rawtimenow = time(0);
  struct tm tmstruc;
  char timestr[16], suffix[32];
  localtime_r(&rawtimenow, &tmstruc);
  strftime (timestr, 11, "%F", &tmstruc);

  // Parse the lfn and pick the 4th token, likely the one with the VO name
  std::vector<std::string> vecurl = dmlite::Url::splitPath(lfn);
  sprintf(suffix, ".%ld.%ld", status.getGlobalputcount(), rawtimenow);

  if (vecurl.size() < 5) {
    std::ostringstream os;
    os << "Unable to get vo name from the lfn: " << lfn;

    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, DOME_HTTP_UNPROCESSABLE, os);
  }


  std::string pfn = selectedfss[fspos].fs + "/" + vecurl[4] + "/" + timestr + "/" + *vecurl.rbegin() + suffix;

  Log(Logger::Lvl4, domelogmask, domelogname, "lfn: '" << lfn << "' --> '" << selectedfss[fspos].server << ":" << pfn << "'");

  // NOTE: differently from the historical dpmd, here we do not create the remote path/file
  // of the replica in the disk. We jsut make sure that the LFN exists
  // The replica in the catalog instead is created here

  // Create the logical catalog entry, if not already present. We also create the parent dirs
  // if they are absent


  ExtendedStat parentstat, lfnstat;
  std::string parentpath;

  {
    // Security credentials are mandatory, and they have to carry the identity of the remote client
    SecurityCredentials cred;
    cred.clientName = (std::string) req.creds.clientName;
    cred.remoteAddress = req.creds.remoteAddress;
    cred.fqans = req.creds.groups;

    try {
      if(!addreplica) {
        stack->setSecurityCredentials(cred);
      }
    } catch (DmException &e) {
      std::ostringstream os;
      os << "Cannot set security credentials. dn: '" << req.creds.clientName << "' addr: '" <<
            req.creds.remoteAddress << "' - " << e.code() << "-" << e.what();

      Err(domelogname, os.str());
      return DomeReq::SendSimpleResp(request, http_status(e), os);
    }
  }

  if (!addreplica)
    try {


      mkdirminuspandcreate(stack->getCatalog(), lfn, parentpath, parentstat, lfnstat);




    } catch (DmException &e) {
      std::ostringstream os;
      os << "Cannot create logical directories for '" << lfn << "' : " << e.code() << "-" << e.what();

      Err(domelogname, os.str());
      return DomeReq::SendSimpleResp(request, http_status(e), os);
    }
  else
    try {
      lfnstat = stack->getCatalog()->extendedStat(lfn);
    } catch (DmException &e) {
      std::ostringstream os;
      os << "Cannot add replica to '" << lfn << "' : " << e.code() << "-" << e.what();

      Err(domelogname, os.str());
      return DomeReq::SendSimpleResp(request, http_status(e), os);
    }

  // Create the replica in the catalog
  dmlite::Replica r;
  r.fileid = lfnstat.stat.st_ino;
  r.replicaid = 0;
  r.nbaccesses = 0;
  r.atime = r.ptime = r.ltime = time(0);
  r.status = dmlite::Replica::kBeingPopulated;
  r.type = dmlite::Replica::kPermanent;
  r.rfn = selectedfss[fspos].server + ":" + pfn;
  r["pool"] = selectedfss[fspos].poolname;
  r["filesystem"] = selectedfss[fspos].fs;
  r.setname = token.s_token;
  r["accountedspacetokenname"] = token.u_token;
  try {
    stack->getCatalog()->addReplica(r);
  } catch (DmException &e) {
    std::ostringstream os;
    os << "Cannot create replica '" << r.rfn << "' for '" << lfn << "' : " << e.code() << "-" << e.what();
    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, http_status(e), os);
  }

  // Here we are assuming that some frontend will soon start to write a new replica
  //  with the name we chose here

  // Return the response

  // This function may have been invoked to know
  // details about the placement without telling the client
  if (dest && destrfn) {
    *dest = selectedfss[fspos];
    *destrfn = r.rfn;
    success = true;
    Log(Logger::Lvl4, domelogmask, domelogname, "No data to the client will be sent yet - supplying destination to caller. ('" <<
                                                *destrfn << "')");
    return 0;
  }

  Log(Logger::Lvl4, domelogmask, domelogname, "Sending response to client for '" << pfn << "'");

  boost::property_tree::ptree jresp;
  jresp.put("pool", selectedfss[fspos].poolname);
  jresp.put("host", selectedfss[fspos].server);
  jresp.put("filesystem", selectedfss[fspos].fs);
  jresp.put("pfn", pfn);

  int rc = 0;

  if (!dontsendok)
    return DomeReq::SendSimpleResp(request, 200, jresp);

  Log(Logger::Lvl3, domelogmask, domelogname, "Success");
  success = true;
  return rc;
};

int DomeCore::dome_putdone_disk(DomeReq &req, FCGX_Request &request) {


  // The command takes as input server and pfn, separated
  // in order to put some distance from rfio concepts, at least in the api
  std::string server = req.bodyfields.get<std::string>("server", "");
  std::string pfn = req.bodyfields.get<std::string>("pfn", "");
  std::string lfn = req.bodyfields.get<std::string>("lfn", "");

  size_t size = req.bodyfields.get<size_t>("size", 0);
  std::string chktype = req.bodyfields.get<std::string>("checksumtype", "");
  std::string chkval = req.bodyfields.get<std::string>("checksumvalue", "");

  Log(Logger::Lvl1, domelogmask, domelogname, " server: '" << server << "' pfn: '" << pfn << "' "
    " size: " << size << " cksumt: '" << chktype << "' cksumv: '" << chkval << "'" );

  // Check for the mandatory arguments
  if ( !pfn.length() ) {
    std::ostringstream os;
    os << "Invalid pfn: '" << pfn << "'";
    return DomeReq::SendSimpleResp(request, DOME_HTTP_BAD_REQUEST, os);
  }

  // Please note that the server field can be empty

  if ( size < 0 ) {
    std::ostringstream os;
    os << "Invalid size: " << size << " '" << pfn << "'";
    return DomeReq::SendSimpleResp(request, DOME_HTTP_BAD_REQUEST, os);
  }

  // Now the optional ones for basic sanity
  if ( !(chktype.length() > 0) != !(chkval.length() > 0) ) {
    std::ostringstream os;
    os << "Invalid checksum hint. type:'" << chktype << "' val: '" << chkval << "'";
    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, DOME_HTTP_BAD_REQUEST, os);
  }

  if (chktype.length() && !checksums::isChecksumFullName(chktype)) {
    std::ostringstream os;
    os << "Invalid checksum hint. type:'" << chktype << "' val: '" << chkval << "'";
    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, DOME_HTTP_BAD_REQUEST, os);

  }


  // We are in the disk server, hence we check only things that reside here
  // and then forward the request to the head
  // Head node stuff will be checked by the headnode

  // We check the stat information of the file.
  Log(Logger::Lvl2, domelogmask, domelogname, " Stat-ing pfn: '" << pfn << "' "
    " on disk.");

  struct stat st;

  if ( stat(pfn.c_str(), &st) ) {
    std::ostringstream os;
    char errbuf[1024];
    os << "Cannot stat pfn:'" << pfn << "' err: " << errno << ":" << strerror_r(errno, errbuf, 1023);
    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, DOME_HTTP_NOT_FOUND, os);
  }

  Log(Logger::Lvl2, domelogmask, domelogname, " pfn: '" << pfn << "' "
    " disksize: " << st.st_size);

  if (size == 0) size = st.st_size;

  if ( (off_t)size != st.st_size ) {
    std::ostringstream os;
    os << "Reported size (" << size << ") does not match with the size of the file (" << st.st_size << ")";
    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, DOME_HTTP_BAD_REQUEST, os);
  }

  // Now forward the request to the head node
  Log(Logger::Lvl1, domelogmask, domelogname, " Forwarding to headnode. server: '" << server << "' pfn: '" << pfn << "' "
    " size: " << size << " cksumt: '" << chktype << "' cksumv: '" << chkval << "'" );

  std::string domeurl = CFG->GetString("disk.headnode.domeurl", (char *)"(empty url)/");

  DomeTalker talker(*davixPool, req.creds, domeurl,
                    "POST", "dome_putdone");

  // Copy the same body fields as the original one, except for some fields,
  // where we write this machine's hostname (we are a disk server here) and the validated size
  if(server.empty()) {
    server = status.myhostname;
  }

  req.bodyfields.put("server", server);
  req.bodyfields.put("size", size);
  req.bodyfields.put("lfn", lfn);

  if(!talker.execute(req.bodyfields)) {
    Err(domelogname, talker.err());
    return DomeReq::SendSimpleResp(request, 500, talker.err());
  }

  return DomeReq::SendSimpleResp(request, DOME_HTTP_OK, talker.response());
}

int DomeCore::dome_putdone_head(DomeReq &req, FCGX_Request &request) {


  DmlitePoolHandler stack(status.dmpool);
  INode *inodeintf = dynamic_cast<INode *>(stack->getINode());
  if (!inodeintf) {
    Err( domelogname , " Cannot retrieve inode interface. Fatal.");
    return -1;
  }

  // The command takes as input server and pfn, separated
  // in order to put some distance from rfio concepts, at least in the api
  std::string server = req.bodyfields.get<std::string>("server", "");
  std::string pfn = req.bodyfields.get<std::string>("pfn", "");
  std::string lfn = req.bodyfields.get<std::string>("lfn", "");
  size_t size = req.bodyfields.get<size_t>("size", 0);
  std::string chktype = req.bodyfields.get<std::string>("checksumtype", "");
  std::string chkval = req.bodyfields.get<std::string>("checksumvalue", "");

  Log(Logger::Lvl1, domelogmask, domelogname, " server: '" << server << "' pfn: '" << pfn << "' "
    " size: " << size << " cksumt: '" << chktype << "' cksumv: '" << chkval << "'" );

  // Check for the mandatory arguments
  if ( !pfn.length() ) {
    std::ostringstream os;
    os << "Invalid pfn: '" << pfn << "'";
    return DomeReq::SendSimpleResp(request, 422, os);
  }
  if ( !server.length() ) {
    std::ostringstream os;
    os << "Invalid server: '" << server << "'";
    return DomeReq::SendSimpleResp(request, 422, os);
  }
  if ( size < 0 ) {
    std::ostringstream os;
    os << "Invalid size: " << size << " '" << pfn << "'";
    return DomeReq::SendSimpleResp(request, 422, os);
  }

  // Now the optional ones for basic sanity
  if ( !(chktype.length() > 0) != !(chkval.length() > 0) ) {
    std::ostringstream os;
    os << "Invalid checksum hint. type:'" << chktype << "' val: '" << chkval << "'";
    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, 422, os);
  }

  if (chktype.length() && !checksums::isChecksumFullName(chktype)) {
    std::ostringstream os;
    os << "Invalid checksum hint. type:'" << chktype << "' val: '" << chkval << "'";
    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, 422, os);

  }

  // Here unfortunately, for backward compatibility we are forced to
  // use the rfio syntax.
  std::string rfn = server + ":" + pfn;

  dmlite::Replica rep;
  try {
    rep = stack->getCatalog()->getReplicaByRFN(rfn);
  } catch (DmException e) {
    std::ostringstream os;
    os << "Cannot find replica '"<< rfn << "' : " << e.code() << "-" << e.what();

    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, 404, os);
  }

  if (rep.status != dmlite::Replica::kBeingPopulated) {

    std::ostringstream os;
    os << "Invalid status for replica '"<< rfn << "'";

    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, 422, os);

  }

  dmlite::ExtendedStat st;
  try {
    st = inodeintf->extendedStat(rep.fileid);
  } catch (DmException e) {
    std::ostringstream os;
    os << "Cannot fetch logical entry for replica '"<< rfn << "' : " << e.code() << "-" << e.what();

    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, 422, os);
  }

  // We are in the headnode getting a size of zero is fishy and has to be doublechecked, old style
  if (size == 0) {
    std::string domeurl = SSTR("https://" << server << "/domedisk");

    DomeTalker talker(*davixPool, req.creds, domeurl,
                      "GET", "dome_statpfn");

    if(!talker.execute("pfn", pfn)) {
      Err(domelogname, talker.err());
      return DomeReq::SendSimpleResp(request, 500, talker.err());
    }

    try {
      size = talker.jresp().get<size_t>("size");
    }
    catch(boost::property_tree::ptree_error &e) {
      std::string errmsg = SSTR("Received invalid json when talking to " << domeurl << ":" << e.what() << " '" << talker.response() << "'");
      Err("takeJSONbodyfields", errmsg);
      return DomeReq::SendSimpleResp(request, 500, errmsg);
    }

  } // if size == 0

  // -------------------------------------------------------
  // If a miracle took us here, the size has been confirmed
  Log(Logger::Lvl1, domelogmask, domelogname, " Final size:   " << size );














  // Update the replica values, including the checksum
  rep.ptime = rep.ltime = rep.atime = time(0);
  rep.status = dmlite::Replica::kAvailable;
  if(!chktype.empty()) {
    Log(Logger::Lvl4, domelogmask, domelogname, " setting checksum: " << chktype << "," << chkval);
    rep[chktype] = chkval;
  }

  try {
    stack->getCatalog()->updateReplica(rep);
  } catch (DmException e) {
    std::ostringstream os;
    os << "Cannot update replica '"<< rfn << "' : " << e.code() << "-" << e.what();

    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, 500, os);
  }

  // If the checksum of the main entry is different, just output a bad warning in the log
  std::string ck;
  if ( !st.getchecksum(chktype, ck) && (ck != chkval) ) {
    Err(domelogname, SSTR("Replica checksum mismatch rfn:'"<< rfn << "' : " << chkval << " fileid: " << rep.fileid << " : " << ck));
  }



  // Anyway propagate the checksum to the main stat
  inodeintf->setChecksum(st.stat.st_ino, chktype, chkval);
  inodeintf->setSize(st.stat.st_ino, size);

  // Now update the space counters for the parent directories!
  // Please note that this substitutes the IOPassthrough plugin in the disk's dmlite stack
  if (st.parent <= 0) {

    try {
      Log(Logger::Lvl4, domelogmask, domelogname, " Looking up parent of inode " << st.stat.st_ino << " " << " main entry for replica: '" << rfn << "'");
      st = inodeintf->extendedStat(st.stat.st_ino);
      Log(Logger::Lvl4, domelogmask, domelogname, " Ok. Parent of  inode " << st.stat.st_ino << " is " << st.parent);
    }
    catch (DmException& e) {
      Err( domelogname , " Cannot retrieve parent for inode:" << st.stat.st_ino << " " << " main entry for replica: '" << rfn << "'");
      return -1;
    }

  }


  // Add this filesize to the size of its parent dirs, only the first N levels
  {


    // Start transaction
    InodeTrans trans(inodeintf);

    off_t sz = size;


    ino_t hierarchy[128];
    size_t hierarchysz[128];
    unsigned int idx = 0;
    while (st.parent) {

      Log(Logger::Lvl4, domelogmask, domelogname, " Going to stat " << st.parent << " parent of " << st.stat.st_ino << " with idx " << idx);

      try {
        st = inodeintf->extendedStat(st.parent);
      }
      catch (DmException& e) {
        Err( domelogname , " Cannot stat inode " << st.parent << " parent of " << st.stat.st_ino);
        return -1;
      }

      hierarchy[idx] = st.stat.st_ino;
      hierarchysz[idx] = st.stat.st_size;

      Log(Logger::Lvl4, domelogmask, domelogname, " Size of inode " << st.stat.st_ino <<
      " is " << st.stat.st_size << " with idx " << idx);

      idx++;

      if (idx >= sizeof(hierarchy)) {
        Err( domelogname , " Too many parent directories for replica " << rfn);
        return -1;
      }
    }

    // Update the filesize in the first levels
    // Avoid the contention on /dpm/voname/home
    if (idx > 0) {
      Log(Logger::Lvl4, domelogmask, domelogname, " Going to set sizes. Max depth found: " << idx);
      for (int i = MAX(0, idx-3); i >= MAX(0, idx-1-CFG->GetLong("head.dirspacereportdepth", 6)); i--) {
        Log(Logger::Lvl4, domelogmask, domelogname, " Inide: " << hierarchy[i] << " Size: " << hierarchysz[i] << "-->" <<  hierarchysz[i] + sz);
        inodeintf->setSize(hierarchy[i], sz + hierarchysz[i]);
      }
    }
    else {
      Log(Logger::Lvl4, domelogmask, domelogname, " Cannot set any size. Max depth found: " << idx);
    }


    // Commit the local trans object
    // This also releases the connection back to the pool
    trans.Commit();
  }


  // For backward compatibility with the DPM daemon, we also update its
  // spacetoken counters, adjusting u_space
  {
    DomeQuotatoken token;

    if (rep.setname.size() > 0) {
      Log(Logger::Lvl4, domelogmask, domelogname, " Accounted space token: '" << rep.setname <<
        "' rfn: '" << rep.rfn << "'");

      DomeMySql sql;
      DomeMySqlTrans  t(&sql);

      // Occupy some space
      sql.addtoQuotatokenUspace(rep.setname, -size);
    }

  }


  int rc = DomeReq::SendSimpleResp(request, 200, SSTR("dome_putdone successful."));

  Log(Logger::Lvl3, domelogmask, domelogname, "Result: " << rc);
  return rc;
};

std::vector<std::string> list_folders(const std::string &folder) {
  std::vector<std::string> ret;

  DIR *d;
  struct dirent *dir;
  d = opendir(folder.c_str());
  if(!d) return ret;

  while((dir = readdir(d)) != NULL) {
    std::string name = dir->d_name;
    if(name != "." && name != ".." && dir->d_name && dir->d_type == DT_DIR) {
      ret.push_back(folder + "/" + name);
    }
  }

  closedir(d);

  std::sort(ret.begin(), ret.end(), std::less<std::string>());
  return ret;
}

int DomeCore::makespace(std::string fsplusvo, int size) {
  // retrieve the list of folders and iterate over them, starting from the oldest
  std::vector<std::string> folders = list_folders(fsplusvo);
  size_t folder = 0;
  size_t evictions = 0;
  int space_cleared = 0;

  std::string domeurl = CFG->GetString("disk.headnode.domeurl", (char *)"(empty url)/");

  while(size >= space_cleared && folder < folders.size()) {
    DIR *d = opendir(folders[folder].c_str());
    if(!d) break; // should not happen

    while(size >= space_cleared) {
      struct dirent *entry = readdir(d);
      if(!entry) {
        break;
      }

      if(entry->d_type == DT_REG) {
        std::string victim = SSTR(folder << "/" << entry->d_name);
        struct stat tmp;
        if(stat(victim.c_str(), &tmp) != 0) continue; // should not happen

        DomeTalker talker(*davixPool, NULL, domeurl,
                          "POST", "dome_delreplica");

        if(!talker.execute("pfn", victim, "server", status.myhostname)) {
          Err(domelogname, talker.err()); // dark data? skip file
          continue;
        }

        Log(Logger::Lvl1, domelogmask, domelogname, "Evicting replica '" << victim << "' of size " << tmp.st_size << "from volatile filesystem to make space");
        evictions++;
        space_cleared += tmp.st_size;
      }
    }
    closedir(d);
    folder++;
  }
  return space_cleared;
}

// semi-random. Depends in what order the filesystem returns the files, which
// is implementation-defined
std::pair<size_t, std::string> pick_a_file(const std::string &folder) {
  DIR *d = opendir(folder.c_str());

  while(true) {
    struct dirent *entry = readdir(d);
    if(!entry) {
      closedir(d);
      return std::make_pair(-1, "");
    }

    if(entry->d_type == DT_REG) {
      std::string filename = SSTR(folder << "/" << entry->d_name);
      struct stat tmp;
      if(stat(filename.c_str(), &tmp) != 0) continue; // should not happen

      closedir(d);
      return std::make_pair(tmp.st_size, filename);
    }
  }
}

int DomeCore::dome_makespace(DomeReq &req, FCGX_Request &request) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering");
  if(status.role == status.roleHead) {
    return DomeReq::SendSimpleResp(request, DOME_HTTP_BAD_REQUEST, SSTR("makespace only available on disknodes"));
  }

  std::string fs = req.bodyfields.get<std::string>("fs", "");
  std::string voname = req.bodyfields.get<std::string>("vo", "");
  int size = req.bodyfields.get<size_t>("size", 0);
  bool ensure_space = DomeUtils::str_to_bool(req.bodyfields.get<std::string>("ensure-space", "true"));

  if(fs.empty()) {
    return DomeReq::SendSimpleResp(request, DOME_HTTP_BAD_REQUEST, "fs cannot be empty.");
  }
  if(voname.empty()) {
    return DomeReq::SendSimpleResp(request, DOME_HTTP_BAD_REQUEST, "vo cannot be empty.");
  }
  if(size <= 0) {
    return DomeReq::SendSimpleResp(request, DOME_HTTP_BAD_REQUEST, "size is required and must be positive.");
  }

  // verify fs exists!
  {
  boost::unique_lock<boost::recursive_mutex> l(status);
  bool found = false;
  size_t i, selected_fs;
  for(i = 0; i < status.fslist.size(); i++) {
    if(status.fslist[i].fs == fs) {
      found = true;
      selected_fs = i;
      if(ensure_space) {
        size -= status.fslist[i].freespace;
      }
      break;
    }
  }
  if(!found) {
    return DomeReq::SendSimpleResp(request, DOME_HTTP_BAD_REQUEST, SSTR("Could not find filesystem '" << fs << "'"));
  }
  if(size <= 0) {
    return DomeReq::SendSimpleResp(request, DOME_HTTP_OK, SSTR("Selected fs " << status.fslist[selected_fs].server << ":" << status.fslist[selected_fs].fs << "' has enough space. (" << status.fslist[i].freespace << ")"));
  }
  }

  // retrieve the list of folders and iterate over them, starting from the oldest
  std::vector<std::string> folders = list_folders(fs + "/" + voname);
  size_t folder = 0;
  size_t evictions = 0;
  int space_cleared = 0;

  std::string domeurl = CFG->GetString("disk.headnode.domeurl", (char *)"(empty url)/");
  std::ostringstream response;

  while(size >= space_cleared && folder < folders.size()) {
    std::pair<size_t, std::string> victim = pick_a_file(folders[folder]);

    if(victim.second.empty()) {
      // rmdir is part of POSIX and only removes a directory if empty!
      // If for some crazy reason we end up here even though the directory
      // has files, the following will not have any effects.
      rmdir(folders[folder].c_str());
      folder++;
      continue;
    }

    space_cleared += victim.first;
    evictions++;
    Log(Logger::Lvl1, domelogmask, domelogname, "Evicting replica '" << victim.second << "' from volatile filesystem to make space");
    response << "Evicting replica '" << victim.second << "' of size '" << victim.first << "'" << "\r\n";

    DomeTalker talker(*davixPool, req.creds, domeurl,
                      "POST", "dome_delreplica");

    if(!talker.execute("pfn", victim.second, "server", status.myhostname)) {
      Err(domelogname, talker.err());
      return DomeReq::SendSimpleResp(request, 500, talker.err());
    }
  }

  response << "Cleared '" << space_cleared << "' bytes through the removal of " << evictions << " files\r\n";

  if(space_cleared < size) {
    response << "Error: could not clear up the requested amount of space. " << size << "\r\n";
    return DomeReq::SendSimpleResp(request, DOME_HTTP_UNPROCESSABLE, response.str());
  }

  return DomeReq::SendSimpleResp(request, DOME_HTTP_OK, response.str());
}

int DomeCore::dome_getspaceinfo(DomeReq &req, FCGX_Request &request) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering");

  boost::unique_lock<boost::recursive_mutex> l(status);

  boost::property_tree::ptree jresp;
  for (unsigned int i = 0; i < status.fslist.size(); i++) {
    std::string fsname, poolname;
    boost::property_tree::ptree top;

    fsname = "fsinfo^" + status.fslist[i].server + "^" + status.fslist[i].fs;

    // Add this server if not already there

    if (status.role == status.roleHead) { // Only headnodes report about pools
      jresp.put(boost::property_tree::ptree::path_type(fsname+"^poolname", '^'), status.fslist[i].poolname);
      jresp.put(boost::property_tree::ptree::path_type(fsname+"^fsstatus", '^'), status.fslist[i].status);
    }
    jresp.put(boost::property_tree::ptree::path_type(fsname+"^freespace", '^'), status.fslist[i].freespace);
    jresp.put(boost::property_tree::ptree::path_type(fsname+"^physicalsize", '^'), status.fslist[i].physicalsize);
    jresp.put(boost::property_tree::ptree::path_type(fsname+"^activitystatus", '^'), status.fslist[i].activitystatus);

    if (status.role == status.roleHead) { //Only headnodes report about pools
      poolname = "poolinfo^" + status.fslist[i].poolname;
      long long tot, free;
      int pool_st;
      status.getPoolSpaces(status.fslist[i].poolname, tot, free, pool_st);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^poolstatus", '^'), 0);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^freespace", '^'), free);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^physicalsize", '^'), tot);

      try {
        jresp.put(boost::property_tree::ptree::path_type(poolname+"^s_type", '^'), status.poolslist[status.fslist[i].poolname].stype);
        jresp.put(boost::property_tree::ptree::path_type(poolname+"^defsize", '^'), status.poolslist[status.fslist[i].poolname].defsize);
      } catch ( ... ) {};

      poolname = "poolinfo^" + status.fslist[i].poolname + "^fsinfo^" + status.fslist[i].server + "^" + status.fslist[i].fs;

      jresp.put(boost::property_tree::ptree::path_type(poolname+"^fsstatus", '^'), status.fslist[i].status);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^freespace", '^'), status.fslist[i].freespace);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^physicalsize", '^'), status.fslist[i].physicalsize);


    }
  }

  // For completeness, add also the pools that have no filesystems :-(
  for (std::map <std::string, DomePoolInfo>::iterator it = status.poolslist.begin();
       it != status.poolslist.end();
       it++) {
    std::string poolname = "poolinfo^" + it->second.poolname;
    jresp.put(boost::property_tree::ptree::path_type(poolname+"^s_type", '^'), it->second.stype);
    jresp.put(boost::property_tree::ptree::path_type(poolname+"^defsize", '^'), it->second.defsize);

  }

  return DomeReq::SendSimpleResp(request, 200, jresp);
};


int DomeCore::calculateChecksum(DomeReq &req, FCGX_Request &request, std::string lfn, Replica replica, std::string checksumtype, bool updateLfnChecksum) {
  // create queue entry
  GenPrioQueueItem::QStatus qstatus = GenPrioQueueItem::Waiting;
  std::string namekey = lfn + "[#]" + replica.rfn + "[#]" + checksumtype;
  std::vector<std::string> qualifiers;

  qualifiers.push_back(""); // the first qualifier is common for all items,
                            // so the global limit triggers

  qualifiers.push_back(replica.server); // server name as second qualifier, so
                                        // the per-node limit triggers

  // necessary information to keep when sending dochksum - order is important
  qualifiers.push_back(DomeUtils::bool_to_str(updateLfnChecksum));
  qualifiers.push_back(req.creds.clientName);
  qualifiers.push_back(req.creds.remoteAddress);

  status.checksumq->touchItemOrCreateNew(namekey, qstatus, 0, qualifiers);
  status.notifyQueues();

  boost::property_tree::ptree jresp;
  jresp.put("status", "enqueued");
  jresp.put("server", replica.server);
  jresp.put("pfn", DomeUtils::pfn_from_rfio_syntax(replica.rfn));
  jresp.put("queue-size", status.checksumq->nTotal());
  return DomeReq::SendSimpleResp(request, 202, jresp);
}

void DomeCore::touch_pull_queue(DomeReq &req, const std::string &lfn, const std::string &server, const std::string &fs,
                                  const std::string &rfn) {
  // create or update queue entry
  GenPrioQueueItem::QStatus qstatus = GenPrioQueueItem::Waiting;
  std::vector<std::string> qualifiers;

  qualifiers.push_back(""); // the first qualifier is common for all items,
                            // so the global limit triggers

  qualifiers.push_back(lfn); // lfn as second qualifier
  qualifiers.push_back(server);
  qualifiers.push_back(fs);

  // necessary information to keep - order is important

  qualifiers.push_back(rfn);
  qualifiers.push_back(req.creds.clientName);
  qualifiers.push_back(req.creds.remoteAddress);

  status.filepullq->touchItemOrCreateNew(lfn, qstatus, 0, qualifiers);
}

int DomeCore::enqfilepull(DomeReq &req, FCGX_Request &request, std::string lfn) {

  // This simple implementation is like a put
  DomeFsInfo destfs;
  std::string destrfn;

  bool success;
  dome_put(req, request, success, &destfs, &destrfn, true);
  if (!success)
    return 1; // means that a response has already been sent in the context of dome_put, btw it can only be an error

  touch_pull_queue(req, lfn, destfs.server, destfs.fs, destrfn);
  status.notifyQueues();

  // TODO: Here we have to trigger the file pull in the disk server,
  // by sending a dome_pull request

  return DomeReq::SendSimpleResp(request, 202, SSTR("Enqueued file pull request " << destfs.server
                                                     << ", path " << lfn
                                                     << ", check back later.\r\nTotal pulls in queue right now: "
                                                     << status.filepullq->nTotal()));
}

static Replica pickReplica(std::string lfn, std::string rfn, DmlitePoolHandler &stack) {
  std::vector<Replica> replicas = stack->getCatalog()->getReplicas(lfn);
  if(replicas.size() == 0) {
    throw DmException(DMLITE_CFGERR(ENOENT), "The provided LFN does not have any replicas");
  }

  if(rfn != "") {
    for(std::vector<Replica>::iterator it = replicas.begin(); it != replicas.end(); it++) {
      if(it->rfn == rfn) {
        return *it;
      }
    }
    throw DmException(DMLITE_CFGERR(ENOENT), "The provided PFN does not correspond to any of LFN's replicas");
  }

  // no explicit pfn? pick a random replica
  int index = rand() % replicas.size();
  return replicas[index];
}

int DomeCore::dome_info(DomeReq &req, FCGX_Request &request, int myidx, bool authorized) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering");

  std::ostringstream response;
  response << "dome [" << DMLITE_MAJOR << "." << DMLITE_MINOR << "." << DMLITE_PATCH << "] running as ";
  if(status.role == status.roleDisk) response << "disk";
  else response << "head";

  response << "\r\nServer PID: " << getpid() << " - Thread Index: " << myidx << " \r\n";
  response << "Your DN: " << req.clientdn << "\r\n\r\n";

  if(authorized) {
    response << "ACCESS TO DOME GRANTED.\r\n"; // magic string, don't change. The tests look for this string
    for (char **envp = request.envp ; *envp; ++envp) {
      response << *envp << "\r\n";
    }
  }
  else {
    response << "ACCESS TO DOME DENIED.\r\n"; // magic string, don't change
    response << "Your client certificate is not authorized to directly access dome. Sorry :-)\r\n";
  }

  return DomeReq::SendSimpleResp(request, 200, response);
}

int DomeCore::dome_chksum(DomeReq &req, FCGX_Request &request) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering");
  if(status.role == status.roleDisk) {
    return DomeReq::SendSimpleResp(request, 500, "chksum only available on head nodes");
  }

  try {
    DmlitePoolHandler stack(status.dmpool);

    std::string chksumtype = req.bodyfields.get<std::string>("checksum-type", "");
    chksumtype = DomeUtils::remove_prefix_if_exists(chksumtype, "checksum.");

    std::string fullchecksum = "checksum." + chksumtype;
    std::string pfn = req.bodyfields.get<std::string>("pfn", "");
    std::string lfn = req.bodyfields.get<std::string>("lfn", "");
    bool forcerecalc = DomeUtils::str_to_bool(req.bodyfields.get<std::string>("force-recalc", "false"));
    bool updateLfnChecksum = (pfn == "");

    if(chksumtype == "") {
      return DomeReq::SendSimpleResp(request, 422, "checksum-type cannot be empty.");
    }

    if(chksumtype != "md5" && chksumtype != "crc32" && chksumtype != "adler32") {
      return DomeReq::SendSimpleResp(request, 422, SSTR("unknown checksum type " << chksumtype));
    }

    if(forcerecalc) {
      Replica replica = pickReplica(lfn, pfn, stack);
      return calculateChecksum(req, request, lfn, replica, chksumtype, updateLfnChecksum);
    }

    // Not forced to do a recalc - maybe I can find the checksums in the db
    std::string lfnchecksum;
    std::string pfnchecksum;
    Replica replica;

    // retrieve lfn checksum
    ExtendedStat xstat;
    {
      DomeMySql sql;
      DmStatus st = sql.getStatbyLFN(xstat, lfn);
      if (!st.ok())
        return DomeReq::SendSimpleResp(request, 404, SSTR("Cannot stat lfn: '" << lfn << "'"));
    }

    if(xstat.hasField(fullchecksum)) {
      lfnchecksum = xstat.getString(fullchecksum);
      Log(Logger::Lvl3, domelogmask, domelogname, "Found lfn checksum in the db: " << lfnchecksum);
    }
    else {
      Log(Logger::Lvl3, domelogmask, domelogname, "Lfn checksum not in the db");
    }

    // retrieve pfn checksum
    if(pfn != "") {
      replica = pickReplica(lfn, pfn, stack);
      if(replica.hasField(fullchecksum)) {
        pfnchecksum = replica.getString(fullchecksum);
        Log(Logger::Lvl3, domelogmask, domelogname, "Found pfn checksum in the db: " << pfnchecksum);
      }
    }

    // can I send a response right now? Of course, sir !
    if(lfnchecksum != "" && (pfn == "" || pfnchecksum != "")) {
      boost::property_tree::ptree jresp;
      jresp.put("status", "found");
      jresp.put("checksum", lfnchecksum);
      if(pfn != "") {
        jresp.put("pfn-checksum", pfnchecksum);
      }
      return DomeReq::SendSimpleResp(request, 200, jresp);
    }

    // something is missing, need to calculate
    if(pfn == "") {
      replica = pickReplica(lfn, pfn, stack);
    }

    return calculateChecksum(req, request, lfn, replica, chksumtype, updateLfnChecksum);
  }
  catch(dmlite::DmException& e) {
    std::ostringstream os("An error has occured.\r\n");
    os << "Dmlite exception: " << e.what();
    return DomeReq::SendSimpleResp(request, 404, os);
  }

  Log(Logger::Lvl1, domelogmask, domelogname, "Error - execution should never reach this point");
  return DomeReq::SendSimpleResp(request, 500, "Something went wrong, execution should never reach this point.");
}

int DomeCore::dome_chksumstatus(DomeReq &req, FCGX_Request &request) {
  if(status.role == status.roleDisk) {
    return DomeReq::SendSimpleResp(request, 500, "chksumstatus only available on head nodes");
  }

  try {
    DmlitePoolHandler stack(status.dmpool);

    std::string chksumtype = req.bodyfields.get<std::string>("checksum-type", "");
    chksumtype = DomeUtils::remove_prefix_if_exists(chksumtype, "checksum.");

    std::string fullchecksum = "checksum." + chksumtype;
    std::string pfn = req.bodyfields.get<std::string>("pfn", "");
    std::string lfn = req.bodyfields.get<std::string>("lfn", "");
    std::string str_status = req.bodyfields.get<std::string>("status", "");
    std::string reason = req.bodyfields.get<std::string>("reason", "");
    std::string checksum = req.bodyfields.get<std::string>("checksum", "");
    bool updateLfnChecksum = DomeUtils::str_to_bool(req.bodyfields.get<std::string>("update-lfn-checksum", "false"));

    if(chksumtype == "") {
      return DomeReq::SendSimpleResp(request, 422, "checksum-type cannot be empty.");
    }
    if(pfn == "") {
      return DomeReq::SendSimpleResp(request, 422, "pfn cannot be empty.");
    }

    GenPrioQueueItem::QStatus qstatus;

    if(str_status == "pending") {
      qstatus = GenPrioQueueItem::Running;
    }
    else if(str_status == "done" || str_status == "aborted") {
      qstatus = GenPrioQueueItem::Finished;
    }
    else {
      return DomeReq::SendSimpleResp(request, 422, "The status provided is not recognized.");
    }

    // modify the queue as needed
    std::string namekey = lfn + "[#]" + pfn + "[#]" + chksumtype;
    std::vector<std::string> qualifiers;

    Url u(pfn);
    std::string server = u.domain;

    qualifiers.push_back("");
    qualifiers.push_back(server);
    qualifiers.push_back(DomeUtils::bool_to_str(updateLfnChecksum));
    status.checksumq->touchItemOrCreateNew(namekey, qstatus, 0, qualifiers);

    if(qstatus != GenPrioQueueItem::Running) {
      status.notifyQueues();
    }

    if(str_status == "aborted") {
      Log(Logger::Lvl1, domelogmask, domelogname, "Checksum calculation failed. LFN: " << lfn
      << "PFN: " << pfn << ". Reason: " << reason);
      return DomeReq::SendSimpleResp(request, 200, "");
    }

    if(str_status == "pending") {
      return DomeReq::SendSimpleResp(request, 200, "");
    }

    // status is done, checksum should not be empty
    if(checksum == "") {
      Log(Logger::Lvl2, domelogmask, domelogname, "Received 'done' checksum status without a checksum");
      return DomeReq::SendSimpleResp(request, 400, "checksum cannot be empty when status is done.");
    }

    // replace pfn checksum
    Replica replica = pickReplica(lfn, pfn, stack);
    replica[fullchecksum] = checksum;
    stack->getCatalog()->updateReplica(replica);

    // replace lfn checksum?
    if(updateLfnChecksum) {
      stack->getCatalog()->setChecksum(lfn, fullchecksum, checksum);
    }
    // still update if it's empty, though
    else {
      // retrieve lfn checksum
      ExtendedStat xstat;
      {
        DomeMySql sql;
        DmStatus st = sql.getStatbyLFN(xstat, lfn);
        if (!st.ok())
          return DomeReq::SendSimpleResp(request, 404, SSTR("Cannot stat lfn: '" << lfn << "'"));
      }

      if(!xstat.hasField(fullchecksum)) {
        stack->getCatalog()->setChecksum(lfn, fullchecksum, checksum);
      }
    }

    return DomeReq::SendSimpleResp(request, 200, "");
  }
  catch(dmlite::DmException& e) {
    std::ostringstream os("An error has occured.\r\n");
    os << "Dmlite exception: " << e.what();
    return DomeReq::SendSimpleResp(request, 404, os);
  }

  Log(Logger::Lvl1, domelogmask, domelogname, "Error - execution should never reach this point");
  return DomeReq::SendSimpleResp(request, 500, "Something went wrong, execution should never reach this point.");
}

int DomeCore::dome_dochksum(DomeReq &req, FCGX_Request &request) {
  if(status.role == status.roleHead) {
    return DomeReq::SendSimpleResp(request, 500, "dochksum only available on disk nodes");
  }

  try {
    std::string chksumtype = req.bodyfields.get<std::string>("checksum-type", "");
    std::string pfn = req.bodyfields.get<std::string>("pfn", "");
    std::string lfn = req.bodyfields.get<std::string>("lfn", "");
    bool updateLfnChecksum = DomeUtils::str_to_bool(req.bodyfields.get<std::string>("update-lfn-checksum", "false"));

    if(chksumtype == "") {
      return DomeReq::SendSimpleResp(request, 422, "checksum-type cannot be empty.");
    }
    if(pfn == "") {
      return DomeReq::SendSimpleResp(request, 422, "pfn cannot be empty.");
    }
    if(lfn == "") {
      return DomeReq::SendSimpleResp(request, 422, "lfn cannot be empty.");
    }

    PendingChecksum pending(lfn, status.myhostname, pfn, req.creds, chksumtype, updateLfnChecksum);

    std::vector<std::string> params;
    params.push_back("/usr/bin/dome-checksum");
    params.push_back(chksumtype);
    params.push_back(pfn);
    int id = this->submitCmd(params);

    if(id < 0) {
      return DomeReq::SendSimpleResp(request, 500, SSTR("An error occured - unable to initiate checksum calculation"));
    }

    {
    boost::lock_guard<boost::recursive_mutex> l(mtx);
    diskPendingChecksums[id] = pending;
    }
    return DomeReq::SendSimpleResp(request, 202, SSTR("Initiated checksum calculation on " << pfn << ", task executor ID: " << id));
  }
  catch(dmlite::DmException& e) {
    std::ostringstream os("An error has occured.\r\n");
    os << "Dmlite exception: " << e.what();
    return DomeReq::SendSimpleResp(request, 404, os);
  }
  return DomeReq::SendSimpleResp(request, 500, SSTR("Not implemented, dude."));
};

static std::string extract_checksum(std::string stdout, std::string &err) {
  // were there any errors?
  std::string magic = ">>>>> HASH ";
  size_t pos = stdout.find(magic);

  if(pos == std::string::npos) {
    err = "Could not find magic string, unable to extract checksum. ";
    return "";
  }

  size_t pos2 = stdout.find("\n", pos);
  if(pos2 == std::string::npos) {
    err = "Could not find newline after magic string, unable to extract checksum. ";
    return "";
  }

  return stdout.substr(pos+magic.size(), pos2-pos-magic.size());
}


static int extract_stat(std::string stdout, std::string &err, struct dmlite::ExtendedStat &st) {
  // were there any errors?
  std::string magic = ">>>>> STAT ";
  size_t pos = stdout.find(magic);

  if(pos == std::string::npos) {
    err = "Could not find magic string, unable to extract stat information. ";
    return -1;
  }

  size_t pos2 = stdout.find("\n", pos);
  if(pos2 == std::string::npos) {
    err = "Could not find newline after magic string, unable to extract stat information. ";
    return -1;
  }

  std::string s = stdout.substr(pos+magic.size(), pos2-pos-magic.size());
  return sscanf(s.c_str(), "%ld %d", &st.stat.st_size, &st.stat.st_mode);
}





void DomeCore::sendFilepullStatus(const PendingPull &pending, const DomeTask &task, bool completed) {


  std::string checksum, extract_error;
  bool failed = (task.resultcode != 0);

  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. Completed: " << completed << " rc: " << task.resultcode);

  if(completed) {
    checksum = extract_checksum(task.stdout, extract_error);
    if( ! extract_error.empty()) {
      Log(Logger::Lvl4, domelogmask, domelogname, "File pull did not provide any checksum. err: " << extract_error << task.stdout);
    }
    Log(Logger::Lvl4, domelogmask, domelogname, "File pull checksum: " << checksum);
  }

  std::string domeurl = CFG->GetString("disk.headnode.domeurl", (char *)"(empty url)/");
  Log(Logger::Lvl4, domelogmask, domelogname, domeurl);

  DomeTalker talker(*davixPool, pending.creds, domeurl,
                    "POST", "dome_pullstatus");

  // set chksumstatus params
  boost::property_tree::ptree jresp;

  jresp.put("lfn", pending.lfn);
  jresp.put("pfn", pending.pfn);
  jresp.put("server", status.myhostname);
  Log(Logger::Lvl4, domelogmask, domelogname, "pfn: " << pending.pfn);

  jresp.put("checksum-type", pending.chksumtype);

  if(completed) {
    if(failed) {
      jresp.put("status", "aborted");
      jresp.put("reason", SSTR(extract_error << task.stdout));
    }
    else {
      jresp.put("status", "done");
      jresp.put("checksum", checksum);

      // Let's stat the real file on disk, we are in a disk node
      struct stat st;

      if ( stat(pending.pfn.c_str(), &st) ) {
        std::ostringstream os;
        char errbuf[1024];
        os << "Cannot stat pfn:'" << pending.pfn << "' err: " << errno << ":" << strerror_r(errno, errbuf, 1023);
        Err(domelogname, os.str());

        // A successful execution and no file should be aborted!
        jresp.put("status", "aborted");
        jresp.put("reason", SSTR("disk node could not stat pfn: '" << pending.pfn << "' - " << os ));

      }
      else {
        // If stat was successful then we can get the final filesize
        Log(Logger::Lvl1, domelogmask, domelogname, "pfn: " << pending.pfn << " has size: " << st.st_size);
        jresp.put("filesize", st.st_size);
      }

    }
  }
  else {
    jresp.put("status", "pending");
  }

  if(!talker.execute(jresp)) {
    Err(domelogname, talker.err());
  }

}










void DomeCore::sendChecksumStatus(const PendingChecksum &pending, const DomeTask &task, bool completed) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. Completed: " << completed);

  std::string checksum, extract_error;
  bool failed = false;

  if(completed) {
    checksum = extract_checksum(task.stdout, extract_error);
    if( ! extract_error.empty()) {
      Err(domelogname, extract_error << task.stdout);
      failed = true;
    }
  }

  std::string domeurl = CFG->GetString("disk.headnode.domeurl", (char *)"(empty url)/");
  Log(Logger::Lvl4, domelogmask, domelogname, domeurl);
  std::string rfn = pending.server + ":" + pending.pfn;

  DomeTalker talker(*davixPool, pending.creds, domeurl,
                    "POST", "dome_chksumstatus");

  // set chksumstatus params
  boost::property_tree::ptree jresp;
  jresp.put("lfn", pending.lfn);
  jresp.put("pfn", rfn);
  Log(Logger::Lvl4, domelogmask, domelogname, "rfn: " << rfn);

  jresp.put("checksum-type", pending.chksumtype);

  if(completed) {
    if(failed) {
      jresp.put("status", "aborted");
      jresp.put("reason", SSTR(extract_error << task.stdout));
    }
    else {
      jresp.put("status", "done");
      jresp.put("checksum", checksum);
      jresp.put("update-lfn-checksum", DomeUtils::bool_to_str(pending.updateLfnChecksum));
    }
  }
  else {
    jresp.put("status", "pending");
  }

  if(!talker.execute(jresp)) {
    Err(domelogname, talker.err());
  }
}

int DomeCore::dome_statpool(DomeReq &req, FCGX_Request &request) {

  int rc = 0;
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering");

  if (status.role == status.roleDisk) { // Only headnodes report about pools
    std::ostringstream os;
    os << "I am a disk node and don't know what a pool is. Only head nodes know pools.";
    return DomeReq::SendSimpleResp(request, 422, os);
  }

  std::string pn = req.bodyfields.get("poolname", "");
  if ( !pn.size() ) {
    std::ostringstream os;
    os << "Pool '" << pn << "' not found.";
    return DomeReq::SendSimpleResp(request, 404, os);
  }




  long long tot, free;
  int poolst;
  status.getPoolSpaces(pn, tot, free, poolst);

  boost::property_tree::ptree jresp;
  for (unsigned int i = 0; i < status.fslist.size(); i++)
    if (status.fslist[i].poolname == pn) {
      std::string fsname, poolname;
      boost::property_tree::ptree top;



      poolname = "poolinfo^" + status.fslist[i].poolname;

      jresp.put(boost::property_tree::ptree::path_type(poolname+"^poolstatus", '^'), poolst);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^freespace", '^'), free);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^physicalsize", '^'), tot);

      poolname = "poolinfo^" + status.fslist[i].poolname + "^fsinfo^" + status.fslist[i].server + "^" + status.fslist[i].fs;

      jresp.put(boost::property_tree::ptree::path_type(poolname+"^fsstatus", '^'), status.fslist[i].status);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^freespace", '^'), status.fslist[i].freespace);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^physicalsize", '^'), status.fslist[i].physicalsize);

    }

  // maybe the pool contains no filesystems..
  for (std::map <std::string, DomePoolInfo>::iterator it = status.poolslist.begin();
       it != status.poolslist.end();
       it++) {
    if (it->second.poolname == pn) {
      std::string poolname = "poolinfo^" + it->second.poolname;
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^s_type", '^'), it->second.stype);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^defsize", '^'), it->second.defsize);
    }
  }

  rc = DomeReq::SendSimpleResp(request, 200, jresp);
  Log(Logger::Lvl3, domelogmask, domelogname, "Result: " << rc);
  return rc;
};

int DomeCore::dome_getdirspaces(DomeReq &req, FCGX_Request &request) {

  // Crawl upwards the directory hierarchy of the given path
  // stopping when a matching one is found
  // The quota tokens indicate the pools that host the files written into
  // this directory subtree

  Log(Logger::Lvl4, domelogmask, domelogname, "Entering");

  std::string absPath =  req.bodyfields.get<std::string>("path", "");
  if ( !absPath.size() ) {
    std::ostringstream os;
    os << "Path '" << absPath << "' is empty.";
    return DomeReq::SendSimpleResp(request, 422, os);
  }

  // Make sure it's an absolute lfn path
  if (absPath[0] != '/')  {
    std::ostringstream os;
    os << "Path '" << absPath << "' is not an absolute path.";
    return DomeReq::SendSimpleResp(request, 422, os);
  }

  // Remove any trailing slash
  while (absPath[ absPath.size()-1 ] == '/') {
    absPath.erase(absPath.size() - 1);
  }

  Log(Logger::Lvl4, domelogmask, domelogname, "Getting spaces for path: '" << absPath << "'");
  long long totspace = 0LL;
  long long usedspace = 0LL;
  long long quotausedspace = 0LL;
  long long poolfree = 0LL;
  std::string tkname = "<unknown>";
  std::string poolname = "<unknown>";
  // Crawl
  {
    boost::unique_lock<boost::recursive_mutex> l(status);
    while (absPath.length() > 0) {

      Log(Logger::Lvl4, domelogmask, domelogname, "Processing: '" << absPath << "'");
      // Check if any matching quotatoken exists
      std::pair <std::multimap<std::string, DomeQuotatoken>::iterator, std::multimap<std::string, DomeQuotatoken>::iterator> myintv;
      myintv = status.quotas.equal_range(absPath);

      if (myintv.first != myintv.second) {
        for (std::multimap<std::string, DomeQuotatoken>::iterator it = myintv.first; it != myintv.second; ++it) {
          totspace += it->second.t_space;

          // Now find the free space in the mentioned pool
          long long ptot, pfree;
          int poolst;
          status.getPoolSpaces(it->second.poolname, ptot, pfree, poolst);
          poolfree += pfree;

          Log(Logger::Lvl1, domelogmask, domelogname, "Quotatoken '" << it->second.u_token << "' of pool: '" <<
          it->second.poolname << "' matches path '" << absPath << "' totspace: " << totspace);

          tkname = it->second.u_token;
          poolname = it->second.poolname;
          quotausedspace = status.getQuotatokenUsedSpace(it->second);
          usedspace = status.getDirUsedSpace(absPath);
        }
        break;
      }

      // No match found, look upwards by trimming the last token from absPath
      size_t pos = absPath.rfind("/");
      absPath.erase(pos);
    }
  }
  // Prepare the response
  // NOTE:
  //  A pool may be assigned to many dir subtrees at the same time, hence
  //   the best value that we can publish for the pool is how much free space it has
  //  The quotatotspace is the sum of all the quotatokens assigned to this subtree
  //   In limit cases the tot quota can be less than the used space, hence it's better
  //   to publish the tot space rather than the free space in the quota.
  //   One of these cases could be when the sysadmin (with punitive mood) manually assigns
  //   a quota that is lower than the occupied space. This is a legitimate attempt
  //   to prevent clients from writing there until the space decreases...
  boost::property_tree::ptree jresp;
  jresp.put("quotatotspace", totspace);
  long long sp = (totspace - usedspace);
  jresp.put("quotafreespace", (sp < 0 ? 0 : sp));
  jresp.put("quotausedspace", quotausedspace);
  jresp.put("poolfreespace", poolfree);
  jresp.put("dirusedspace", usedspace);
  jresp.put("quotatoken", tkname);
  jresp.put("poolname", poolname);

  int rc = DomeReq::SendSimpleResp(request, 200, jresp);
  Log(Logger::Lvl3, domelogmask, domelogname, "Result: " << rc);
  return rc;

}


int DomeCore::dome_get(DomeReq &req, FCGX_Request &request)  {
  // Currently just returns a list of all replicas

  Log(Logger::Lvl4, domelogmask, domelogname, "Entering");

  DomeFsInfo fs;
  std::string lfn = req.bodyfields.get<std::string>("lfn", "");

  bool canpull = status.LfnMatchesAnyCanPullFS(lfn, fs);
  size_t pending_index = -1;

  DmlitePoolHandler stack(status.dmpool);
  try {
    std::vector<Replica> replicas = stack->getCatalog()->getReplicas(lfn);
    using boost::property_tree::ptree;
    ptree jresp;
    bool found = false;
    bool foundpending = false;

    for(size_t i = 0; i < replicas.size(); i++) {
      // give only path as pfn
      std::string rfn = replicas[i].rfn;
      std::string pfn;
      size_t pos = rfn.find(":");
      if (pos == std::string::npos) pfn = rfn;
      else
        pfn = rfn.substr(rfn.find(":")+1, rfn.size());

      // Check if the replica makes sense and whether its filesystem is enabled
      DomeFsInfo fsinfo;
      if (!status.PfnMatchesAnyFS(replicas[i].server, pfn, fsinfo)) {
        Err(domelogname, SSTR("Replica '" << rfn << "' in server '" << replicas[i].server << "' cannot be matched to any working filesystem. A configuration check is needed."));
        continue;
      }
      if (!fsinfo.isGoodForRead()) continue;

      if (replicas[i].status == Replica::kBeingPopulated) {
        foundpending = true;
        pending_index = i;
        continue;
      }

      found = true;
      jresp.put(ptree::path_type(SSTR(i << "^server"), '^'), replicas[i].server);
      jresp.put(ptree::path_type(SSTR(i << "^pfn"), '^'), pfn);
      jresp.put(ptree::path_type(SSTR(i << "^filesystem"), '^'), replicas[i].getString("filesystem"));
    }



    if (found)
      return DomeReq::SendSimpleResp(request, 200, jresp);

    if(foundpending && canpull) {
      std::string fs = replicas[pending_index].getString("filesystem");
      touch_pull_queue(req, lfn, replicas[pending_index].server, fs, replicas[pending_index].rfn);
      return DomeReq::SendSimpleResp(request, 202, SSTR("Refreshed file pull request for " << replicas[pending_index].server
                                                     << ", path " << lfn
                                                     << ", check back later.\r\nTotal pulls in queue right now: "
                                                     << status.filepullq->nTotal()));
    }

    if (foundpending)
      return DomeReq::SendSimpleResp(request, 500, "Only pending replicas are available.");
  }
  catch (dmlite::DmException e) {

    // The lfn does not seemm to exist ? We may have to pull the file from elsewhere
    if (e.code() == ENOENT) {
      Log(Logger::Lvl1, domelogmask, domelogname, "Lfn not found: '" << lfn << "'");

    }
    else
      return DomeReq::SendSimpleResp(request, 500, SSTR("Unable to find replicas for '" << lfn << "'"));
  }

  // Here we have to trigger the file pull and tell to the client to come back later
  if (canpull) {
    Log(Logger::Lvl1, domelogmask, domelogname, "Volatile filesystem detected. Seems we can try pulling the file: '" << lfn << "'");
    return enqfilepull(req, request, lfn);
  }

  return DomeReq::SendSimpleResp(request, 404, SSTR("No available replicas for '" << lfn << "'"));

}

int DomeCore::dome_pullstatus(DomeReq &req, FCGX_Request &request)  {
  if(status.role == status.roleDisk) {
    return DomeReq::SendSimpleResp(request, 500, "pullstatus only available on head nodes");
  }

  Log(Logger::Lvl4, domelogmask, domelogname, "Entering");

  try {
    DmlitePoolHandler stack(status.dmpool);
    INode *inodeintf = dynamic_cast<INode *>(stack->getINode());
    if (!inodeintf) {
      Err( domelogname , " Cannot retrieve inode interface. Fatal.");
      return -1;
    }

    std::string chksumtype = req.bodyfields.get<std::string>("checksum-type", "");
    std::string fullchecksum = "checksum." + chksumtype;
    std::string pfn = req.bodyfields.get<std::string>("pfn", "");
    std::string lfn = req.bodyfields.get<std::string>("lfn", "");
    std::string server = req.bodyfields.get<std::string>("server", "");
    std::string str_status = req.bodyfields.get<std::string>("status", "");
    std::string reason = req.bodyfields.get<std::string>("reason", "");
    std::string checksum = req.bodyfields.get<std::string>("checksum", "");
    size_t size = req.bodyfields.get("filesize", 0L);

    Log(Logger::Lvl1, domelogmask, domelogname, "lfn: '" << lfn << "' server: '" << server << "' pfn: '" << pfn <<
      "' pullstatus: '" << str_status << "' cktype: '" << checksum << "' ck: '" << checksum << "' reason: '" << reason << "'");

    if(pfn == "") {
      return DomeReq::SendSimpleResp(request, 422, "pfn cannot be empty.");
    }
    if(lfn == "") {
      return DomeReq::SendSimpleResp(request, 422, "lfn cannot be empty.");
    }

    GenPrioQueueItem::QStatus qstatus;

    if(str_status == "pending") {
      qstatus = GenPrioQueueItem::Running;
    }
    else if(str_status == "done" || str_status == "aborted") {
      qstatus = GenPrioQueueItem::Finished;
    }
    else {
      return DomeReq::SendSimpleResp(request, 422, "The status provided is not recognized.");
    }

    // modify the queue as needed
    std::string namekey = lfn;
    std::vector<std::string> qualifiers;

    qualifiers.push_back("");
    qualifiers.push_back(server);
    status.filepullq->touchItemOrCreateNew(namekey, qstatus, 0, qualifiers);
    if(qstatus != GenPrioQueueItem::Running) {
      status.notifyQueues();
    }

    if(str_status == "aborted") {
      Log(Logger::Lvl1, domelogmask, domelogname, "File pull failed. LFN: " << lfn
      << "PFN: " << pfn << ". Reason: " << reason);
      return DomeReq::SendSimpleResp(request, 200, "");
    }

    if(str_status == "pending") {
      Log(Logger::Lvl2, domelogmask, domelogname, "File pull pending... LFN: " << lfn
      << "PFN: " << pfn << ". Reason: " << reason);
      return DomeReq::SendSimpleResp(request, 200, "");
    }

    // status is done, checksum can be empty
    Log(Logger::Lvl2, domelogmask, domelogname, "File pull finished. LFN: " << lfn
        << "PFN: " << pfn << ". Reason: " << reason);

    // In practice it's like a putdone request, unfortunately we have to
    // apparently duplicate some code


    // Here unfortunately, for backward compatibility we are forced to
    // use the rfio syntax.
    std::string rfn = server + ":" + pfn;


    dmlite::Replica rep;
    try {
      rep = stack->getCatalog()->getReplicaByRFN(rfn);
    } catch (DmException e) {
      std::ostringstream os;
      os << "Cannot find replica '"<< rfn << "' : " << e.code() << "-" << e.what();

      Err(domelogname, os.str());
      return DomeReq::SendSimpleResp(request, 404, os);
    }

    if (rep.status != dmlite::Replica::kBeingPopulated) {

      std::ostringstream os;
      os << "Invalid status for replica '"<< rfn << "'";

      Err(domelogname, os.str());
      return DomeReq::SendSimpleResp(request, 422, os);

    }

    dmlite::ExtendedStat st;
    try {
      st = inodeintf->extendedStat(rep.fileid);
    } catch (DmException e) {
      std::ostringstream os;
      os << "Cannot fetch logical entry for replica '"<< rfn << "' : " << e.code() << "-" << e.what();

      Err(domelogname, os.str());
      return DomeReq::SendSimpleResp(request, 422, os);
    }





    // -------------------------------------------------------
    // If a miracle took us here, the size has been confirmed
    Log(Logger::Lvl1, domelogmask, domelogname, " Final size:   " << size );


    try {
      stack->getCatalog()->setSize(lfn, size);
    } catch (DmException e) {
      std::ostringstream os;
      os << "Cannot update replica '"<< rfn << "' : " << e.code() << "-" << e.what();

      Err(domelogname, os.str());
      return DomeReq::SendSimpleResp(request, 500, os);
    }



    // Update the replica values, including the checksum, if present
    rep.ptime = rep.ltime = rep.atime = time(0);
    rep.status = dmlite::Replica::kAvailable;
    if (checksum.size() && chksumtype.size())
      rep[fullchecksum] = checksum;

    try {
      stack->getCatalog()->updateReplica(rep);
    } catch (DmException e) {
      std::ostringstream os;
      os << "Cannot update replica '"<< rfn << "' : " << e.code() << "-" << e.what();

      Err(domelogname, os.str());
      return DomeReq::SendSimpleResp(request, 500, os);
    }

    // If the checksum of the main entry is different, just output a bad warning in the log
    std::string ck;
    if ( !st.getchecksum(fullchecksum, ck) && (ck != checksum) ) {
      Err(domelogname, SSTR("Replica checksum mismatch rfn:'"<< rfn << "' : " << checksum << " fileid: " << rep.fileid << " : " << ck));
    }


    return DomeReq::SendSimpleResp(request, 200, "");
  }
  catch(dmlite::DmException& e) {
    std::ostringstream os("An error has occured.\r\n");
    os << "Dmlite exception: " << e.what();
    return DomeReq::SendSimpleResp(request, 404, os);
  }

  Log(Logger::Lvl1, domelogmask, domelogname, "Error - execution should never reach this point");
  return DomeReq::SendSimpleResp(request, 500, "Something went wrong, execution should never reach this point.");



};

int DomeCore::dome_pull(DomeReq &req, FCGX_Request &request) {
    if(status.role == status.roleHead) {
    return DomeReq::SendSimpleResp(request, 500, "dome_pull only available on disk nodes");
  }

  Log(Logger::Lvl4, domelogmask, domelogname, "Entering");

  try {
    //DmlitePoolHandler stack(status.dmpool);

    std::string chksumtype = req.bodyfields.get<std::string>("checksum-type", "");
    std::string pfn = req.bodyfields.get<std::string>("pfn", "");
    std::string lfn = req.bodyfields.get<std::string>("lfn", "");
    int64_t neededspace = req.bodyfields.get<int64_t>("neededspace", 0LL);

    // Checksumtype in this case can be empty, as it's just a suggestion...
    if(pfn == "") {
      return DomeReq::SendSimpleResp(request, 422, "pfn cannot be empty.");
    }

    DomeFsInfo fsinfo;
    if(!status.PfnMatchesAnyFS(status.myhostname, pfn, fsinfo)) {
      return DomeReq::SendSimpleResp(request, 422, "pfn does not match any of the filesystems of this server.");
    }
    if(lfn == "") {
      return DomeReq::SendSimpleResp(request, 422, "lfn cannot be empty.");
    }
    if (!CFG->GetString("disk.filepuller.pullhook", (char *)"").size()) {
      return DomeReq::SendSimpleResp(request, 500, "File puller is disabled.");
    }

    Log(Logger::Lvl4, domelogmask, domelogname, "Request to pull pfn: '" << pfn << "' lfn: '" << lfn << "'");

// Commented out because it's normal that a new file has 0 size until it is pulled
//     // We retrieve the size of the remote file
//     int64_t filesz = 0LL;
//     {
//
//       std::string domeurl = CFG->GetString("disk.headnode.domeurl", (char *)"(empty url)/");
//
//       DomeTalker talker(*davixPool, req.creds, domeurl,
//                         "GET", "dome_getstatinfo");
//
//       if(!talker.execute(req.bodyfields)) {
//         Err(domelogname, talker.err());
//         return DomeReq::SendSimpleResp(request, 500, talker.err());
//       }
//
//       try {
//         filesz = talker.jresp().get<size_t>("size");
//       }
//       catch(boost::property_tree::ptree_error &e) {
//         std::string errmsg = SSTR("Received invalid json when talking to " << domeurl << ":" << e.what() << " '" << talker.response() << "'");
//         Err("takeJSONbodyfields", errmsg);
//         return DomeReq::SendSimpleResp(request, 500, errmsg);
//       }
//
//     }
//
//     Log(Logger::Lvl4, domelogmask, domelogname, "Remote size: " << filesz << " for pfn: '" << pfn << "' lfn: '" << lfn << "'");
//
//     if (filesz == 0LL) {
//       return DomeReq::SendSimpleResp(request, 422, SSTR("Cannot pull a 0-sized file. lfn: '" << lfn << "'") );
//     }

    // TODO: Doublecheck that there is a suitable replica in P status for the file that we want to fetch

    if (neededspace <= 0LL) {
      // Try getting the default space for the pool
      int64_t pool_defsize = 0LL;
      char pool_stype;
      if (!status.getPoolInfo(fsinfo.poolname, pool_defsize, pool_stype)) {
        Err("dome_pull", SSTR("Can't get pool for fs: '" << fsinfo.server << ":" << fsinfo.fs));
        return DomeReq::SendSimpleResp(request, 500, SSTR("Can't get pool for fs: '" << fsinfo.server << ":" << fsinfo.fs) );
      }
      neededspace = pool_defsize*2;
    }

    // Make sure that there is enough space to fit filesz bytes
    if (fsinfo.freespace < neededspace) {
      Log(Logger::Lvl1, domelogmask, domelogname, "Filesystem can only accommodate " << fsinfo.freespace << "B, filesize is : " << neededspace << " ... trying to purge volatile files.");
      std::vector<std::string> comps = Url::splitPath(pfn);
      if (comps.size() < 3)
        return DomeReq::SendSimpleResp(request, 422, SSTR("Invalid pfn: '" << pfn << "'") );

      // Drop the last two tokens, to get the fs+vo prefix
      comps.pop_back();
      comps.pop_back();

      std::string fsvopfx = Url::joinPath(comps);

      int freed = makespace(fsvopfx, neededspace);
      if (freed < neededspace)
        return DomeReq::SendSimpleResp(request, 422, SSTR("Volatile file purging failed. Not enough disk space to pull pfn: '" << pfn << "'") );
    }

    // TODO: Make sure that the phys file does not already exist

    Log(Logger::Lvl1, domelogmask, domelogname, "Starting filepull. Remote size: " << neededspace << " for pfn: '" << pfn << "' lfn: '" << lfn << "'");

    // Create the necessary directories, if needed
    try {
      DomeUtils::mkdirp(pfn);
    }
    catch(DmException &e) {
      return DomeReq::SendSimpleResp(request, DOME_HTTP_INTERNAL_SERVER_ERROR, SSTR("Unable to create physical directories for '" << pfn << "'- internal error: '" << e.what() << "'"));
    }

    // Let's just execute the external hook, passing the obvious parameters

    PendingPull pending(lfn, status.myhostname, pfn, req.creds, chksumtype);

    std::vector<std::string> params;
    params.push_back(CFG->GetString("disk.filepuller.pullhook", (char *)""));
    params.push_back(lfn);
    params.push_back(pfn);
    params.push_back(SSTR(neededspace));
    int id = this->submitCmd(params);

    if (id < 0)
      return DomeReq::SendSimpleResp(request, 500, "Could not invoke file puller.");

    diskPendingPulls[id] = pending;

    // Now exit, the file pull is hopefully ongoing

    return DomeReq::SendSimpleResp(request, 202, SSTR("Initiated file pull. lfn: '" << lfn << "' pfn: '"<< pfn << "', task executor ID: " << id));
  }
  catch(dmlite::DmException& e) {
    std::ostringstream os("An error has occured.\r\n");
    os << "Dmlite exception: " << e.what();
    return DomeReq::SendSimpleResp(request, 404, os);
  }


};

// returns true if str2 is a strict subdir of str1
// both arguments are assumed not to have trailing slashes
static bool is_subdir(const std::string &str1, const std::string &str2) {
  size_t pos = str1.find(str2);
  return pos == 0 && str1.length() > str2.length() && str1[str2.length()] == '/';
}

int DomeCore::dome_getquotatoken(DomeReq &req, FCGX_Request &request) {
  std::string absPath = DomeUtils::trim_trailing_slashes(req.bodyfields.get<std::string>("path", ""));
  Log(Logger::Lvl4, domelogmask, domelogname, "Processing: '" << absPath << "'");
  bool getsubdirs = req.bodyfields.get<bool>("getsubdirs", false);
  bool getparentdirs = req.bodyfields.get<bool>("getparentdirs", false);

  // Get the ones that match the object of the query
  boost::property_tree::ptree jresp;
  int cnt = 0;

  // Make a local copy of the quotas to loop on
  std::multimap <std::string, DomeQuotatoken> localquotas;
  {
    boost::unique_lock<boost::recursive_mutex> l(status);
    localquotas = status.quotas;
  }
  
  for (std::multimap<std::string, DomeQuotatoken>::iterator it = localquotas.begin(); it != localquotas.end(); ++it) {
    bool match = false;

    if(absPath == it->second.path) {
      match = true; // perfect match, exact directory we're looking for
    }
    else if(getparentdirs && is_subdir(absPath, it->second.path)) {
      match = true; // parent dir match
    }
    else if(getsubdirs && is_subdir(it->second.path, absPath)) {
      match = true; // subdir match
    }

    Log(Logger::Lvl4, domelogmask, domelogname, "Checking: '" << it->second.path << "' versus '" << absPath << "' getparentdirs: " << getparentdirs << " getsubdirs: " << getsubdirs << " match: " << match);
    if(!match) continue;

    // Get the used space for this path
    long long pathfree = 0LL;
    long long pathused = 0LL;
    DmlitePoolHandler stack(status.dmpool);
    try {
      struct dmlite::ExtendedStat st = stack->getCatalog()->extendedStat(absPath);
      pathused = st.stat.st_size;
    }
    catch (dmlite::DmException e) {
      std::ostringstream os;
      os << "Found quotatokens for non-existing path '"<< it->second.path << "' : " << e.code() << "-" << e.what();

      Err(domelogname, os.str());
      continue;
    }

    // Now find the free space in the mentioned pool
    long long ptot, pfree;
    int poolst;
    status.getPoolSpaces(it->second.poolname, ptot, pfree, poolst);

    pathfree = ( (it->second.t_space - pathused < ptot - pathused) ? it->second.t_space - pathused : ptot - pathused );
    if (pathfree < 0) pathfree = 0;

    Log(Logger::Lvl4, domelogmask, domelogname, "Quotatoken '" << it->second.u_token << "' of pool: '" <<
    it->second.poolname << "' matches path '" << absPath << "' quotatktotspace: " << it->second.t_space <<
    " pooltotspace: " << ptot << " pathusedspace: " << pathused << " pathfreespace: " << pathfree );

    boost::property_tree::ptree pt, grps;
    pt.put("path", it->second.path);
    pt.put("quotatkname", it->second.u_token);
    pt.put("quotatkpoolname", it->second.poolname);
    pt.put("quotatktotspace", it->second.t_space);
    pt.put("pooltotspace", ptot);
    pt.put("pathusedspace", pathused);
    pt.put("pathfreespace", pathfree);

    // Push the groups array into the response
    for (unsigned i = 0; i < it->second.groupsforwrite.size(); i++) {
      DomeGroupInfo gi;
      int thisgid = atoi(it->second.groupsforwrite[i].c_str());

      if (!status.getGroup(thisgid, gi))
        grps.push_back(std::make_pair(it->second.groupsforwrite[i], "<unknown>"));
      else
        grps.push_back(std::make_pair(it->second.groupsforwrite[i], gi.groupname));
    }
    pt.push_back(std::make_pair("groups", grps));

    jresp.push_back(std::make_pair(it->second.s_token, pt));
    cnt++;
  }

  if (cnt > 0) {
    return DomeReq::SendSimpleResp(request, 200, jresp);
  }

  return DomeReq::SendSimpleResp(request, 404, SSTR("No quotatokens match path '" << absPath << "'"));

};

template<class T>
static void set_if_field_exists(T& target, const boost::property_tree::ptree &bodyfields, const std::string &key) {
  if(bodyfields.count(key) != 0) {
    target = bodyfields.get<T>(key);
  }
}

static bool translate_group_names(DomeStatus &status, const std::string &groupnames, std::vector<std::string> &ids, std::string &err) {
  std::vector<std::string> groupnames_vec = DomeUtils::split(groupnames, ",");

  ids.clear();
  ids.push_back("0"); // not really sure if necessary

  for(size_t i = 0; i < groupnames_vec.size(); i++) {
    DomeGroupInfo tmp;
    if(status.getGroup(groupnames_vec[i], tmp) == 0) {
      err = SSTR("Invalid group name: " << groupnames_vec[i]);
      return false;
    }
    ids.push_back(SSTR(tmp.groupid));
  }
  return true;
}

int DomeCore::dome_setquotatoken(DomeReq &req, FCGX_Request &request) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering.");

  DomeQuotatoken mytk;

  mytk.path = DomeUtils::trim_trailing_slashes(req.bodyfields.get("path", ""));
  mytk.poolname = req.bodyfields.get("poolname", "");

  if (!status.existsPool(mytk.poolname)) {
    std::ostringstream os;
    os << "Cannot find pool: '" << mytk.poolname << "'";

    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, 404, os);
  }

  DmlitePoolHandler stack(status.dmpool);
  try {
    struct dmlite::ExtendedStat st = stack->getCatalog()->extendedStat(mytk.path);
  }
  catch (dmlite::DmException e) {
    std::ostringstream os;
    os << "Cannot find logical path: '" << mytk.path << "'";

    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, 404, os);

  }

  // We fetch the values that we may have in the internal map, using the keys
  if ( status.getQuotatoken(mytk.path, mytk.poolname, mytk) ) {
    std::ostringstream os;
    Log(Logger::Lvl1, domelogmask, domelogname, "No quotatoken found for pool: '" <<
      mytk.poolname << "' path '" << mytk.path << "'. Creating new one.");

    // set default values
    mytk.t_space = 0LL;
    mytk.u_token = "(unnamed)";
    mytk.s_token = "";
  }

  set_if_field_exists(mytk.t_space, req.bodyfields, "quotaspace");
  set_if_field_exists(mytk.u_token, req.bodyfields, "description");
  set_if_field_exists(mytk.s_token, req.bodyfields, "uniqueid");

  if(req.bodyfields.count("groups") != 0) {
    std::string err;
    if(!translate_group_names(status, req.bodyfields.get("groups", ""), mytk.groupsforwrite, err)) {
      return DomeReq::SendSimpleResp(request, 422, SSTR("Unable to write quotatoken - " << err));
    }
  }

  // First we write into the db, if it goes well then we update the internal map
  int rc;
  {
  DomeMySql sql;
  DomeMySqlTrans  t(&sql);
  std::string clientid = req.creds.clientName;
  if (clientid.size() == 0) clientid = req.clientdn;
  if (clientid.size() == 0) clientid = "(unknown)";
  rc =  sql.setQuotatoken(mytk, clientid);
  if (!rc) t.Commit();
  }

  if (rc) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Cannot write quotatoken into the DB. poolname: '" << mytk.poolname
      << "' t_space: " << mytk.t_space << " u_token: '" << mytk.u_token << "'"));
    return 1;
  }

  status.loadQuotatokens();
  return DomeReq::SendSimpleResp(request, 200, SSTR("Quotatoken written. poolname: '" << mytk.poolname
      << "' t_space: " << mytk.t_space << " u_token: '" << mytk.u_token << "'"));

};


int DomeCore::dome_delquotatoken(DomeReq &req, FCGX_Request &request) {
  if (status.role != status.roleHead) {
    return DomeReq::SendSimpleResp(request, 500, "dome_delquotatoken only available on head nodes.");
  }
  DomeQuotatoken mytk;

  mytk.path = req.bodyfields.get("path", "");
  mytk.poolname = req.bodyfields.get("poolname", "");

  if (!status.existsPool(mytk.poolname)) {
    std::ostringstream os;
    os << "Cannot find pool: '" << mytk.poolname << "'";

    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, 404, os);
  }


  // We fetch the values that we may have in the internal map, using the keys, and remove it
  if ( status.delQuotatoken(mytk.path, mytk.poolname, mytk) ) {
    std::ostringstream os;
    os << "No quotatoken found for pool: '" <<
      mytk.poolname << "' path '" << mytk.path << "'.";

    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, 404, os);

  }

  // If everything was ok, we delete it from the db too
  // First we write into the db, if it goes well then we update the internal map
  int rc;
  {
  DomeMySql sql;
  DomeMySqlTrans  t(&sql);
  std::string clientid = req.creds.clientName;
  if (clientid.size() == 0) clientid = req.clientdn;
  if (clientid.size() == 0) clientid = "(unknown)";
  rc =  sql.delQuotatoken(mytk, clientid);
  if (!rc) t.Commit();
  }

  if (rc) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Cannot delete quotatoken from the DB. poolname: '" << mytk.poolname
      << "' t_space: " << mytk.t_space << " u_token: '" << mytk.u_token << "'"));
    return 1;
  }

  // To avoid race conditions without locking, we have to make sure that it's not in memory
  status.delQuotatoken(mytk.path, mytk.poolname, mytk);

  return DomeReq::SendSimpleResp(request, 200, SSTR("Quotatoken deleted. poolname: '" << mytk.poolname
      << "' t_space: " << mytk.t_space << " u_token: '" << mytk.u_token << "'"));

};

int DomeCore::dome_modquotatoken(DomeReq &req, FCGX_Request &request) {
  if(status.role != status.roleHead) {
    return DomeReq::SendSimpleResp(request, 500, "dome_modquotatoken only available on head nodes");
  }

  std::string tokenid = req.bodyfields.get<std::string>("tokenid", "");
  if(tokenid.empty()) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("tokenid is empty."));
  }

  DomeQuotatoken mytk;
  if(!status.getQuotatoken(tokenid, mytk)) {
    return DomeReq::SendSimpleResp(request, 404, SSTR("No quotatoken with id '" << tokenid << "' could be found"));
  }

  set_if_field_exists(mytk.t_space, req.bodyfields, "quotaspace");
  set_if_field_exists(mytk.u_token, req.bodyfields, "description");
  set_if_field_exists(mytk.path, req.bodyfields, "path");
  set_if_field_exists(mytk.poolname, req.bodyfields, "poolname");

  if(req.bodyfields.count("groups") != 0) {
    std::string err;
    if(!translate_group_names(status, req.bodyfields.get("groups", ""), mytk.groupsforwrite, err)) {
      return DomeReq::SendSimpleResp(request, 422, SSTR("Unable to write quotatoken - " << err));
    }
  }

  // First we write into the db, if it goes well then we update the internal map
  int rc;
  {
  DomeMySql sql;
  DomeMySqlTrans  t(&sql);
  rc =  sql.setQuotatokenByStoken(mytk);
  if (!rc) t.Commit();
  }

  if (rc) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Cannot write quotatoken into the DB. poolname: '" << mytk.poolname
      << "' t_space: " << mytk.t_space << " u_token: '" << mytk.u_token << "'"));
    return 1;
  }

  status.loadQuotatokens();
  return DomeReq::SendSimpleResp(request, 200, SSTR("Quotatoken written. poolname: '" << mytk.poolname
      << "' t_space: " << mytk.t_space << " u_token: '" << mytk.u_token << "'"));
}


int DomeCore::dome_pfnrm(DomeReq &req, FCGX_Request &request) {

  if (status.role != status.roleDisk) {
    return DomeReq::SendSimpleResp(request, 500, "pfnrm only available on disk nodes");
  }

  std::string absPath =  req.bodyfields.get<std::string>("pfn", "");
  if (!absPath.size()) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Path '" << absPath << "' is empty."));
  }

  if (absPath[0] != '/') {
    return DomeReq::SendSimpleResp(request, 404, SSTR("Path '" << absPath << "' is not an absolute path."));
  }

  // Remove any trailing slash
  while (absPath[ absPath.size()-1 ] == '/') {
    absPath.erase(absPath.size() - 1);
  }

  if (!status.PfnMatchesAnyFS(status.myhostname, absPath)) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Path '" << absPath << "' is not a valid pfn."));
  }

  // OK, remove directly on disk

  // This is not a high perf function, so we can afford one stat call
  struct stat st;
  int rc = stat(absPath.c_str(), &st);
  if (rc) {
    if (errno == ENOENT) {
      return DomeReq::SendSimpleResp(request, 200, SSTR("Rm successful. The file or dir '" << absPath << "' not there anyway."));
    }

    char errbuf[1024];
    return DomeReq::SendSimpleResp(request, 422, SSTR("Rm of '" << absPath << "' failed. err: " << errno << " msg: " << strerror_r(errno, errbuf, sizeof(errbuf))));
  }

  if (S_ISDIR(st.st_mode)) {
    int rc = rmdir(absPath.c_str());
    if (rc) {
      char errbuf[1024];
      return DomeReq::SendSimpleResp(request, 422, SSTR("Rmdir of directory '" << absPath << "' failed. err: " << errno << " msg: " << strerror_r(errno, errbuf, sizeof(errbuf))));
    }

  }
  else {
  int rc = unlink(absPath.c_str());
  if (rc) {
      char errbuf[1024];
      return DomeReq::SendSimpleResp(request, 422, SSTR("Rm of file '" << absPath << "' failed. err: " << errno << " msg: " << strerror_r(errno, errbuf, sizeof(errbuf))));
    }
  }

  return DomeReq::SendSimpleResp(request, 200, SSTR("Rm successful."));
}

int DomeCore::dome_delreplica(DomeReq &req, FCGX_Request &request) {
  if (status.role != status.roleHead) {
    return DomeReq::SendSimpleResp(request, 500, "dome_delreplica only available on head nodes.");
  }

  std::string absPath =  req.bodyfields.get<std::string>("pfn", "");
  std::string srv =  req.bodyfields.get<std::string>("server", "");

  Log(Logger::Lvl4, domelogmask, domelogname, " srv: '" << srv << "' pfn: '" << absPath << "' ");

  if (!absPath.size()) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Path '" << absPath << "' is empty."));
  }
  if (!srv.size()) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Server name '" << srv << "' is empty."));
  }

  if (absPath[0] != '/') {
    return DomeReq::SendSimpleResp(request, 404, SSTR("Path '" << absPath << "' is not an absolute path."));
  }

  // Remove any trailing slash
  while (absPath[ absPath.size()-1 ] == '/') {
    absPath.erase(absPath.size() - 1);
  }

  if (!status.PfnMatchesAnyFS(srv, absPath)) {
    return DomeReq::SendSimpleResp(request, 404, SSTR("Path '" << absPath << "' is not a valid pfn for server '" << srv << "'"));
  }


  // Get the replica. Unfortunately to delete it we must first fetch it
  DmlitePoolHandler stack(status.dmpool);
  std::string rfiopath = srv + ":" + absPath;
  Log(Logger::Lvl4, domelogmask, domelogname, "Getting replica: '" << rfiopath);
  dmlite::Replica rep;
  try {
    rep = stack->getCatalog()->getReplicaByRFN(rfiopath);
  } catch (DmException e) {
    std::ostringstream os;
    os << "Cannot find replica '"<< rfiopath << "' : " << e.code() << "-" << e.what();
    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, 404, os);
  }


  // We fetched it, which means that many things are fine.
  // Now delete the physical file
  std::string diskurl = "https://" + srv + "/domedisk/";
  Log(Logger::Lvl4, domelogmask, domelogname, "Dispatching deletion of replica '" << absPath << "' to disk node: '" << diskurl);

  DomeTalker talker(*davixPool, req.creds, diskurl,
                    "POST", "dome_pfnrm");

  if(!talker.execute(req.bodyfields)) {
    Err(domelogname, talker.err());
    return DomeReq::SendSimpleResp(request, 500, talker.err());
  }

  dmlite::INode *ino;
  try {
    ino = stack->getINode();
    if (!ino)
      Err(domelogname, "Cannot retrieve inode interface.");
  } catch (DmException e) {
    std::ostringstream os;
    os << "Cannot find replicas for fileid: '"<< rep.fileid << "' : " << e.code() << "-" << e.what();
    Err(domelogname, os.str());
    //return DomeReq::SendSimpleResp(request, 404, os);
  }

  if (ino) {
    Log(Logger::Lvl4, domelogmask, domelogname, "Removing replica: '" << rep.rfn);
    // And now remove the replica
    try {
      ino->deleteReplica(rep);
    } catch (DmException e) {
      std::ostringstream os;
      os << "Cannot find replica '"<< rfiopath << "' : " << e.code() << "-" << e.what();
      Err(domelogname, os.str());
      return DomeReq::SendSimpleResp(request, 404, os);
    }

    Log(Logger::Lvl4, domelogmask, domelogname, "Check if we have to remove the logical file entry: '" << rep.fileid);


    // Get the file size :-(
    int64_t sz = 0;
    dmlite::ExtendedStat st;
    try {
      st = ino->extendedStat(rep.fileid);
      sz = st.stat.st_size;

    } catch (DmException e) {
      std::ostringstream os;
      os << "Cannot fetch logical entry for replica '"<< rep.rfn << "' Id: " << rep.fileid << " : " << e.code() << "-" << e.what();

      Err(domelogname, os.str());
    }

    std::vector<Replica> repls;
    try {

      repls = ino->getReplicas(rep.fileid);

    } catch (DmException e) {
      std::ostringstream os;
      os << "Cannot find replicas for fileid: '"<< rep.fileid << "' : " << e.code() << "-" << e.what();
      Err(domelogname, os.str());
      //return DomeReq::SendSimpleResp(request, 404, os);
    }

    if (repls.size() == 0) {
      // Delete the logical entry if this was the last replica
      try {

        ino->unlink(rep.fileid);

      } catch (DmException e) {
        std::ostringstream os;
        os << "Cannot unlink fileid: '"<< rep.fileid << "' : " << e.code() << "-" << e.what();
        Err(domelogname, os.str());
        //return DomeReq::SendSimpleResp(request, 404, os);
      }
    }

/*
    // Subtract this filesize to the size of its parent dirs, only the first N levels
    {


      // Start transaction
      InodeTrans trans(ino);

      sz = st.stat.st_size;

      ino_t hierarchy[128];
      size_t hierarchysz[128];
      unsigned int idx = 0;
      while (st.parent) {

        Log(Logger::Lvl4, domelogmask, domelogname, " Going to stat " << st.parent << " parent of " << st.stat.st_ino << " with idx " << idx);

        try {
          st = ino->extendedStat(st.parent);
        }
        catch (DmException& e) {
          Err( domelogname , " Cannot stat inode " << st.parent << " parent of " << st.stat.st_ino);
          return -1;
        }

        hierarchy[idx] = st.stat.st_ino;
        hierarchysz[idx] = st.stat.st_size;

        Log(Logger::Lvl4, domelogmask, domelogname, " Size of inode " << st.stat.st_ino <<
        " is " << st.stat.st_size << " with idx " << idx);

        idx++;

        if (idx >= sizeof(hierarchy)) {
          Err( domelogname , " Too many parent directories for replica " << rep.rfn);
          return -1;
        }
      }

      // Update the filesize in the first levels
      // Avoid the contention on /dpm/voname/home
      if (idx > 0) {
        Log(Logger::Lvl4, domelogmask, domelogname, " Going to set sizes. Max depth found: " << idx);
        for (int i = MAX(0, idx-3); i >= MAX(0, idx-1-CFG->GetLong("head.dirspacereportdepth", 6)); i--) {
          Log(Logger::Lvl4, domelogmask, domelogname, " Inode: " << hierarchy[i] << " Size: " << hierarchysz[i] << "-->" <<  hierarchysz[i] - sz);
          ino->setSize(hierarchy[i], hierarchysz[i] - sz);
        }
      }
      else {
        Log(Logger::Lvl4, domelogmask, domelogname, " Cannot set any size. Max depth found: " << idx);
      }


      // Commit the local trans object
      // This also releases the connection back to the pool
      trans.Commit();
    }*/


    // For backward compatibility with the DPM daemon, we also update its
    // spacetoken counters, adjusting u_space
    {
      DomeQuotatoken token;


      if (rep.setname.size() > 0) {
        Log(Logger::Lvl4, domelogmask, domelogname, " Accounted space token: '" << rep.setname <<
        "' rfn: '" << rep.rfn << "'");

        DomeMySql sql;
        DomeMySqlTrans  t(&sql);
        // Free some space
        sql.addtoQuotatokenUspace(rep.setname, sz);
        t.Commit();
      }

    }

  }


return DomeReq::SendSimpleResp(request, 200, SSTR("Deleted '" << absPath << "' in server '" << srv << "'. Have a nice day."));
}


  /// Removes a pool and all the related filesystems
int DomeCore::dome_rmpool(DomeReq &req, FCGX_Request &request) {
  if (status.role != status.roleHead) {
    return DomeReq::SendSimpleResp(request, 500, "dome_rmpool only available on head nodes.");
  }

  std::string poolname =  req.bodyfields.get<std::string>("poolname", "");

  Log(Logger::Lvl4, domelogmask, domelogname, " poolname: '" << poolname << "'");

  if (!poolname.size()) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("poolname '" << poolname << "' is empty."));
  }

  // First we write into the db, if it goes well then we update the internal map
  int rc;
  {
  DomeMySql sql;
  DomeMySqlTrans  t(&sql);

  rc =  sql.rmPool(poolname);
  if (!rc) t.Commit();
  }

  if (rc) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Cannot delete pool: '" << poolname << "'"));
    return 1;
  }

  status.loadFilesystems();
  return DomeReq::SendSimpleResp(request, 200, "Pool deleted.");
}

int DomeCore::dome_statpfn(DomeReq &req, FCGX_Request &request) {
  if (status.role != status.roleDisk) {
    return DomeReq::SendSimpleResp(request, 500, "dome_statpfn only available on disk nodes.");
  }


  std::string pfn = req.bodyfields.get<std::string>("pfn", "");
  bool matchesfs = DomeUtils::str_to_bool(req.bodyfields.get<std::string>("matchfs", "true"));

  Log(Logger::Lvl4, domelogmask, domelogname, " pfn: '" << pfn << "'");

  if (!pfn.size()) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("pfn '" << pfn << "' is empty."));
  }

  if (matchesfs && !status.PfnMatchesAnyFS(status.myhostname, pfn)) {
    return DomeReq::SendSimpleResp(request, 404, SSTR("Path '" << pfn << "' does not match any existing filesystems in disk server '" << status.myhostname << "'"));
  }

  struct stat st;

  if ( stat(pfn.c_str(), &st) ) {
    std::ostringstream os;
    char errbuf[1024];
    os << "Cannot stat pfn:'" << pfn << "' err: " << errno << ":" << strerror_r(errno, errbuf, 1023);
    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, 404, os);
  }

  Log(Logger::Lvl2, domelogmask, domelogname, " pfn: '" << pfn << "' "
    " disksize: " << st.st_size << " flags: " << st.st_mode);


  boost::property_tree::ptree jresp;
  jresp.put("size", st.st_size);
  jresp.put("mode", st.st_mode);
  jresp.put("isdir", ( S_ISDIR(st.st_mode) ));

  return DomeReq::SendSimpleResp(request, 200, jresp);



};

int DomeCore::dome_addpool(DomeReq &req, FCGX_Request &request) {
  if(status.role != status.roleHead) {
    return DomeReq::SendSimpleResp(request, 500, "dome_addpool only available on head nodes.");
  }

  std::string poolname = req.bodyfields.get<std::string>("poolname", "");
  long pool_defsize = req.bodyfields.get("pool_defsize", 3L * 1024 * 1024 * 1024);
  std::string pool_stype = req.bodyfields.get("pool_stype", "P");

  Log(Logger::Lvl4, domelogmask, domelogname, " poolname: '" << poolname << "'");

  if (!poolname.size()) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("poolname '" << poolname << "' is empty."));
  }
  if (pool_defsize < 1024*1024) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Invalid defsize: " << pool_defsize));
  }
  if(pool_stype != "P" && pool_stype != "V") {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Invalid pool_stype: " << pool_stype));
  }

  // make sure it doesn't already exist
  {
    boost::unique_lock<boost::recursive_mutex> l(status);

    for (std::vector<DomeFsInfo>::iterator fs = status.fslist.begin(); fs != status.fslist.end(); fs++) {
      if(fs->poolname == poolname) {
        return DomeReq::SendSimpleResp(request, 422, SSTR("poolname '" << poolname << "' already exists."));
      }
    }

    if (status.poolslist.find(poolname) != status.poolslist.end()) {
      return DomeReq::SendSimpleResp(request, 422, SSTR("poolname '" << poolname << "' already exists in the groups map (may have no filesystems)."));
    }
  }

  int rc;
  {
  DomeMySql sql;
  DomeMySqlTrans  t(&sql);
  rc =  sql.addPool(poolname, pool_defsize, pool_stype[0]);
  if (!rc) t.Commit();
  }

  if (rc) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Could not add new pool - error code: " << rc));
  }

  status.loadFilesystems();
  return DomeReq::SendSimpleResp(request, 200, "Pool was created.");
}




int DomeCore::dome_modifypool(DomeReq &req, FCGX_Request &request) {
  if(status.role != status.roleHead) {
    return DomeReq::SendSimpleResp(request, 500, "dome_modifypool only available on head nodes.");
  }

  std::string poolname = req.bodyfields.get<std::string>("poolname", "");
  long pool_defsize = req.bodyfields.get("pool_defsize", 3L * 1024 * 1024 * 1024);
  std::string pool_stype = req.bodyfields.get("pool_stype", "P");

  Log(Logger::Lvl4, domelogmask, domelogname, " poolname: '" << poolname << "'");

  if (!poolname.size()) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("poolname '" << poolname << "' is empty."));
  }
  if (pool_defsize < 1024*1024) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Invalid defsize: " << pool_defsize));
  }
  if (!pool_stype.size()) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("pool_stype '" << pool_stype << "' is empty."));
  }

  // make sure it DOES exist
  {
    boost::unique_lock<boost::recursive_mutex> l(status);

    if (status.poolslist.find(poolname) == status.poolslist.end()) {
      return DomeReq::SendSimpleResp(request, 422, SSTR("poolname '" << poolname << "' does not exist, cannot modify it."));
    }
  }

  int rc;
  {
    DomeMySql sql;
    DomeMySqlTrans  t(&sql);
    rc =  sql.addPool(poolname, pool_defsize, pool_stype[0]);
    if (!rc) t.Commit();
  }

  if (rc) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Could not modify pool - error code: " << rc));
  }

  status.loadFilesystems();
  return DomeReq::SendSimpleResp(request, 200, "Pool was modified.");
}





/// Adds a filesystem to a pool
int DomeCore::dome_addfstopool(DomeReq &req, FCGX_Request &request) {
  if (status.role != status.roleHead) {
    return DomeReq::SendSimpleResp(request, 500, "dome_addfstopool only available on head nodes.");
  }

  std::string poolname =  req.bodyfields.get<std::string>("poolname", "");
  std::string server =  req.bodyfields.get<std::string>("server", "");
  std::string newfs =  req.bodyfields.get<std::string>("fs", "");
  int fsstatus =  req.bodyfields.get<int>("status", 0); // DomeFsStatus::FsStaticActive



  Log(Logger::Lvl4, domelogmask, domelogname, " poolname: '" << poolname << "'");

  if (!poolname.size()) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("poolname '" << poolname << "' is empty."));
  }

  if ((fsstatus < 0) || (fsstatus > 2))
    return DomeReq::SendSimpleResp(request, 422, SSTR("Invalid status '" << fsstatus << "'. Should be 0, 1 or 2."));

  // Make sure it's not already there or that we are not adding a parent/child of an existing fs
  for (std::vector<DomeFsInfo>::iterator fs = status.fslist.begin(); fs != status.fslist.end(); fs++) {
    if ( status.PfnMatchesFS(server, newfs, *fs) )
      return DomeReq::SendSimpleResp(request, 422, SSTR("Filesystem '" << server << ":" << fs->fs << "' already exists or overlaps an existing filesystem."));
  }
  // Stat the remote path, to make sure it exists and it makes sense
  std::string diskurl = "https://" + server + "/domedisk/";
  Log(Logger::Lvl4, domelogmask, domelogname, "Stat-ing new filesystem '" << newfs << "' in disk node: '" << server);

  DomeTalker talker(*davixPool, req.creds, diskurl,
                    "GET", "dome_statpfn");

  boost::property_tree::ptree jresp;
  jresp.put("pfn", newfs);
  jresp.put("matchfs", "false");
  jresp.put("server", server);

  if(!talker.execute(jresp)) {
    Err(domelogname, talker.err());
    return DomeReq::SendSimpleResp(request, 500, talker.err());
  }

  // Everything seems OK here, like UFOs invading Earth. We can start updating values.
  // First we write into the db, if it goes well then we update the internal map
  int rc;
  {
  DomeMySql sql;
  DomeMySqlTrans  t(&sql);

  DomeFsInfo fsfs;
  fsfs.poolname = poolname;
  fsfs.server = server;
  fsfs.fs = newfs;
  fsfs.status = (DomeFsInfo::DomeFsStatus)fsstatus;


  rc =  sql.addFs(fsfs);
  if (!rc) t.Commit();
  }

  if (rc) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Could not insert new fs: '" << newfs << "' It likely already exists."));
    return 1;
  }

  status.loadFilesystems();
  return DomeReq::SendSimpleResp(request, 200, SSTR("New filesystem added."));
}



/// Modifies an existing filesystem
int DomeCore::dome_modifyfs(DomeReq &req, FCGX_Request &request) {
  if (status.role != status.roleHead) {
    return DomeReq::SendSimpleResp(request, 500, "dome_modifyfs only available on head nodes.");
  }

  std::string poolname =  req.bodyfields.get<std::string>("poolname", "");
  std::string server =  req.bodyfields.get<std::string>("server", "");
  std::string newfs =  req.bodyfields.get<std::string>("fs", "");
  int fsstatus =  req.bodyfields.get<int>("status", 0); // DomeFsStatus::FsStaticActive



  Log(Logger::Lvl4, domelogmask, domelogname, " poolname: '" << poolname << "'");

  if (!poolname.size()) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("poolname '" << poolname << "' is empty."));
  }

  if ((fsstatus < 0) || (fsstatus > 2))
    return DomeReq::SendSimpleResp(request, 422, SSTR("Invalid status '" << fsstatus << "'. Should be 0, 1 or 2."));

  // Make sure it is already there exactly
  {
    // Lock status!
    boost::unique_lock<boost::recursive_mutex> l(status);

    for (std::vector<DomeFsInfo>::iterator fs = status.fslist.begin(); fs != status.fslist.end(); fs++) {
      if ( status.PfnMatchesFS(server, newfs, *fs) ) {
        if (fs->fs.length() != newfs.length())
          return DomeReq::SendSimpleResp(request, 422, SSTR("Filesystem '" << server << ":" << newfs << "' overlaps the existing filesystem '" << fs->fs << "'"));
      }
    }
  }
  // Stat the remote path, to make sure it exists and it makes sense
  std::string diskurl = "https://" + server + "/domedisk/";
  Log(Logger::Lvl4, domelogmask, domelogname, "Stat-ing new filesystem '" << newfs << "' in disk node: '" << server);

  DomeTalker talker(*davixPool, req.creds, diskurl,
                    "GET", "dome_statpfn");

  boost::property_tree::ptree jresp;
  jresp.put("pfn", newfs);
  jresp.put("matchfs", "false");
  jresp.put("server", server);

  if(!talker.execute(jresp)) {
    Err(domelogname, talker.err());
    return DomeReq::SendSimpleResp(request, 500, talker.err());
  }

  // Everything seems OK here, the technological singularity will come. We can start updating values.
  // First we write into the db, if it goes well then we update the internal map
  int rc;
  {
    DomeMySql sql;
    DomeMySqlTrans  t(&sql);

    DomeFsInfo fsfs;
    fsfs.poolname = poolname;
    fsfs.server = server;
    fsfs.fs = newfs;
    fsfs.status = (DomeFsInfo::DomeFsStatus)fsstatus;


    rc =  sql.modifyFs(fsfs);
    if (!rc) t.Commit();
  }

  if (rc) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Could not insert new fs: '" << newfs << "' It likely already exists."));
    return 1;
  }

  status.loadFilesystems();
  return DomeReq::SendSimpleResp(request, 200, SSTR("New filesystem added."));
}


/// Removes a filesystem, no matter to which pool it was attached
int DomeCore::dome_rmfs(DomeReq &req, FCGX_Request &request) {
  if (status.role != status.roleHead) {
    return DomeReq::SendSimpleResp(request, 500, "dome_rmfs only available on head nodes.");
  }

  std::string server =  req.bodyfields.get<std::string>("server", "");
  std::string newfs = req.bodyfields.get<std::string>("fs", "");

  Log(Logger::Lvl4, domelogmask, domelogname, " serrver: '" << server << "' fs: '" << newfs << "'");

  int ndel = 0;
  bool found = false;
  {
    // Lock status!
    boost::unique_lock<boost::recursive_mutex> l(status);

    for (std::vector<DomeFsInfo>::iterator fs = status.fslist.begin(); fs != status.fslist.end(); fs++) {
      if(newfs == fs->fs) {
        found = true;
        break;
      }
    }
  }

  if(!found) {
    return DomeReq::SendSimpleResp(request, DOME_HTTP_NOT_FOUND, SSTR("Filesystem '" << newfs << "' not found on server '" << server << "'"));
  }

  int rc;
  {
  DomeMySql sql;
  DomeMySqlTrans  t(&sql);
  rc =  sql.rmFs(server, newfs);
  if (!rc) t.Commit();
  }

  if (rc)
    return DomeReq::SendSimpleResp(request, 422, SSTR("Failed deleting filesystem '" << newfs << "' of server '" << server << "'"));

  status.loadFilesystems();
  return DomeReq::SendSimpleResp(request, 200, SSTR("Deleted " << ndel << "filesystems matching '" << newfs << "' of server '" << server << "'"));
}




static void xstat_to_ptree(const dmlite::ExtendedStat& xstat, boost::property_tree::ptree &ptree) {
  ptree.put("fileid", xstat.stat.st_ino);
  ptree.put("parentfileid", xstat.parent);
  ptree.put("size", xstat.stat.st_size);
  ptree.put("mode", xstat.stat.st_mode);
  ptree.put("atime", xstat.stat.st_atime);
  ptree.put("mtime", xstat.stat.st_mtime);
  ptree.put("ctime", xstat.stat.st_ctime);
  ptree.put("uid", xstat.stat.st_uid);
  ptree.put("gid", xstat.stat.st_gid);
  ptree.put("nlink", xstat.stat.st_nlink);
  ptree.put("name", xstat.name);
  ptree.put("xattrs", xstat.serialize());
}


/// Fecthes logical stat information for an LFN or file ID or a pfn
int DomeCore::dome_getstatinfo(DomeReq &req, FCGX_Request &request) {
  if (status.role != status.roleHead) {
    return DomeReq::SendSimpleResp(request, DOME_HTTP_BAD_REQUEST, "dome_getstatinfo only available on head nodes.");
  }

  std::string server =  req.bodyfields.get<std::string>("server", "");
  std::string pfn =  req.bodyfields.get<std::string>("pfn", "");
  std::string rfn =  req.bodyfields.get<std::string>("rfn", "");
  std::string lfn =  req.bodyfields.get<std::string>("lfn", "");

  Log(Logger::Lvl4, domelogmask, domelogname, " server: '" << server << "' pfn: '" << pfn << "' rfn: '" << rfn << "' lfn: '" << lfn << "'");

  struct dmlite::ExtendedStat st;

  // If lfn is filled then we stat the logical file
  if (lfn.size()) {
    DmStatus ret;

    {
        DomeMySql sql;
        ret = sql.getStatbyLFN(st, lfn);
    }

    if (ret.code() == ENOENT) {
        // If the lfn maps to a pool that can pull files then we want to invoke
        // the external stat hook before concluding that the file is not available
        DomeFsInfo fsnfo;
        if (status.LfnMatchesAnyCanPullFS(lfn, fsnfo)) {
          // problem... for an external file there's no damn inode yet, let's set it to 0

          std::string hook = CFG->GetString("head.filepuller.stathook", (char *)"");

          if ((hook.size() < 5) || (hook[0] != '/'))
            return DomeReq::SendSimpleResp(request, 500, "Invalid stat hook.");

          std::vector<std::string> params;
          params.push_back(hook);
          params.push_back(lfn);

          int id = this->submitCmd(params);

          if (id < 0)
            return DomeReq::SendSimpleResp(request, 500, "Could not invoke stat hook.");

          // Now wait for the process to have finished
          int taskrc = waitResult(id, CFG->GetLong("head.filepuller.stathooktimeout", 60));
          if (taskrc)
            return DomeReq::SendSimpleResp(request, 404, SSTR("Cannot remotely stat lfn: '" << lfn << "'"));

          std::string err;
          if (extract_stat(this->getTask(id)->stdout, err, st) <= 0) {
            Err(domelogname, "Failed stating lfn: '" << lfn << "' err: '" << err << "'");
            return DomeReq::SendSimpleResp(request, 404, SSTR("Cannot remotely stat lfn: '" << lfn << "'"));
          }

        }
        else
          return DomeReq::SendSimpleResp(request, 404, SSTR("Cannot stat lfn: '" << lfn << "' err: " << ret.code() << " what: '" << ret.what() << "' and no volatile filesystem matches.") );
      }


  }
  else {
    DmStatus ret;

    // Let's be kind with the client and also accept the rfio syntax
    if ( rfn.size() )  {
      pfn = DomeUtils::pfn_from_rfio_syntax(rfn);
      server = DomeUtils::server_from_rfio_syntax(rfn);
    }

    // If no stat happened so far, we check if we can stat the pfn
    if ( (!server.size() || !pfn.size() ) )
      return DomeReq::SendSimpleResp(request, 422, SSTR("Not enough parameters."));

    // Else we stat the replica, recomposing the rfioname (sob)

      rfn = server + ":" + pfn;

      {
        DomeMySql sql;
        ret = sql.getStatbyRFN(st, rfn);
      }
      if (ret.code() != DMLITE_SUCCESS) {
        return DomeReq::SendSimpleResp(request, 404, SSTR("Cannot stat server: '" << server << "' pfn: '" << pfn << "' err: " << ret.code() << " what: '" << ret.what() << "'"));
      }
  }

  boost::property_tree::ptree jresp;
  xstat_to_ptree(st, jresp);

  return DomeReq::SendSimpleResp(request, 200, jresp);

}






/// Fecthes replica info from its rfn
int DomeCore::dome_getreplicainfo(DomeReq &req, FCGX_Request &request) {
  if (status.role != status.roleHead) {
    return DomeReq::SendSimpleResp(request, DOME_HTTP_BAD_REQUEST, "dome_getstatinfo only available on head nodes.");
  }

  std::string rfn =  req.bodyfields.get<std::string>("rfn", "");

  Log(Logger::Lvl4, domelogmask, domelogname, " rfn: '" << rfn << "'");

  struct dmlite::Replica r;


    DmStatus ret;

    if ( !rfn.size() )  {
      return DomeReq::SendSimpleResp(request, 422, SSTR("Empty rfn"));
    }

    {
      DomeMySql sql;
      ret = sql.getReplicabyRFN(r, rfn);
    }
    if (ret.code() != DMLITE_SUCCESS) {
      return DomeReq::SendSimpleResp(request, 404, SSTR("Cannot stat rfn: '" << rfn << "' err: " << ret.code() << " what: '" << ret.what() << "'"));
    }


  boost::property_tree::ptree jresp;

  jresp.put("replicaid", r.replicaid);
  jresp.put("fileid", r.fileid);
  jresp.put("nbaccesses", r.nbaccesses);
  jresp.put("atime", r.atime);
  jresp.put("ptime", r.ptime);
  jresp.put("ltime", r.ltime);
  jresp.put("status", r.status);
  jresp.put("type", r.type);
  jresp.put("server", r.server);
  jresp.put("rfn", rfn);
  jresp.put("setname", r.setname);
  jresp.put("xattrs", r.serialize());

  return DomeReq::SendSimpleResp(request, 200, jresp);

}





/// Like an HTTP GET on a directory, gets all the content
int DomeCore::dome_getdir(DomeReq &req, FCGX_Request &request) {
  if (status.role != status.roleHead) {
    return DomeReq::SendSimpleResp(request, 500, "dome_getdir only available on head nodes.");
  }

  std::string path = req.bodyfields.get<std::string>("path", "");
  bool statentries = req.bodyfields.get<bool>("statentries", false);

  if (!path.size()) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Cannot list an empty path"));
  }

  DmlitePoolHandler stack(status.dmpool);
  boost::property_tree::ptree jresp, jresp2;

  Directory *d;
  try {

    d = stack->getCatalog()->openDir(path);
    stack->getCatalog()->changeDir(path);

    struct dirent *dent;
    while ( (dent = stack->getCatalog()->readDir(d)) ) {
      boost::property_tree::ptree pt;
      pt.put("name", dent->d_name);
      pt.put("type", dent->d_type);

      if (statentries) {
        struct dmlite::ExtendedStat st = stack->getCatalog()->extendedStat(dent->d_name);
        xstat_to_ptree(st, pt);
      }

      jresp2.push_back(std::make_pair("", pt));
    }
  }
  catch (DmException e) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Unable to get directory content: '" << path << "' err: " << e.code() << " what: '" << e.what() << "'"));
  }

  jresp.push_back(std::make_pair("entries", jresp2));
  return DomeReq::SendSimpleResp(request, 200, jresp);

}

/// Get information about a user
int DomeCore::dome_getuser(DomeReq &req, FCGX_Request &request) {
  if (status.role != status.roleHead) {
    return DomeReq::SendSimpleResp(request, DOME_HTTP_BAD_REQUEST, "dome_getuser only available on head nodes.");
  }

  std::string username = req.bodyfields.get<std::string>("username", "");
  if (!username.size()) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Username not specified"));
  }

  DmlitePoolHandler stack(status.dmpool);
  boost::property_tree::ptree jresp;

  try {
    UserInfo userinfo = stack->getAuthn()->getUser(username);
    boost::property_tree::ptree pt;
    pt.put("uid", userinfo.getLong("uid"));
    pt.put("banned", userinfo.getLong("banned"));
    return DomeReq::SendSimpleResp(request, 200, pt);
  }
  catch (DmException e) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Unable to get user info: '" << username << "' err: " << e.code() << " what: '" << e.what()));
  }
}

/// Get id mapping
int DomeCore::dome_getidmap(DomeReq &req, FCGX_Request &request) {
  if (status.role != status.roleHead) {
    return DomeReq::SendSimpleResp(request, 500, "dome_getidmap only available on head nodes.");
  }

  using namespace boost::property_tree;

  try {
    std::string username = req.bodyfields.get<std::string>("username");
    std::vector<std::string> groupnames;

    boost::optional<ptree&> groups_in = req.bodyfields.get_child_optional("groupnames");
    if(groups_in) {
      for(ptree::const_iterator it = groups_in->begin(); it != groups_in->end(); it++) {
        groupnames.push_back(it->second.get_value<std::string>());
      }
    }

    UserInfo userinfo;
    std::vector<GroupInfo> groupinfo;

    DmlitePoolHandler stack(status.dmpool);
    stack->getAuthn()->getIdMap(username, groupnames, &userinfo, &groupinfo);

    ptree resp;
    resp.put("uid", userinfo.getLong("uid"));
    resp.put("banned", userinfo.getLong("banned"));

    for(std::vector<GroupInfo>::iterator it = groupinfo.begin(); it != groupinfo.end(); it++) {
      resp.put(boost::property_tree::ptree::path_type("groups^" + it->name + "^gid", '^'), it->getLong("gid"));
      resp.put(boost::property_tree::ptree::path_type("groups^" + it->name + "^banned", '^'), it->getLong("banned"));
    }

    return DomeReq::SendSimpleResp(request, 200, resp);
  }
  catch(ptree_error e) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Error while parsing json body: " << e.what()));
  }
  catch(DmException e) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Unable to get id mapping: '" << e.code() << " what: '" << e.what()));
  }
}

int DomeCore::dome_updatexattr(DomeReq &req, FCGX_Request &request) {
  if(status.role != status.roleHead) {
    return DomeReq::SendSimpleResp(request, 500, "dome_updatexattr only available on head nodes.");
  }

  using namespace boost::property_tree;

  try {
    std::string lfn = req.bodyfields.get<std::string>("lfn");
    std::string xattr = req.bodyfields.get<std::string>("xattr");
    DmlitePoolHandler stack(status.dmpool);

    dmlite::Extensible e;
    e.deserialize(xattr);

    stack->getCatalog()->updateExtendedAttributes(lfn, e);
    return DomeReq::SendSimpleResp(request, 200,  "");
  }
  catch(ptree_error &e) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Error while parsing json body: " << e.what()));
  }
  catch(DmException &e) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Unable to update xattr: '" << e.code() << " what: '" << e.what()));
  }
}
