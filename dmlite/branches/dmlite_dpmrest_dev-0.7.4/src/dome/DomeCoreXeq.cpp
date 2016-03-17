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

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/optional/optional.hpp>

#include "cpp/authn.h"
#include "cpp/dmlite.h"
#include "cpp/catalog.h"
#include "cpp/utils/urls.h"
#include "utils/checksums.h"
#include "utils/DavixPool.h"




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
  std::string path;
  if ( filepath[0] != '/' )
    path = catalog->getWorkingDir() + "/" + filepath;
  
  if ( path[0] != '/' )
    path.insert(0, "/");
  
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. Absolute path: path");
  
  std::vector<std::string> components = Url::splitPath(path);
  std::vector<std::string> todo;
  std::string name;
  bool parentok = false;
  
  components.pop_back();
  
  // Make sure that all the parent dirs exist
  do {
    
    std::string ppath = Url::joinPath(components);
    ExtendedStat st;
    
    // Try to get the stat of the parent
    try {
      st = catalog->extendedStat(ppath);
      if (!parentok) {
        parentstat = st;
        parentpath = ppath;
        // This means that we successfully processed at least the parent dir
        // that we have to return
        parentok = true;
        break;
      }
      
      
    } catch (DmException e) {
      // No parent means that we have to create it later
      name = components.back();
      components.pop_back();
      
      todo.push_back(ppath);
    }
    
  } while ( !components.empty() );
  
  
  // Here we have a todo list of directories that we have to create
  // .... so we do it
  while (!todo.empty()) {
    std::string p = todo.back();
    todo.pop_back();
    
    // Try to get the stat of the parent
    try {
      catalog->makeDir(p, 0664);
    } catch (DmException e) {
      // If we can't create the dir then this is a serious error, unless it already exists
      Err(domelogname, "Cannot create path '" << p << "' err: " << e.code() << "-" << e.what());
      if (e.code() != EEXIST) throw;
    }
    
    
  }
  
  // If a miracle took us here, we only miss to create the final file
  try {
    catalog->create(filepath, 0664);
    statinfo = catalog->extendedStat(filepath);
  } catch (DmException e) {
    // If we can't create the file then this is a serious error
    Err(domelogname, "Cannot create file '" << filepath << "'");
    throw;
  }
 
  return 0;
}

int DomeCore::dome_put(DomeReq &req, FCGX_Request &request, struct DomeFsInfo *dest, std::string *destrfn) {
  
  // fetch the parameters, lfn and placement suggestions
  std::string lfn = req.object;
  std::string addreplica_ = req.bodyfields.get<std::string>("additionalreplica", "");
  std::string pool = req.bodyfields.get<std::string>("pool", "");
  std::string host = req.bodyfields.get<std::string>("host", "");
  std::string fs = req.bodyfields.get<std::string>("fs", "");

  bool addreplica = false;
  if ( (addreplica_ == "yes") || (addreplica_ == "1") || (addreplica_ == "on") )
    addreplica = true;
  
  // Log the parameters, level 1
  Log(Logger::Lvl1, domelogmask, domelogname, "Entering. lfn: '" << lfn <<
    "' addreplica: " << addreplica << " pool: '" << pool <<
    "' host: '" << host << "' fs: '" << fs << "'");
  
  if(!req.remoteclientdn.size() || !req.remoteclienthost.size()) {
    return DomeReq::SendSimpleResp(request, 501, "Invalid remote client or remote host credentials");
  }
  
  // Give errors for combinations of the parameters that are obviously wrong
  if ( (host != "") && (pool != "") ) {
    // Error! Log it as such!, level1
    return DomeReq::SendSimpleResp(request, 501, "The pool hint and the host hint are mutually exclusive.");
  }
 
  std::vector<DomeFsInfo> selectedfss;
  {
    // Lock!
    boost::unique_lock<boost::recursive_mutex> l(status);
    
    // Build a list of the filesystems that match the suggestions
    // Loop on the filesystems and take the ones that match
    // The filesystems must be writable and working

    for (unsigned int i = 0; i < status.fslist.size(); i++) {
      if ( (pool.size() > 0) && (status.fslist[i].poolname == pool) ) {
        
        // Take only pools that are associated to the lfn parent dirs
        if ( status.LfnMatchesPool(lfn, status.fslist[i].poolname) && status.fslist[i].isGoodForWrite() ) 
          selectedfss.push_back(status.fslist[i]);
        
        continue;
      }
      
      if ( (host.size() > 0) && (fs.size() == 0) && (status.fslist[i].server == host) ) {
        
        // Take only pools that are associated to the lfn parent dirs
        if ( status.LfnMatchesPool(lfn, status.fslist[i].poolname) && status.fslist[i].isGoodForWrite() )
          selectedfss.push_back(status.fslist[i]);
        
        continue;
      }
      
      if ( (host.size() > 0) && (fs.size() > 0) && (status.fslist[i].server == host) && (status.fslist[i].fs == fs) ) {
        
        // Take only pools that are associated to the lfn parent dirs through a quotatoken
        if ( status.LfnMatchesPool(lfn, status.fslist[i].poolname) && status.fslist[i].isGoodForWrite() )
          selectedfss.push_back(status.fslist[i]);
        
        continue;
      }
      
      // No hints matched because there a re no hintss. Add the filesystem if its path is not empty
      // and matches the put path
      if ( !host.size() && !fs.size() )
        if ( status.LfnMatchesPool(lfn, status.fslist[i].poolname) && status.fslist[i].isGoodForWrite() )
          selectedfss.push_back(status.fslist[i]);
        
    }
    
    
  } // end lock
    
    // If no filesystems matched, return error "no filesystems match the given logical path and placement hints"
    if ( !selectedfss.size() ) {
      // Error!
      return DomeReq::SendSimpleResp(request, 400, "No filesystems match the given logical path and placement hints. HINT: make sure that the correct pools are associated to the LFN, and that they are writable and online.");
    }
    
    // Remove the filesystems that have less then the minimum free space available
    for (int i = selectedfss.size()-1; i >= 0; i--) {
      if (selectedfss[i].freespace / 1024 / 1024 < CFG->GetLong("glb.put.minfreespace_mb", 1024*4)) {// default is 4GB
        Log(Logger::Lvl2, domelogmask, domelogname, "Filesystem: '" << selectedfss[i].server << ":" << selectedfss[i].fs <<
          "' has less than " << CFG->GetLong("glb.put.minfreespace_mb", 1024*4) << "MB free");
        selectedfss.erase(selectedfss.begin()+i);
      }
    }
    
    // If no filesystems remain, return error "filesystems full for path ..."
    if ( !selectedfss.size() ) {
      // Error!
      return DomeReq::SendSimpleResp(request, 400, "All matching filesystems are full.");
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
      return DomeReq::SendSimpleResp(request, 422, os);      
    }
    
    
    std::string pfn = selectedfss[fspos].fs + "/" + vecurl[4] + "/" + timestr + "/" + *vecurl.rbegin() + suffix;
    
    Log(Logger::Lvl4, domelogmask, domelogname, "lfn: '" << lfn << "' --> '" << selectedfss[fspos].server << ":" << pfn << "'");
    
    // NOTE: differently from the historical dpmd, here we do not create the remote path/file
    // of the replica in the disk. We jsut make sure that the LFN exists
    // The replica in the catalog instead is created here
    
    // Create the logical catalog entry, if not already present. We also create the parent dirs
    // if they are absent
    
    DmlitePoolHandler stack(dmpool);
    ExtendedStat parentstat, lfnstat;
    std::string parentpath;
    
    {
      // Security credentials are mandatory, and they have to carry the identity of the remote client
      SecurityCredentials cred;
      cred.clientName = (std::string)req.remoteclientdn;
      cred.remoteAddress = req.remoteclienthost;
      
      try {
        stack->setSecurityCredentials(cred);
      } catch (DmException e) {
        std::ostringstream os;
        os << "Cannot set security credentials. dn: '" << req.remoteclientdn << "' addr: '" <<
          req.remoteclienthost << "' - " << e.code() << "-" << e.what();
          
        Err(domelogname, os.str());
        return DomeReq::SendSimpleResp(request, 422, os);
      }
    }

    try {
      mkdirminuspandcreate(stack->getCatalog(), lfn, parentpath, parentstat, lfnstat);
    } catch (DmException e) {
      std::ostringstream os;
      os << "Cannot create logical directories for '" << lfn << "' : " << e.code() << "-" << e.what();
      
      Err(domelogname, os.str());
      return DomeReq::SendSimpleResp(request, 422, os);
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
    try {
      stack->getCatalog()->addReplica(r);
    } catch (DmException e) {
      std::ostringstream os;
      os << "Cannot create replica '" << r.rfn << "' for '" << lfn << "' : " << e.code() << "-" << e.what();
      
      Err(domelogname, os.str());
      return DomeReq::SendSimpleResp(request, 501, os);
    }
    
  // Here we are assuming that some frontend will soon start to write a new replica
  //  with the name we chose here
    
  // Return the response
    
  // This function may have been invoked to know
  // details about the placement without telling the client
  if (dest && destrfn) {
    *dest = selectedfss[fspos];
    *destrfn = r.rfn;
    return 0;
  }
  
  std::ostringstream os;
  boost::property_tree::ptree jresp;
  jresp.put("pool", selectedfss[fspos].poolname);
  jresp.put("host", selectedfss[fspos].server);
  jresp.put("filesystem", selectedfss[fspos].fs);
  jresp.put("pfn", pfn);

  boost::property_tree::write_json(os, jresp);
  int rc = DomeReq::SendSimpleResp(request, 200, os);

  Log(Logger::Lvl3, domelogmask, domelogname, "Result: " << rc);
  return rc;
};

int DomeCore::dome_putdone_disk(DomeReq &req, FCGX_Request &request) {
  
  
  // The command takes as input server and pfn, separated
  // in order to put some distance from rfio concepts, at least in the api
  std::string server = req.bodyfields.get<std::string>("server", "");
  std::string pfn = req.bodyfields.get<std::string>("pfn", "");
  
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

  // Please note that the server field can be empty
  
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
    return DomeReq::SendSimpleResp(request, 404, os);
  }
  
  Log(Logger::Lvl2, domelogmask, domelogname, " pfn: '" << pfn << "' "
    " disksize: " << st.st_size);
  
  if (size == 0) size = st.st_size;
  
  if ( (unsigned)size != st.st_size ) {
    std::ostringstream os;
    os << "Reported size (" << size << ") does not match with the size of the file (" << st.st_size << ")";
    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, 422, os);
  }
  
  // Now forward the request to the head node
  Log(Logger::Lvl1, domelogmask, domelogname, " Forwarding to headnode. server: '" << server << "' pfn: '" << pfn << "' "
    " size: " << size << " cksumt: '" << chktype << "' cksumv: '" << chkval << "'" );
  
  std::string domeurl = CFG->GetString("disk.headnode.domeurl", (char *)"(empty url)/") + req.object;
  Davix::Uri url(domeurl);

  Davix::DavixError* tmp_err = NULL;
  DavixGrabber hdavix(*davixPool);
  DavixStuff *ds(hdavix);
  
  Davix::PostRequest req2(*(ds->ctx), url, &tmp_err);
  if( tmp_err ) {
    std::ostringstream os;
    os << "Cannot initialize Davix query to" << url << ", Error: "<< tmp_err->getErrMsg();
    Err(domelogname, os.str());
    Davix::DavixError::clearError(&tmp_err);
    return DomeReq::SendSimpleResp(request, 500, os);
  } 
  
  req2.addHeaderField("cmd", "dome_putdone");
  req2.addHeaderField("remoteclientdn", req.remoteclientdn);
  req2.addHeaderField("remoteclientaddr", req.remoteclienthost);
  
  // Copy the same body fields as the original one, except for some fields,
  // where we write this machine's hostname (we are a disk server here) and the validated size
  req.bodyfields.put("server", status.myhostname);
  req.bodyfields.put("size", size);
  std::ostringstream os;
  boost::property_tree::write_json(os, req.bodyfields);
  req2.setRequestBody(os.str());
      
  // Set the dome timeout values for the operation
  req2.setParameters(*(ds->parms));
      
  if (req2.executeRequest(&tmp_err) != 0) {
    // The error must be propagated to the response, in clear readable text
    std::ostringstream os;
    int errcode = req2.getRequestCode();
    if (tmp_err)
      os << "Cannot forward cmd_putdone to head node. errcode: " << errcode << "'"<< tmp_err->getErrMsg() << "'";
    else
      os << "Cannot forward cmd_putdone to head node. errcode: " << errcode;
    
    Err(domelogname, os.str());
    
    Davix::DavixError::clearError(&tmp_err);
    return DomeReq::SendSimpleResp(request, 500, os);
  }
  
  // The request has been successfully forwarded to the headnode
  os.clear();
  if (req2.getAnswerContent()) os << req2.getAnswerContent();
  return DomeReq::SendSimpleResp(request, 200, os);
  
      
}

int DomeCore::dome_putdone_head(DomeReq &req, FCGX_Request &request) {
  
  
  DmlitePoolHandler stack(dmpool);
  INode *inodeintf = dynamic_cast<INode *>(stack->getINode());
  if (!inodeintf) {
    Err( domelogname , " Cannot retrieve inode interface. Fatal.");
    return -1;
  }
  
  // The command takes as input server and pfn, separated
  // in order to put some distance from rfio concepts, at least in the api
  std::string server = req.bodyfields.get<std::string>("server", "");
  std::string pfn = req.bodyfields.get<std::string>("pfn", "");
  
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
  if ( size <= 0 ) {
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
    std::string domeurl = CFG->GetString("disk.headnode.domeurl", (char *)"") + req.object;
    Davix::Uri url(domeurl);

    Davix::DavixError* tmp_err = NULL;
    DavixGrabber hdavix(*davixPool);
    DavixStuff *ds(hdavix);
  
    Davix::PostRequest req2(*(ds->ctx), url, &tmp_err);
    if( tmp_err ) {
      std::ostringstream os;
      os << "Cannot initialize Davix query to" << url << ", Error: "<< tmp_err->getErrMsg();
      Err(domelogname, os.str());
      Davix::DavixError::clearError(&tmp_err);
      return DomeReq::SendSimpleResp(request, 500, os);
    } 
  
    req2.addHeaderField("cmd", "dome_stat");
    req2.addHeaderField("remoteclientdn", req.remoteclientdn);
    req2.addHeaderField("remoteclientaddr", req.remoteclienthost);
  
    std::ostringstream os;
    boost::property_tree::ptree jstat;
    jstat.put("pfn", pfn);
    boost::property_tree::write_json(os, jstat);
    req2.setRequestBody(os.str());
    
    // Set the dome timeout values for the operation
    req2.setParameters(*(ds->parms));
      
    if (req2.executeRequest(&tmp_err) != 0) {
      // The error must be propagated to the response, in clear readable text
      std::ostringstream os;
      int errcode = req2.getRequestCode();
      if (tmp_err)
        os << "Cannot remote stat pfn: '" << server << ":" << pfn << "'. errcode: " << errcode << "'"<< tmp_err->getErrMsg() << "'";
      else
        os << "Cannot remote stat pfn: '" << server << ":" << pfn << "'. errcode: " << errcode;
    
      Err(domelogname, os.str());
    
      Davix::DavixError::clearError(&tmp_err);
      return DomeReq::SendSimpleResp(request, errcode, os);
    }
    
    if (!req2.getAnswerContent()) {
      std::ostringstream os;
      os << "Cannot remote stat pfn: '" << server << ":" << pfn << "'. null response.";
      Err(domelogname, os.str());
      return DomeReq::SendSimpleResp(request, 404, os);
    }
    
    // The stat towards the dsk server was successful. Take the size
    jstat.clear();
    std::istringstream s(req2.getAnswerContent());
    try {
      boost::property_tree::read_json(s, jstat);
    } catch (boost::property_tree::json_parser_error e) {
      Err("takeJSONbodyfields", "Could not process JSON: " << e.what() << " '" << s.str() << "'");
      return -1;
    }
    
    size = jstat.get<size_t>("size", 0L);

    
  } // if size == 0

  
  // -------------------------------------------------------
  // If a miracle took us here, the size has been confirmed
  Log(Logger::Lvl1, domelogmask, domelogname, " Final size:   " << size );
    
  
  
  
  
  
  
  
  
  
  
  
  
  
  // Update the replica values, including the checksum
  rep.ptime = rep.ltime = rep.atime = time(0);
  rep.status = dmlite::Replica::kAvailable;
  rep[chktype] = chkval;
  
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
    
    off_t sz = st.stat.st_size;
    
    
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
      for (int i = MAX(0, idx-3); i >= MAX(0, idx-1-CFG->GetLong("glb.dirspacereportdepth", 6)); i--) {
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
  
  
  
  
  
  int rc = DomeReq::SendSimpleResp(request, 200, SSTR("dome_putdone successful."));

  Log(Logger::Lvl3, domelogmask, domelogname, "Result: " << rc);
  return rc;
};

int DomeCore::dome_getspaceinfo(DomeReq &req, FCGX_Request &request) {
  int rc = 0;
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering");


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

      poolname = "poolinfo^" + status.fslist[i].poolname + "^fsinfo^" + status.fslist[i].server + "^" + status.fslist[i].fs;

      jresp.put(boost::property_tree::ptree::path_type(poolname+"^fsstatus", '^'), status.fslist[i].status);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^freespace", '^'), status.fslist[i].freespace);
      jresp.put(boost::property_tree::ptree::path_type(poolname+"^physicalsize", '^'), status.fslist[i].physicalsize);
    }
  }


  std::ostringstream os;
  os << "Content-type: text\r\n\r\n";
  boost::property_tree::write_json(os, jresp);

  FCGX_PutS(os.str().c_str(), request.out);

  Log(Logger::Lvl3, domelogmask, domelogname, "Result: " << rc);
  return rc;
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
  qualifiers.push_back(req.remoteclientdn);
  qualifiers.push_back(req.remoteclienthost);

  status.checksumq->touchItemOrCreateNew(namekey, qstatus, 0, qualifiers);
  
  return DomeReq::SendSimpleResp(request, 202, SSTR("Enqueued checksum calculation for server " << replica.server
                                                     << ", path " << DomeUtils::pfn_from_rfio_syntax(replica.rfn)
                                                     << ", check back later.\r\nTotal checksums in queue right now: "
                                                     << status.checksumq->nTotal()));
}



int DomeCore::enqfilepull(DomeReq &req, FCGX_Request &request, std::string lfn) {
  
  // This simple implementation is like a put
  DomeFsInfo destfs;
  std::string destrfn;
  int rc = dome_put(req, request, &destfs, &destrfn);
  if (rc != 0)
    return rc; // means that a response has already been sent in the context of dome_put, btw it can only be an error
  
  // create queue entry
  GenPrioQueueItem::QStatus qstatus = GenPrioQueueItem::Waiting;
  std::string namekey = lfn;
  std::vector<std::string> qualifiers;

  qualifiers.push_back(""); // the first qualifier is common for all items,
                            // so the global limit triggers

  qualifiers.push_back(lfn); // lfn as second qualifier
  qualifiers.push_back(destfs.server);
  qualifiers.push_back(destfs.fs);
  
  // necessary information to keep - order is important
  
  qualifiers.push_back(destrfn);
  qualifiers.push_back(req.remoteclientdn);
  qualifiers.push_back(req.remoteclienthost);

  status.filepullq->touchItemOrCreateNew(namekey, qstatus, 0, qualifiers);
  
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

int DomeCore::dome_chksum(DomeReq &req, FCGX_Request &request) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering");
  if(status.role == status.roleDisk) {
    return DomeReq::SendSimpleResp(request, 500, "chksum only available on head nodes");
  }

  try {
    DmlitePoolHandler stack(dmpool);

    std::string chksumtype = req.bodyfields.get<std::string>("checksum-type", "");
    std::string fullchecksum = "checksum." + chksumtype;
    std::string pfn = req.bodyfields.get<std::string>("pfn", "");
    bool forcerecalc = DomeUtils::str_to_bool(req.bodyfields.get<std::string>("force-recalc", "false"));
    bool updateLfnChecksum = (pfn == "");

    if(chksumtype == "") {
      return DomeReq::SendSimpleResp(request, 422, "checksum-type cannot be empty.");
    }

    if(forcerecalc) {
      Replica replica = pickReplica(req.object, pfn, stack);
      return calculateChecksum(req, request, req.object, replica, chksumtype, updateLfnChecksum);
    }

    // Not forced to do a recalc - maybe I can find the checksums in the db
    std::string lfnchecksum;
    std::string pfnchecksum;
    Replica replica;

    // retrieve lfn checksum
    ExtendedStat xstat = stack->getCatalog()->extendedStat(req.object);
    if(xstat.hasField(fullchecksum)) {
      lfnchecksum = xstat.getString(fullchecksum);
      Log(Logger::Lvl3, domelogmask, domelogname, "Found lfn checksum in the db: " << lfnchecksum);
    }
    else {
      Log(Logger::Lvl3, domelogmask, domelogname, "Lfn checksum not in the db");
    }

    // retrieve pfn checksum
    if(pfn != "") {
      replica = pickReplica(req.object, pfn, stack);
      if(replica.hasField(fullchecksum)) {
        pfnchecksum = replica.getString(fullchecksum);
        Log(Logger::Lvl3, domelogmask, domelogname, "Found pfn checksum in the db: " << pfnchecksum);
      }
    }

    // can I send a response right now? Of course, sir !
    if(lfnchecksum != "" && (pfn == "" || pfnchecksum != "")) {
      boost::property_tree::ptree jresp;
      jresp.put("checksum", lfnchecksum);
      if(pfn != "") {
        jresp.put("pfn-checksum", pfnchecksum);
      }
      return DomeReq::SendSimpleResp(request, 200, jresp);
    }

    // something is missing, need to calculate
    if(pfn == "") {
      replica = pickReplica(req.object, pfn, stack);
    }

    return calculateChecksum(req, request, req.object, replica, chksumtype, updateLfnChecksum);
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
    DmlitePoolHandler stack(dmpool);

    std::string chksumtype = req.bodyfields.get<std::string>("checksum-type", "");
    std::string fullchecksum = "checksum." + chksumtype;
    std::string pfn = req.bodyfields.get<std::string>("pfn", "");
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
    std::string namekey = req.object + "[#]" + pfn + "[#]" + chksumtype;
    std::vector<std::string> qualifiers;

    Url u(pfn);
    std::string server = u.domain;

    qualifiers.push_back("");
    qualifiers.push_back(server);
    qualifiers.push_back(DomeUtils::bool_to_str(updateLfnChecksum));
    status.checksumq->touchItemOrCreateNew(namekey, qstatus, 0, qualifiers);

    if(str_status == "aborted") {
      Log(Logger::Lvl1, domelogmask, domelogname, "Checksum calculation failed. LFN: " << req.object
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
    Replica replica = pickReplica(req.object, pfn, stack);
    replica[fullchecksum] = checksum;
    stack->getCatalog()->updateReplica(replica);

    // replace lfn checksum?
    if(updateLfnChecksum) {
      stack->getCatalog()->setChecksum(req.object, fullchecksum, checksum);
    }
    // still update if it's empty, though
    else {
      ExtendedStat xstat = stack->getCatalog()->extendedStat(req.object);
      if(!xstat.hasField(fullchecksum)) {
        stack->getCatalog()->setChecksum(req.object, fullchecksum, checksum);
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
    //DmlitePoolHandler stack(dmpool);

    std::string chksumtype = req.bodyfields.get<std::string>("checksum-type", "");
    std::string pfn = req.bodyfields.get<std::string>("pfn", "");
    std::string lfn = req.object;
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

    PendingChecksum pending(lfn, status.myhostname, pfn, chksumtype, updateLfnChecksum);

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

  std::string domeurl = CFG->GetString("disk.headnode.domeurl", (char *)"(empty url)/") + pending.lfn;
  Log(Logger::Lvl4, domelogmask, domelogname, domeurl);
  Davix::Uri url(domeurl);

  Davix::DavixError* err = NULL;
  DavixGrabber hdavix(*davixPool);
  DavixStuff *ds(hdavix);
  Davix::PostRequest req2(*(ds->ctx), url, &err);

  if(err) {
    std::string msg = SSTR("Cannot initialize davix query to " << url << ", error: " << err->getErrMsg());
    Err(domelogname, msg);
    return;
  }

  req2.addHeaderField("cmd", "dome_pullstatus");
  req2.addHeaderField("remoteclientdn", pending.remoteclientdn);
  req2.addHeaderField("remoteclienthost", pending.remoteclienthost);

  // set chksumstatus params
  boost::property_tree::ptree jresp;

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

  std::ostringstream os;
  boost::property_tree::write_json(os, jresp);
  req2.setRequestBody(os.str());

  req2.setParameters(*(ds->parms));
  req2.executeRequest(&err);

  if(err) {
    Err(domelogname, "Error after sending a dome_pullstatus: " << err->getErrMsg());
    Davix::DavixError::clearError(&err);
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

  std::string domeurl = CFG->GetString("disk.headnode.domeurl", (char *)"(empty url)/") + pending.lfn;
  Log(Logger::Lvl4, domelogmask, domelogname, domeurl);
  Davix::Uri url(domeurl);

  Davix::DavixError* err = NULL;
  DavixGrabber hdavix(*davixPool);
  DavixStuff *ds(hdavix);
  Davix::PostRequest req2(*(ds->ctx), url, &err);

  if(err) {
    std::string msg = SSTR("Cannot initialize davix query to " << url << ", error: " << err->getErrMsg());
    Err(domelogname, msg);
    return;
  }

  req2.addHeaderField("cmd", "dome_chksumstatus");
  req2.addHeaderField("remoteclientdn", pending.remoteclientdn);
  req2.addHeaderField("remoteclienthost", pending.remoteclienthost);

  // set chksumstatus params
  boost::property_tree::ptree jresp;
  std::string rfn = pending.server + ":" + pending.pfn;

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

  std::ostringstream os;
  boost::property_tree::write_json(os, jresp);
  req2.setRequestBody(os.str());

  req2.setParameters(*(ds->parms));
  req2.executeRequest(&err);

  if(err) {
    Err(domelogname, "Error after sending a dome_chksumstatus: " << err->getErrMsg());
    Davix::DavixError::clearError(&err);
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


  std::ostringstream os;
  boost::property_tree::write_json(os, jresp);
  rc = DomeReq::SendSimpleResp(request, 200, os);


  Log(Logger::Lvl3, domelogmask, domelogname, "Result: " << rc);
  return rc;



};

int DomeCore::dome_getdirspaces(DomeReq &req, FCGX_Request &request) {

  // Crawl upwards the directory hierarchy of the given path
  // stopping when a matching one is found
  // The result is a list, as more than one quotatoken can be
  // assigned to a directory
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
  long long poolfree = 0LL;
  // Crawl
  {
    boost::unique_lock<boost::recursive_mutex> l(status);
    while (absPath.length() > 0) {

      Log(Logger::Lvl4, domelogmask, domelogname, "Processing: '" << absPath << "'");
      // Check if any matching quotatoken exists
      std::pair <std::multimap<std::string, DomeQuotatoken>::iterator, std::multimap<std::string, DomeQuotatoken>::iterator> myintv;
      myintv = status.quotas.equal_range(absPath);

      if (myintv.first != status.quotas.end()) {
        for (std::multimap<std::string, DomeQuotatoken>::iterator it = myintv.first; it != myintv.second; ++it) {
          totspace += it->second.t_space;

          // Now find the free space in the mentioned pool
          long long ptot, pfree;
          int poolst;
          status.getPoolSpaces(it->second.poolname, ptot, pfree, poolst);
          poolfree += pfree;

          Log(Logger::Lvl1, domelogmask, domelogname, "Quotatoken '" << it->second.u_token << "' of pool: '" <<
          it->second.poolname << "' matches path '" << absPath << "' totspace: " << totspace);
        }


        // Now get the size of this directory, using the dmlite catalog
        usedspace = 0;
        DmlitePoolHandler stack(dmpool);
        try {
          struct dmlite::ExtendedStat st = stack->getCatalog()->extendedStat(absPath);
          usedspace = st.stat.st_size;
        }
        catch (dmlite::DmException e) {
          Err(domelogname, "Ignore exception stat-ing '" << absPath << "'");
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
  jresp.put("poolfreespace", poolfree);
  jresp.put("usedspace", usedspace);

  std::ostringstream os;
  boost::property_tree::write_json(os, jresp);
  int rc = DomeReq::SendSimpleResp(request, 200, os);


  Log(Logger::Lvl3, domelogmask, domelogname, "Result: " << rc);
  return rc;

}


int DomeCore::dome_get(DomeReq &req, FCGX_Request &request)  {
  // Currently just returns a list of all replicas

  Log(Logger::Lvl4, domelogmask, domelogname, "Entering");
  bool canpull = false;
  DmlitePoolHandler stack(dmpool);
  try {
    std::vector<Replica> replicas = stack->getCatalog()->getReplicas(req.object);
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
        continue;
      }
      
      found = true;
      jresp.put(ptree::path_type(SSTR(i << "^server"), '^'), replicas[i].server);
      jresp.put(ptree::path_type(SSTR(i << "^pfn"), '^'), pfn);
      jresp.put(ptree::path_type(SSTR(i << "^filesystem"), '^'), replicas[i].getString("filesystem"));
    }
    
    

    if (found)
      return DomeReq::SendSimpleResp(request, 200, jresp);
    if (foundpending)
      return DomeReq::SendSimpleResp(request, 202, "Only pending replicas are available. Please come back later.");
  }
  catch (dmlite::DmException e) {
    
    // The lfn does not seemm to exist ? We may have to pull the file from elsewhere
    if (e.code() == ENOENT) {
      
      DomeFsInfo fs;
      canpull = status.LfnMatchesAnyCanPullFS(req.object, fs);
    }
    else
      return DomeReq::SendSimpleResp(request, 500, SSTR("Unable to find replicas for '" << req.object << "'"));
  }
  
  // Here we have to trigger the file pull and tell to the client to come back later
  if (canpull) {
    Log(Logger::Lvl1, domelogmask, domelogname, "Volatile filesystem detected. Seems we can try pulling the file: '" << req.object << "'");
    return enqfilepull(req, request, req.object);
  }
  
  return DomeReq::SendSimpleResp(request, 404, SSTR("No available replicas for '" << req.object << "'"));
  
}

int DomeCore::dome_pullstatus(DomeReq &req, FCGX_Request &request)  {
  if(status.role == status.roleDisk) {
    return DomeReq::SendSimpleResp(request, 500, "pullstatus only available on head nodes");
  }
  
  
  try {
    DmlitePoolHandler stack(dmpool);
    INode *inodeintf = dynamic_cast<INode *>(stack->getINode());
    if (!inodeintf) {
      Err( domelogname , " Cannot retrieve inode interface. Fatal.");
      return -1;
    }
  
    std::string chksumtype = req.bodyfields.get<std::string>("checksum-type", "");
    std::string fullchecksum = "checksum." + chksumtype;
    std::string pfn = req.bodyfields.get<std::string>("pfn", "");
    std::string server = req.bodyfields.get<std::string>("server", "");
    std::string str_status = req.bodyfields.get<std::string>("status", "");
    std::string reason = req.bodyfields.get<std::string>("reason", "");
    std::string checksum = req.bodyfields.get<std::string>("checksum", "");
    size_t size = req.bodyfields.get("filesize", 0L);
    
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
    std::string namekey = req.object;
    std::vector<std::string> qualifiers;

    qualifiers.push_back("");
    qualifiers.push_back(server);
    status.filepullq->touchItemOrCreateNew(namekey, qstatus, 0, qualifiers);

    if(str_status == "aborted") {
      Log(Logger::Lvl1, domelogmask, domelogname, "File pull failed. LFN: " << req.object
      << "PFN: " << pfn << ". Reason: " << reason);
      return DomeReq::SendSimpleResp(request, 200, "");
    }

    if(str_status == "pending") {
      Log(Logger::Lvl2, domelogmask, domelogname, "File pull pending... LFN: " << req.object
      << "PFN: " << pfn << ". Reason: " << reason);
      return DomeReq::SendSimpleResp(request, 200, "");
    }

    // status is done, checksum can be empty
    Log(Logger::Lvl2, domelogmask, domelogname, "File pull finished. LFN: " << req.object
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
      stack->getCatalog()->setSize(rep);
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

  try {
    //DmlitePoolHandler stack(dmpool);
    
    std::string chksumtype = req.bodyfields.get<std::string>("checksum-type", "");
    std::string pfn = req.bodyfields.get<std::string>("pfn", "");
    std::string lfn = req.object;
    
    // Checksumtype in this case can be empty, as it's just a suggestion...
    if(pfn == "") {
      return DomeReq::SendSimpleResp(request, 422, "pfn cannot be empty.");
    }
    if(!status.PfnMatchesAnyFS(status.myhostname, pfn)) {
      return DomeReq::SendSimpleResp(request, 422, "pfn does not match any of the filesystems of this server.");
    }
    if(lfn == "") {
      return DomeReq::SendSimpleResp(request, 422, "lfn cannot be empty.");
    }
    if (!CFG->GetString("disk.filepuller.pullhook", (char *)"").size()) {
      return DomeReq::SendSimpleResp(request, 500, "File puller is disabled.");
    }
    
    // Let's just execute the external hook, passing the obvious parameters
    
    PendingPull pending(lfn, status.myhostname, pfn, chksumtype);

    std::vector<std::string> params;
    params.push_back(CFG->GetString("disk.filepuller.pullhook", (char *)""));
    params.push_back(lfn);
    params.push_back(pfn);
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



int DomeCore::dome_getquotatoken(DomeReq &req, FCGX_Request &request) {
  
  
  // Remove any trailing slash
  std::string absPath = req.object;
  while (absPath[ absPath.size()-1 ] == '/') {
    absPath.erase(absPath.size() - 1);
  }
  
  Log(Logger::Lvl4, domelogmask, domelogname, "Processing: '" << absPath << "'");
  bool getsubdirs = req.bodyfields.get<bool>("getsubdirs", false);
  bool getparentdirs = req.bodyfields.get<bool>("getparentdirs", false);
  
  // Get the used space for this path
  long long pathused = 0LL;
  long long pathfree = 0L;
  DmlitePoolHandler stack(dmpool);
  try {
    struct dmlite::ExtendedStat st = stack->getCatalog()->extendedStat(absPath);
    pathused = st.stat.st_size;
  }
  catch (dmlite::DmException e) {
    std::ostringstream os;
    os << "Cannot find logical path: " << absPath;
    
    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, 404, os); 
    
  }

  // Get the ones that match the object of the query
  
  boost::property_tree::ptree jresp;
  int cnt = 0;
  
  
  for (std::multimap<std::string, DomeQuotatoken>::iterator it = status.quotas.begin(); it != status.quotas.end(); ++it) {
    Log(Logger::Lvl4, domelogmask, domelogname, "Checking: '" << it->second.path << "' versus '" << absPath << "' getparentdirs: " << getparentdirs);
    // If we can find the quotatoken path into the query...
    size_t pos = absPath.find(it->second.path);
    if ( pos == 0 ) {
      
      // If the query is longer than the tk then it's not matching if getsubdirs is false
      if ((absPath.length() > it->second.path.length()) && (!getparentdirs)) continue;
      
      // If the lengths are not the same, then there should be a slash in the right place in the tk for it to be a subdir
      if ( (absPath.length() != it->second.path.length()) && (absPath[it->second.path.length()] != '/') ) continue;
      
      // Now find the free space in the mentioned pool
      long long ptot, pfree;
      int poolst;
      status.getPoolSpaces(it->second.poolname, ptot, pfree, poolst);
      
      pathfree = ( (it->second.t_space - pathused < ptot - pathused) ? it->second.t_space - pathused : ptot - pathused );
      if (pathfree < 0) pathfree = 0;
      
      Log(Logger::Lvl4, domelogmask, domelogname, "Quotatoken '" << it->second.u_token << "' of pool: '" <<
      it->second.poolname << "' matches path '" << absPath << "' quotatktotspace: " << it->second.t_space <<
      " pooltotspace: " << ptot << " pathusedspace: " << pathused << " pathfreespace: " << pathfree );
      
      boost::property_tree::ptree pt;
      pt.put("path", it->second.path);
      pt.put("quotatkname", it->second.u_token);
      pt.put("quotatkpoolname", it->second.poolname);
      pt.put("quotatktotspace", it->second.t_space);
      pt.put("pooltotspace", ptot);
      pt.put("pathusedspace", pathused);
      pt.put("pathfreespace", pathfree);
      
      jresp.push_back(std::make_pair("", pt));
      cnt++;
    } // if
  } // for
  
  
  if (getsubdirs) {
    // Here we want to match abspaths that are shorter than the quotatokens
      for (std::multimap<std::string, DomeQuotatoken>::iterator it = status.quotas.begin(); it != status.quotas.end(); ++it) {
    Log(Logger::Lvl4, domelogmask, domelogname, "Checking: '" << it->second.path << "' versus '" << absPath << "' getsubdirs: " << getsubdirs);
    // If we can find the abspath into the token then we are selecting tokens that are equal or longer
    size_t pos = it->second.path.find(absPath);
    if ( pos == 0 ) {
      
      // If the lengths are not the same, then there should be a slash in the right place in the tk for it to be a subdir
      if ( (absPath.length() != it->second.path.length()) && (it->second.path[absPath.length()] != '/') ) continue;
      
      // Now find the free space in the mentioned pool
      long long ptot, pfree;
      int poolst;
      status.getPoolSpaces(it->second.poolname, ptot, pfree, poolst);
      
      pathfree = ( (it->second.t_space - pathused < ptot - pathused) ? it->second.t_space - pathused : ptot - pathused );
      if (pathfree < 0) pathfree = 0;
      
      Log(Logger::Lvl4, domelogmask, domelogname, "Quotatoken '" << it->second.u_token << "' of pool: '" <<
      it->second.poolname << "' matches path '" << absPath << "' quotatktotspace: " << it->second.t_space <<
      " pooltotspace: " << ptot << " pathusedspace: " << pathused << " pathfreespace: " << pathfree );
      
      boost::property_tree::ptree pt;
      pt.put("path", it->second.path);
      pt.put("quotatkname", it->second.u_token);
      pt.put("quotatkpoolname", it->second.poolname);
      pt.put("quotatktotspace", it->second.t_space);
      pt.put("pooltotspace", ptot);
      pt.put("pathusedspace", pathused);
      pt.put("pathfreespace", pathfree);
      
      jresp.push_back(std::make_pair("", pt));
      cnt++;
    } // if
  } // for
  }
  
  
  
  if (cnt > 0) {
    std::ostringstream os;
    boost::property_tree::write_json(os, jresp);
    return DomeReq::SendSimpleResp(request, 200, os);
  }
  
  return DomeReq::SendSimpleResp(request, 404, SSTR("No quotatokens match path '" << absPath << "'"));
  
};
int DomeCore::dome_setquotatoken(DomeReq &req, FCGX_Request &request) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering.");
  
  DomeQuotatoken mytk;
  
  mytk.path = req.bodyfields.get("path", "");
  mytk.poolname = req.bodyfields.get("poolname", "");
  
  if (!status.existsPool(mytk.poolname)) {
    std::ostringstream os;
    os << "Cannot find pool: '" << mytk.poolname << "'";
    
    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, 404, os); 
    
  }
    
  
  // Remove any trailing slash
  while (mytk.path[ mytk.path.size()-1 ] == '/') {
    mytk.path.erase(mytk.path.size() - 1);
  }
  
  DmlitePoolHandler stack(dmpool);
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
  }
  
  mytk.t_space = req.bodyfields.get("quotaspace", 0LL);
  mytk.u_token = req.bodyfields.get("description", "(unnamed)");
  
  // First we write into the db, if it goes well then we update the internal map
  int rc;
  {
  DomeMySql sql;
  DomeMySqlTrans  t(&sql);
  std::string clientid = req.remoteclientdn;
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
  
  status.insertQuotatoken(mytk);
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
  std::string clientid = req.remoteclientdn;
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
  DmlitePoolHandler stack(dmpool);
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
  {
  std::string diskurl = "https://" + srv + "/domedisk/";
  Log(Logger::Lvl4, domelogmask, domelogname, "Dispatching deletion of replica '" << absPath << "' to disk node: '" << diskurl);
  Davix::Uri durl(diskurl);

  Davix::DavixError* tmp_err = NULL;
  DavixGrabber hdavix(*davixPool);
  DavixStuff *ds(hdavix);
  Davix::PostRequest req2(*(ds->ctx), durl, &tmp_err);
  if( tmp_err ) {
    std::ostringstream os;
    os << "Cannot initialize Davix query to" << durl << ", Error: "<< tmp_err->getErrMsg();
    Err(domelogname, os.str());
    Davix::DavixError::clearError(&tmp_err);
    return DomeReq::SendSimpleResp(request, 500, os);
  }
  req2.addHeaderField("cmd", "dome_pfnrm");
  req2.addHeaderField("pfn", absPath);
  req2.addHeaderField("remoteclientdn", req.remoteclientdn);
  req2.addHeaderField("remoteclientaddr", req.remoteclienthost);
  std::ostringstream os;
  boost::property_tree::write_json(os, req.bodyfields);
  req2.setRequestBody(os.str());
      
  // Set the dome timeout values for the operation
  req2.setParameters(*(ds->parms));
      
  int rc = req2.executeRequest(&tmp_err);
  if ( rc || tmp_err) {
    // The error must be propagated to the response, in clear readable text
    std::ostringstream os;
    int errcode = req2.getRequestCode();
    if (tmp_err)
      os << "Cannot execute cmd_pfnrm to disk node. pfn: '" << absPath << "' Url: '" << durl << "' errcode: " << errcode << "'"<< tmp_err->getErrMsg() << "' response body: '" << req2.getAnswerContent();
    else
      os << "Cannot execute cmd_pfnrm to disk node. pfn: '" << absPath << "' Url: '" << durl << "' errcode: " << errcode << " response body: '" << req2.getAnswerContent();
    
    Err(domelogname, os.str());   
    Davix::DavixError::clearError(&tmp_err);
    return DomeReq::SendSimpleResp(request, 500, os);
  }
  }
  
  Log(Logger::Lvl4, domelogmask, domelogname, "Removing replica: '" << rep.rfn);
  // And now remove the replica
  try {
    stack->getCatalog()->deleteReplica(rep);
  } catch (DmException e) {
    std::ostringstream os;
    os << "Cannot find replica '"<< rfiopath << "' : " << e.code() << "-" << e.what();  
    Err(domelogname, os.str());
    return DomeReq::SendSimpleResp(request, 404, os);
  }
  
  Log(Logger::Lvl4, domelogmask, domelogname, "Check if we have to remove the logical file entry: '" << rep.fileid);
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
    InodeTrans trans(ino);
    
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
        os << "Cannot find replicas for fileid: '"<< rep.fileid << "' : " << e.code() << "-" << e.what();  
        Err(domelogname, os.str());
        //return DomeReq::SendSimpleResp(request, 404, os);
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
  
  status.rmPoolfs(poolname);
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
  if (!pool_stype.size()) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("pool_stype '" << pool_stype << "' is empty."));
  }

  // make sure it doesn't already exist
  for (std::vector<DomeFsInfo>::iterator fs = status.fslist.begin(); fs != status.fslist.end(); fs++) {
    if(fs->poolname == poolname) {
      return DomeReq::SendSimpleResp(request, 422, SSTR("poolname '" << poolname << "' already exists."));
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

  return DomeReq::SendSimpleResp(request, 200, "Pool was created.");
}

/// Adds a filesystem to a pool
int DomeCore::dome_addfstopool(DomeReq &req, FCGX_Request &request) {
  if (status.role != status.roleHead) {
    return DomeReq::SendSimpleResp(request, 500, "dome_addfstopool only available on head nodes.");
  }
    
  std::string poolname =  req.bodyfields.get<std::string>("poolname", "");
  std::string server =  req.bodyfields.get<std::string>("server", "");
  std::string newfs =  req.bodyfields.get<std::string>("fs", "");
  long pool_defsize = req.bodyfields.get("pool_defsize", 3L * 1024 * 1024 * 1024);
  char pool_stype = req.bodyfields.get("pool_stype", 'P');
  
  Log(Logger::Lvl4, domelogmask, domelogname, " poolname: '" << poolname << "'");
  
  if (!poolname.size()) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("poolname '" << poolname << "' is empty."));
  }


  // Make sure it's not already there or that we are not adding a parent/child of an existing fs
  for (std::vector<DomeFsInfo>::iterator fs = status.fslist.begin(); fs != status.fslist.end(); fs++) {
    if ( status.PfnMatchesFS(server, newfs, *fs) )
      return DomeReq::SendSimpleResp(request, 422, SSTR("Filesystem '" << server << ":" << fs->fs << "' already exists or overlaps an existing filesystem."));
  }
  
  {
  // Stat the remote path, to make sure it exists and it makes sense
  std::string diskurl = "https://" + server + "/domedisk/";
  Log(Logger::Lvl4, domelogmask, domelogname, "Stat-ing new filesystem '" << newfs << "' in disk node: '" << server);
  Davix::Uri durl(diskurl);

  Davix::DavixError* tmp_err = NULL;
  DavixGrabber hdavix(*davixPool);
  DavixStuff *ds(hdavix);
  Davix::GetRequest req2(*(ds->ctx), durl, &tmp_err);
  if( tmp_err ) {
    std::ostringstream os;
    os << "Cannot initialize Davix query to" << durl << ", Error: "<< tmp_err->getErrMsg();
    Err(domelogname, os.str());
    Davix::DavixError::clearError(&tmp_err);
    return DomeReq::SendSimpleResp(request, 500, os);
  }
  req2.addHeaderField("cmd", "dome_statpfn");
  req2.addHeaderField("remoteclientdn", req.remoteclientdn);
  req2.addHeaderField("remoteclientaddr", req.remoteclienthost);

  boost::property_tree::ptree jresp;
  jresp.put("pfn", newfs);
  jresp.put("matchfs", "false");
  jresp.put("server", server);

  std::ostringstream os;
  boost::property_tree::write_json(os, jresp);

  req2.setRequestBody(os.str());
  // Set the dome timeout values for the operation
  req2.setParameters(*(ds->parms));
      
  int rc = req2.executeRequest(&tmp_err);
  if ( rc || tmp_err) {
    // The error must be propagated to the response, in clear readable text
    std::ostringstream os;
    int errcode = req2.getRequestCode();
    if (tmp_err)
      os << "Cannot execute cmd_statpfn to disk node. pfn: '" << newfs << "' Url: '" << durl << "' errcode: " << errcode << "'"<< tmp_err->getErrMsg() << "' response body: '" << req2.getAnswerContent();
    else
      os << "Cannot execute cmd_statrm to head node. pfn: '" << newfs << "' Url: '" << durl << "' errcode: " << errcode << " response body: '" << req2.getAnswerContent();
    
    Err(domelogname, os.str()); 
    Davix::DavixError::clearError(&tmp_err);
    return DomeReq::SendSimpleResp(request, 500, os);
  }
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
  fsfs.pool_defsize = pool_defsize;
  fsfs.pool_stype = pool_stype;
  rc =  sql.addFsPool(fsfs);
  if (!rc) t.Commit();
  }
  
  if (rc) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Could not insert new fs: '" << newfs << "' It likely already exists."));
    return 1;
  }
  
  status.addPoolfs(server, newfs, poolname);
  return DomeReq::SendSimpleResp(request, 200, SSTR("New filesystem added."));
}


/// Removes a filesystem, no matter to which pool it was attached
int DomeCore::dome_rmfs(DomeReq &req, FCGX_Request &request) {
  if (status.role != status.roleHead) {
    return DomeReq::SendSimpleResp(request, 500, "dome_rmfs only available on head nodes.");
  }
  
  std::string server =  req.bodyfields.get<std::string>("server", "");
  std::string newfs =  req.bodyfields.get<std::string>("fs", "");
  
  Log(Logger::Lvl4, domelogmask, domelogname, " serrver: '" << server << "' fs: '" << newfs << "'");
  
  if (!status.PfnMatchesAnyFS(server, newfs)) {
    return DomeReq::SendSimpleResp(request, 404, SSTR("Filesystem '" << newfs << "' not found on server '" << server << "'"));
  }
  
  int ndel = 0;
  {
    // Lock status!
    boost::unique_lock<boost::recursive_mutex> l(status);
    
    // Loop on the filesystems, looking for one that is a proper substring of the pfn
    for (std::vector<DomeFsInfo>::iterator fs = status.fslist.begin(); fs != status.fslist.end(); fs++) {
      if (status.PfnMatchesFS(server, newfs, *fs)) {
        status.fslist.erase(fs);
        ndel++;
        break;
      }
    }
  }
  
  
  // No matter how many deletions happened in the data structure,
  // propagate anyway the deletion to the db
    int rc;
  {
  DomeMySql sql;
  DomeMySqlTrans  t(&sql);
  std::string clientid = req.remoteclientdn;
  if (clientid.size() == 0) clientid = req.clientdn;
  if (clientid.size() == 0) clientid = "(unknown)";
  rc =  sql.rmFs(server, newfs);
  if (!rc) t.Commit();
  }
  
  if (rc) 
    return DomeReq::SendSimpleResp(request, 422, SSTR("Failed deleting filesystem '" << newfs << "' of server '" << server << "'"));
  
  
  return DomeReq::SendSimpleResp(request, 200, SSTR("Deleted " << ndel << "filesystems matching '" << newfs << "' of server '" << server << "'"));
}






/// Fecthes logical stat information for an LFN or file ID or a pfn
int DomeCore::dome_getstatinfo(DomeReq &req, FCGX_Request &request) {
  if (status.role != status.roleHead) {
    return DomeReq::SendSimpleResp(request, 500, "dome_getstatinfo only available on head nodes.");
  }
  
  std::string server =  req.bodyfields.get<std::string>("server", "");
  std::string pfn =  req.bodyfields.get<std::string>("pfn", "");
  std::string rfn =  req.bodyfields.get<std::string>("rfn", "");
  
  std::string lfn =  req.bodyfields.get<std::string>("lfn", "");
  
  Log(Logger::Lvl4, domelogmask, domelogname, " server: '" << server << "' pfn: '" << pfn << "' rfn: '" << rfn << "' lfn: '" << lfn << "'");
  
  struct dmlite::ExtendedStat st;
  
  // If lfn is filled then we stat the logical file
  if (lfn.size()) {
    DmlitePoolHandler stack(dmpool);
    try {
        st = stack->getCatalog()->extendedStat(lfn);
      }
      catch (dmlite::DmException e) {
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
          int taskrc = waitResult(id, CFG->GetLong("head.filepuller.stathooktimeout", 10));
          if (taskrc)
            return DomeReq::SendSimpleResp(request, 404, SSTR("Cannot remotely stat lfn: '" << lfn << "'"));
          
          std::string err;
          if (extract_stat(this->getTask(id)->stdout, err, st) <= 0) {
            Err(domelogname, "Failed stating lfn: '" << lfn << "' err: '" << err << "'");
            return DomeReq::SendSimpleResp(request, 500, SSTR("Cannot remotely stat lfn: '" << lfn << "'"));
          }
          
        }
        else
          return DomeReq::SendSimpleResp(request, 404, SSTR("Cannot stat lfn: '" << lfn << "' err: " << e.code() << " what: '" << e.what() << "'"));
      }
  

  }
  else {
    
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
      DmlitePoolHandler stack(dmpool);
      try {
        st = stack->getCatalog()->extendedStatByRFN(rfn); 
      }
      catch (dmlite::DmException e) {
        return DomeReq::SendSimpleResp(request, 404, SSTR("Cannot stat server: '" << server << "' pfn: '" << pfn << "' err: " << e.code() << " what: '" << e.what() << "'")); 
      }
  }
  
  boost::property_tree::ptree jresp;
    
  jresp.put("fileid", st.stat.st_ino);
  jresp.put("parentfileid", st.parent);
  jresp.put("size", st.stat.st_size);
  jresp.put("mode", st.stat.st_mode);
  jresp.put("atime", st.stat.st_atime);
  jresp.put("mtime", st.stat.st_mtime);
  jresp.put("ctime", st.stat.st_ctime);
  jresp.put("uid", st.stat.st_uid);
  jresp.put("gid", st.stat.st_gid);
  jresp.put("xattrs", st.serialize());
  
  jresp.put("isdir", ( S_ISDIR(st.stat.st_mode) ));
  
  return DomeReq::SendSimpleResp(request, 200, jresp);
  
}


/// Like an HTTP GET on a directory, gets all the content
int DomeCore::dome_getdir(DomeReq &req, FCGX_Request &request) {
  if (status.role != status.roleHead) {
    return DomeReq::SendSimpleResp(request, 500, "dome_getdir only available on head nodes.");
  }
  
  std::string path =  req.bodyfields.get<std::string>("path", "");
  bool statentries = req.bodyfields.get<bool>("statentries", false);
  
  if (!path.size()) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Cannot list an empty path"));
  }
  
  DmlitePoolHandler stack(dmpool);
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
        pt.put("size", st.stat.st_size);
        pt.put("flags", st.stat.st_mode);
        pt.put("isdir", S_ISDIR(st.stat.st_mode));
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
    return DomeReq::SendSimpleResp(request, 500, "dome_getuser only available on head nodes.");
  }

  std::string username = req.bodyfields.get<std::string>("username", "");
  if (!username.size()) {
    return DomeReq::SendSimpleResp(request, 422, SSTR("Username not specified"));
  }

  DmlitePoolHandler stack(dmpool);
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

    DmlitePoolHandler stack(dmpool);
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

  