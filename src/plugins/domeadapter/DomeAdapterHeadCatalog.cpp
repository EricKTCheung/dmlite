/// @file   DomeAdapterIO.cpp
/// @brief  Filesystem IO with dome
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/pooldriver.h>
#include <errno.h>
#include <sys/uio.h>
#include <iostream>
#include <stdio.h>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "utils/logger.h"
#include "utils/DomeUtils.h"
#include "utils/DomeTalker.h"
#include "DomeAdapterUtils.h"
#include "DomeAdapterHeadCatalog.h"

using namespace dmlite;
using boost::property_tree::ptree;

DomeAdapterHeadCatalogFactory::DomeAdapterHeadCatalogFactory(): davixPool_(&davixFactory_, 10) {
  domeadapterlogmask = Logger::get()->getMask(domeadapterlogname);
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " Ctor");
}

DomeAdapterHeadCatalogFactory::~DomeAdapterHeadCatalogFactory()
{
  // Nothing
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " ");
}

void DomeAdapterHeadCatalogFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  bool gotit = true;
  LogCfgParm(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, key, value);

  if (key == "DomeHead") {
    domehead_ = value;
  }
  // if parameter starts with "Davix", pass it on to the factory
  else if( key.find("Davix") != std::string::npos) {
    Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Received davix pool parameter: " << key << "," << value);
    davixFactory_.configure(key, value);
  }
  else gotit = false;

  if (gotit)
    LogCfgParm(Logger::Lvl1, Logger::unregistered, "DomeAdapterHeadCatalogFactory", key, value);
}

Catalog* DomeAdapterHeadCatalogFactory::createCatalog(PluginManager* pm) throw (DmException) {
  return new DomeAdapterHeadCatalog(this);
}

DomeAdapterHeadCatalog::DomeAdapterHeadCatalog(DomeAdapterHeadCatalogFactory *factory) :
  secCtx_(0), factory_(*factory)
{
  // Nothing
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " Ctor");
}

DomeAdapterHeadCatalog::~DomeAdapterHeadCatalog()
{
}

std::string DomeAdapterHeadCatalog::getImplId() const throw ()
{
  return "DomeAdapterHeadCatalog";
}

void DomeAdapterHeadCatalog::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  this->secCtx_ = ctx;
}

void DomeAdapterHeadCatalog::setStackInstance(StackInstance* si) throw (DmException)
{
  this->si_ = si;
}


void DomeAdapterHeadCatalog::getChecksum(const std::string& path,
                                         const std::string& csumtype,
                                         std::string& csumvalue,
                                         const std::string& pfn,
                                         const bool forcerecalc,
                                         const int waitsecs) throw (DmException) {
  Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, " Entering, path '"
                                                            << absPath(path) << "', csumtype '"
                                                            << csumtype << "'");
  time_t start = time(0);
  bool recalc = forcerecalc;

  int waitsecs1 = waitsecs;
  if (waitsecs1 == 0) waitsecs1 = 1800;

  while(true) {
    DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                      "GET", "dome_chksum");

    boost::property_tree::ptree params;
    params.put("checksum-type", csumtype);
    params.put("lfn", absPath(path));
    params.put("force-recalc", DomeUtils::bool_to_str(recalc));
    recalc = false; // no force-recalc in subsequent requests

    if(!talker.execute(params)) {
      throw DmException(EINVAL, talker.err());
    }

    // checksum calculation in progress
    if(talker.status() == 202) {
      if(time(0) - start >= waitsecs1)
        throw DmException(EAGAIN, SSTR(waitsecs << "s were not sufficient to checksum '" << csumtype << ":" << absPath(path) << "'. Try again later."));
      sleep(5);
      continue;
    }

    try {
      csumvalue = talker.jresp().get<std::string>("checksum");
      return;
    }
    catch(boost::property_tree::ptree_error &e) {
      throw DmException(EINVAL, SSTR("Error when parsing json response: " << talker.response()));
    }
  }
}

std::string DomeAdapterHeadCatalog::getWorkingDir() throw (DmException) {
  return this->cwdPath_;
}

void DomeAdapterHeadCatalog::changeDir(const std::string& path) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering. path: '" << path << "'");

  if (path.empty()) {
    this->cwdPath_.clear();
    return;
  }

  this->extendedStat(path,true);
  if (path[0] == '/')
    this->cwdPath_ = path;
  else
    this->cwdPath_ = Url::normalizePath(this->cwdPath_ + "/" + path);
}

DmStatus DomeAdapterHeadCatalog::extendedStat(ExtendedStat &xstat, const std::string& path, bool follow) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "path: " << path << " follow (ignored) :" << follow);
  std::string targetpath = absPath(path);

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "GET", "dome_getstatinfo");

  if(!talker.execute("lfn", targetpath)) {
    if(talker.dmlite_code() == ENOENT) return DmStatus(ENOENT, SSTR(path << " not found"));
    throw DmException(talker.dmlite_code(), talker.err());
  }

  try {
    xstat = ExtendedStat();
    ptree_to_xstat(talker.jresp(), xstat);
    return DmStatus();
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(EINVAL, SSTR("Error when parsing json response: " << talker.response()));
  }
}

ExtendedStat DomeAdapterHeadCatalog::extendedStat(const std::string& path, bool follow) throw (DmException) {
  ExtendedStat ret;
  DmStatus st = this->extendedStat(ret, path, follow);
  if(!st.ok()) throw st.exception();
  return ret;
}

bool DomeAdapterHeadCatalog::access(const std::string& sfn, int mode) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "sfn: '" << sfn << "' mode: '" << mode << "'");

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "GET", "dome_access");

  if(!talker.execute("path", sfn, "mode", SSTR(mode))) {
    if(talker.status() == 403) return false;
    throw DmException(talker.dmlite_code(), talker.err());
  }

  return true;
}

// taken from built-in catalog and slightly modified
bool DomeAdapterHeadCatalog::accessReplica(const std::string& rfn, int mode) throw (DmException) {
  try {
    Replica      replica = this->getReplicaByRFN(rfn);
    // ExtendedStat xstat   = this->extendedStat(replica.fileid);

    bool replicaAllowed = true;
    mode_t perm = 0;

    if (mode & R_OK)
      perm  = S_IREAD;

    if (mode & W_OK) {
      perm |= S_IWRITE;
      replicaAllowed = (replica.status == Replica::kBeingPopulated);
    }

    if (mode & X_OK)
      perm |= S_IEXEC;

    // bool metaAllowed    = (checkPermissions(sec_, xstat.acl, xstat.stat, perm) == 0);
    return replicaAllowed;
  }
  catch (DmException& e) {
    if (e.code() != EACCES) throw;
    return false;
  }
}

Replica DomeAdapterHeadCatalog::getReplicaByRFN(const std::string& rfn) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "rfn: " << rfn);
  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "GET", "dome_getreplicainfo");

  if(!talker.execute("rfn", rfn)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }

  try {
    Replica replica;
    ptree_to_replica(talker.jresp(), replica);
    return replica;
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(EINVAL, SSTR("Error when parsing json response: '" << e.what() << "'. Contents: '" << talker.response() << "'"));
  }
}

Replica DomeAdapterHeadCatalog::getReplica(int64_t rid) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "rid: " << rid);
  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "GET", "dome_getreplicainfo");

  if(!talker.execute("replicaid", SSTR(rid))) {
    throw DmException(talker.dmlite_code(), talker.err());
  }

  try {
    Replica replica;
    ptree_to_replica(talker.jresp(), replica);
    return replica;
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(EINVAL, SSTR("Error when parsing json response: '" << e.what() << "'. Contents: '" << talker.response() << "'"));
  }
}

std::vector<Replica> DomeAdapterHeadCatalog::getReplicas(const std::string& lfn) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "lfn: " << lfn);
  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "GET", "dome_getreplicavec");

  if(!talker.execute("lfn", lfn)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }

  try {
    std::vector<Replica> replicas;
    ptree entries = talker.jresp().get_child("replicas");
    for(ptree::const_iterator it = entries.begin(); it != entries.end(); it++) {
      Replica replica;
      ptree_to_replica(it->second, replica);
      replicas.push_back(replica);
    }
    return replicas;
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(EINVAL, SSTR("Error when parsing json response: '" << e.what() << "'. Contents: '" << talker.response() << "'"));
  }
}

void DomeAdapterHeadCatalog::updateReplica(const Replica& replica) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "rfn: " << replica.rfn);
  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "POST", "dome_updatereplica");

  boost::property_tree::ptree params;
  params.put("rfn", replica.rfn);
  params.put("replicaid", replica.replicaid);
  params.put("status", replica.status);
  params.put("type", replica.type);
  params.put("setname", replica.setname);
  params.put("xattr", replica.serialize());

  if(!talker.execute(params)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

void DomeAdapterHeadCatalog::addReplica(const Replica& rep) throw (DmException) {
  Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, " Entering, replica: '" << rep.rfn << "'");

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "POST", "dome_addreplica");

  boost::property_tree::ptree params;
  params.put("rfn", rep.rfn);
  params.put("status", rep.status);
  params.put("type", rep.type);
  params.put("setname", rep.setname);
  params.put("xattr", rep.serialize());

  if(!talker.execute(params)) {
    throw DmException(EINVAL, talker.err());
  }

}

void DomeAdapterHeadCatalog::deleteReplica(const Replica &rep) throw (DmException) {
  Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, " Entering, rfn: '" << rep.rfn << "'");



    DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                      "POST", "dome_delreplica");

    boost::property_tree::ptree params;
    params.put("server", DomeUtils::server_from_rfio_syntax(rep.rfn));
    params.put("pfn", DomeUtils::pfn_from_rfio_syntax(rep.rfn));


    if(!talker.execute(params)) {
      throw DmException(EINVAL, talker.err());
    }


}

void DomeAdapterHeadCatalog::symlink(const std::string &target, const std::string &link) throw (DmException) {
  Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, " Entering, target: '" << target << "', link: '" << link << "'");

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "POST", "dome_symlink");

  if(!talker.execute("target", absPath(target), "link", absPath(link))) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

void DomeAdapterHeadCatalog::makeDir(const std::string& path, mode_t mode) throw (DmException) {
  Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, " Entering, path: '" << path << "', mode: " << mode);

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "POST", "dome_makedir");

  if(!talker.execute("path", absPath(path), "mode", SSTR(mode))) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

void DomeAdapterHeadCatalog::create(const std::string& path, mode_t mode) throw (DmException) {
  Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, " Entering, path: '" << path << "', mode: " << mode);

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "POST", "dome_create");

  if(!talker.execute("path", absPath(path), "mode", SSTR(mode))) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

void DomeAdapterHeadCatalog::removeDir(const std::string& path) throw (DmException) {
  Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, " Entering, path: '" << absPath(path));

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "POST", "dome_removedir");

  if(!talker.execute("path", absPath(path))) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

void DomeAdapterHeadCatalog::setGuid(const std::string& path, const std::string& guid) throw (DmException) {
  throw DmException(ENOTSUP, "Not supported");
}

void DomeAdapterHeadCatalog::setOwner(const std::string& path, uid_t uid, gid_t gid, bool follow) throw (DmException) {
  Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, " Entering, path: '" << absPath(path) << "', uid: " << uid << ", gid: " << gid);

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "POST", "dome_setowner");

  boost::property_tree::ptree params;
  params.put("path", absPath(path));
  params.put("uid", SSTR(uid));
  params.put("gid", SSTR(gid));
  params.put("follow", DomeUtils::bool_to_str(follow));

  if(!talker.execute(params)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

void DomeAdapterHeadCatalog::setMode(const std::string& path, mode_t mode) throw (DmException) {
  Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, " Entering, path: '" << absPath(path) << "', mode: " << mode);

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "POST", "dome_setmode");

  if(!talker.execute("path", absPath(path), "mode", SSTR(mode))) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

void DomeAdapterHeadCatalog::setSize(const std::string& path, size_t newSize) throw (DmException) {
  Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, " Entering, path: '" << absPath(path) << "', newSize: " << newSize);

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "POST", "dome_setsize");

  if(!talker.execute("path", absPath(path), "size", SSTR(newSize))) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

Directory* DomeAdapterHeadCatalog::openDir(const std::string& path) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering. Path: " << absPath(path));
  using namespace boost::property_tree;
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "path: " << path);
  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "GET", "dome_getdir");

  ptree params;
  params.put("path", absPath(path));
  params.put("statentries", "true");

  if(!talker.execute(params)) {
    throw DmException(EINVAL, talker.err());
  }

  try {
    DomeDir *domedir = new DomeDir(absPath(path));

    ptree entries = talker.jresp().get_child("entries");
    for(ptree::const_iterator it = entries.begin(); it != entries.end(); it++) {
      ExtendedStat xstat;
      xstat.name = it->second.get<std::string>("name");

      Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "entry " << xstat.name);

      ptree_to_xstat(it->second, xstat);
      domedir->entries_.push_back(xstat);
    }
    domedir->dirents_.resize(domedir->entries_.size()); // will fill later, if needed
    return domedir;
  }
  catch(ptree_error &e) {
    throw DmException(EINVAL, SSTR("Error when parsing json response - " << e.what() << " : " << talker.response()));
  }
}

void DomeAdapterHeadCatalog::closeDir(Directory* dir) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering.");
  DomeDir *domedir = static_cast<DomeDir*>(dir);
  delete domedir;
}

ExtendedStat* DomeAdapterHeadCatalog::readDirx(Directory* dir) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering.");
  if (dir == NULL) {
    throw DmException(DMLITE_SYSERR(EFAULT), "Tried to read a null dir");
  }

  DomeDir *domedir = static_cast<DomeDir*>(dir);
  if(domedir->pos_ >= domedir->entries_.size()) {
    return NULL;
  }

  domedir->pos_++;
  return &domedir->entries_[domedir->pos_ - 1];
}

struct dirent* DomeAdapterHeadCatalog::readDir(Directory* dir) throw (DmException) {
  Log(Logger::Lvl1, domeadapterlogmask, domeadapterlogname, "Entering.");
  if (dir == NULL) {
    throw DmException(DMLITE_SYSERR(EFAULT), "Tried to read a null dir");
  }

  ExtendedStat *st = this->readDirx(dir);
  if(st == NULL) return NULL;

  DomeDir *domedir = static_cast<DomeDir*>(dir);
  struct dirent *entry = &domedir->dirents_[domedir->pos_ - 1];
  entry->d_ino = st->stat.st_ino;
  strncpy(entry->d_name, st->name.c_str(), sizeof(entry->d_name));

  return entry;
}

void DomeAdapterHeadCatalog::rename(const std::string& oldPath, const std::string& newPath) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering.");

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "POST", "dome_rename");

  if(!talker.execute("oldpath", absPath(oldPath), "newpath", absPath(newPath))) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

void DomeAdapterHeadCatalog::unlink(const std::string& path) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering.");

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "POST", "dome_unlink");

  if(!talker.execute("lfn", absPath(path))) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

ExtendedStat DomeAdapterHeadCatalog::extendedStatByRFN(const std::string& rfn)  throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "rfn: " << rfn );

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "GET", "dome_getstatinfo");

  if(!talker.execute("rfn", rfn)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }

  try {
    ExtendedStat xstat;
    ptree_to_xstat(talker.jresp(), xstat);
    return xstat;
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(EINVAL, SSTR("Error when parsing json response: " << talker.response()));
  }
}

std::string DomeAdapterHeadCatalog::getComment(const std::string& path) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "path: " << path );
  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "GET", "dome_getcomment");

  if(!talker.execute("lfn", absPath(path))) {
    throw DmException(talker.dmlite_code(), talker.err());
  }

  try {
    return talker.jresp().get<std::string>("comment");
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(EINVAL, SSTR("Error when parsing json response: " << talker.response()));
  }
}

void DomeAdapterHeadCatalog::setComment(const std::string& path, const std::string& comment) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "path: " << path );
  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "POST", "dome_setcomment");

  if(!talker.execute("lfn", absPath(path), "comment", comment)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

void DomeAdapterHeadCatalog::updateExtendedAttributes(const std::string& lfn,
                                                  const Extensible& ext) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering.");

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "POST", "dome_updatexattr");

  if(!talker.execute("lfn", lfn, "xattr", ext.serialize())) {
    throw DmException(EINVAL, talker.err());
  }
}

std::string DomeAdapterHeadCatalog::readLink(const std::string& path) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering.");

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "GET", "dome_readlink");

  if(!talker.execute("lfn", path)) {
    throw DmException(EINVAL, talker.err());
  }

  try {
    return talker.jresp().get<std::string>("target");
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(EINVAL, SSTR("Error when parsing json response: " << talker.response()));
  }
}

std::string DomeAdapterHeadCatalog::absPath(const std::string &relpath) {
  if(relpath.size() > 0 && relpath[0] == '/') return relpath;
  return SSTR(this->cwdPath_ + "/" + relpath);
}
