/// @file   DomeAdapterDiskCatalog.cpp
/// @brief  Dome adapter catalog
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>

#include <dmlite/cpp/dmlite.h>
#include <iostream>

#include "DomeAdapterDiskCatalog.h"
#include "DomeAdapter.h"
#include "utils/DomeTalker.h"
#include "utils/DomeUtils.h"
#include "DomeAdapterUtils.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/optional/optional.hpp>

using namespace dmlite;

DomeAdapterDiskCatalog::DomeAdapterDiskCatalog(DomeAdapterFactory *factory) throw (DmException) {
  factory_ = factory;
  sec_ = NULL;
}

std::string DomeAdapterDiskCatalog::getImplId() const throw() {
  return std::string("DomeAdapterDiskCatalog");
}

void DomeAdapterDiskCatalog::setStackInstance(StackInstance* si) throw (DmException)
{
  this->si_ = si;
}

void DomeAdapterDiskCatalog::setSecurityContext(const SecurityContext* secCtx) throw (DmException)
{
  this->sec_ = secCtx;
}

void DomeAdapterDiskCatalog::getChecksum(const std::string& path,
                                         const std::string& csumtype,
                                         std::string& csumvalue,
                                         const std::string& pfn,
                                         const bool forcerecalc,
                                         const int waitsecs) throw (DmException) {
  Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, " Entering, path '"
                                                            << path << "', csumtype '"
                                                            << csumtype << "'");
  time_t start = time(0);
  bool recalc = forcerecalc;

  int waitsecs1 = waitsecs;
  if (waitsecs1 == 0) waitsecs1 = 1800;

  while(true) {
    DomeTalker talker(factory_->davixPool_, sec_, factory_->domehead_,
                      "GET", "dome_chksum");

    boost::property_tree::ptree params;
    params.put("checksum-type", csumtype);
    params.put("lfn", path);
    params.put("force-recalc", DomeUtils::bool_to_str(recalc));
    recalc = false; // no force-recalc in subsequent requests

    if(!talker.execute(params)) {
      throw DmException(EINVAL, talker.err());
    }

    // checksum calculation in progress
    if(talker.status() == 202) {
      if(time(0) - start >= waitsecs1)
        throw DmException(EAGAIN, SSTR(waitsecs << "s were not sufficient to checksum '" << csumtype << ":" << path << "'. Try again later."));
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


Replica DomeAdapterDiskCatalog::getReplicaByRFN(const std::string& rfn) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "rfn: " << rfn);
  DomeTalker talker(factory_->davixPool_, sec_, factory_->domehead_,
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

// taken from built-in catalog and slightly modified
bool DomeAdapterDiskCatalog::accessReplica(const std::string& rfn, int mode) throw (DmException) {
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

ExtendedStat DomeAdapterDiskCatalog::extendedStat(const std::string& path, bool follow) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "path: " << path << " follow (ignored) :" << follow);

  DomeTalker talker(factory_->davixPool_, sec_, factory_->domehead_,
                    "GET", "dome_getstatinfo");

  if(!talker.execute("lfn", path)) {
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

ExtendedStat DomeAdapterDiskCatalog::extendedStatByRFN(const std::string& rfn)  throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "rfn: " << rfn );

  DomeTalker talker(factory_->davixPool_, sec_, factory_->domehead_,
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

Directory* DomeAdapterDiskCatalog::openDir(const std::string& path) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering. Path: " << path);
  using namespace boost::property_tree;
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "path: " << path);
  DomeTalker talker(factory_->davixPool_, sec_, factory_->domehead_,
                    "GET", "dome_getdir");

  ptree params;
  params.put("path", path);
  params.put("statentries", "true");

  if(!talker.execute(params)) {
    throw DmException(EINVAL, talker.err());
  }

  try {
    DomeDir *domedir = new DomeDir(path);

    ptree entries = talker.jresp().get_child("entries");
    for(ptree::const_iterator it = entries.begin(); it != entries.end(); it++) {
      ExtendedStat xstat;
      xstat.name = it->second.get<std::string>("name");

      Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "entry " << xstat.name);

      ptree_to_xstat(it->second, xstat);
      domedir->entries_.push_back(xstat);
    }
    return domedir;
  }
  catch(ptree_error &e) {
    throw DmException(EINVAL, SSTR("Error when parsing json response - " << e.what() << " : " << talker.response()));
  }
}

void DomeAdapterDiskCatalog::closeDir(Directory* dir) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering.");
  DomeDir *domedir = static_cast<DomeDir*>(dir);
  delete domedir;
}

ExtendedStat* DomeAdapterDiskCatalog::readDirx(Directory* dir) throw (DmException) {
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

void DomeAdapterDiskCatalog::updateExtendedAttributes(const std::string& lfn,
                                                  const Extensible& ext) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering.");

  DomeTalker talker(factory_->davixPool_, sec_, factory_->domehead_,
                    "POST", "dome_updatexattr");

  if(!talker.execute("lfn", lfn, "xattr", ext.serialize())) {
    throw DmException(EINVAL, talker.err());
  }
}
