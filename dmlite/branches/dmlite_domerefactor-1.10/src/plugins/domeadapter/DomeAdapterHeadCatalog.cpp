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
using namespace Davix;
using boost::property_tree::ptree;

DomeAdapterHeadCatalogFactory::DomeAdapterHeadCatalogFactory(CatalogFactory *nested): nested_(nested), davixPool_(&davixFactory_, 10) {
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
  if(this->nested_ == 0x00)
    return 0x00;

  return new DomeAdapterHeadCatalog(this,
                                    CatalogFactory::createCatalog(this->nested_, pm));
}

DomeAdapterHeadCatalog::DomeAdapterHeadCatalog(DomeAdapterHeadCatalogFactory *factory, Catalog *nested) :
  DummyCatalog(nested), decorated_(nested), secCtx_(0), factory_(*factory)
{
  // Nothing
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " Ctor");
}

DomeAdapterHeadCatalog::~DomeAdapterHeadCatalog()
{
  // Nothing, dummy catalog deletes the nested catalog
}

std::string DomeAdapterHeadCatalog::getImplId() const throw ()
{
  return "DomeAdapterHeadCatalog over " + decorated_->getImplId();
}

void DomeAdapterHeadCatalog::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  BaseInterface::setSecurityContext(this->decorated_, ctx);
  this->secCtx_ = ctx;
}

void DomeAdapterHeadCatalog::setStackInstance(StackInstance* si) throw (DmException)
{
  BaseInterface::setStackInstance(this->decorated_, si);
  this->si_ = si;
}


void DomeAdapterHeadCatalog::getChecksum(const std::string& path,
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
    DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
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

void DomeAdapterHeadCatalog::changeDir(const std::string& path) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering. path: '" << path << "'");
  this->decorated_->changeDir(path);

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
  std::string targetpath;

  if (path[0] == '/') {
    // path is absolute
    targetpath = path;
  }
  else {
    // path is relative, no matter what the current path is
    // we can prefix the / anyway
    targetpath = SSTR(cwdPath_ << "/" << path);
  }


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

void DomeAdapterHeadCatalog::makeDir(const std::string& path, mode_t mode) throw (DmException) {
  Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, " Entering, path: '" << path << "', mode: " << mode);

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "POST", "dome_makedir");

  if(!talker.execute("path", path, "mode", SSTR(mode))) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

void DomeAdapterHeadCatalog::create(const std::string& path, mode_t mode) throw (DmException) {
  Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, " Entering, path: '" << path << "', mode: " << mode);

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "POST", "dome_create");

  if(!talker.execute("path", path, "mode", SSTR(mode))) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

void DomeAdapterHeadCatalog::removeDir(const std::string& path) throw (DmException) {
  Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, " Entering, path: '" << path);

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "POST", "dome_removedir");

  if(!talker.execute("path", path)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

void DomeAdapterHeadCatalog::setGuid(const std::string& path, const std::string& guid) throw (DmException) {
  throw DmException(ENOTSUP, "Not supported");
}

void DomeAdapterHeadCatalog::setSize(const std::string& path, size_t newSize) throw (DmException) {
  Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, " Entering, path: '" << path << "', newSize: " << newSize);

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "POST", "dome_setsize");

  if(!talker.execute("path", path, "size", SSTR(newSize))) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

Directory* DomeAdapterHeadCatalog::openDir(const std::string& path) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering. Path: " << path);
  using namespace boost::property_tree;
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "path: " << path);
  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
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

void DomeAdapterHeadCatalog::rename(const std::string& oldPath, const std::string& newPath) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering.");

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "POST", "dome_rename");

  if(!talker.execute("oldpath", oldPath, "newpath", newPath)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

void DomeAdapterHeadCatalog::unlink(const std::string& path) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering.");

  DomeTalker talker(factory_.davixPool_, secCtx_, factory_.domehead_,
                    "POST", "dome_unlink");

  if(!talker.execute("lfn", path)) {
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
