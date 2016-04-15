/// @file   DomeAdapterIO.cpp
/// @brief  Filesystem IO with dome
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/pooldriver.h>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>

#include "DomeAdapter.h"
#include "DomeAdapterPools.h"
#include "DomeAdapterDriver.h"

#include "utils/DomeTalker.h"
#include "DomeAdapterUtils.h"

using namespace dmlite;
using boost::property_tree::ptree;

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()


DomeAdapterPoolManager::DomeAdapterPoolManager(DomeAdapterFactory *factory) {
  factory_ = factory;
}

DomeAdapterPoolManager::~DomeAdapterPoolManager() {

}

std::string DomeAdapterPoolManager::getImplId() const throw () {
  return "DomeAdapterPoolManager";
}

void DomeAdapterPoolManager::setStackInstance(StackInstance* si) throw (DmException) {
  si_ = si;
}

void DomeAdapterPoolManager::setSecurityContext(const SecurityContext* secCtx) throw (DmException) {
  creds_ = &secCtx->credentials;

  // Id mechanism
  if (factory_->tokenUseIp_)
    userId_ = creds_->remoteAddress;
  else
    userId_ = creds_->clientName;
}

static PoolManager::PoolAvailability getAvailability(const Pool &p) {
  return PoolManager::kNone; // TODO
}

static boost::property_tree::ptree parseJSON(const char *s) {
  std::istringstream iss(s);
  boost::property_tree::ptree jresp;

  try {
      boost::property_tree::read_json(iss, jresp);
  } catch (boost::property_tree::json_parser_error e) {
      throw DmException(EINVAL, "Could not process JSON: %s --- %s", e.what(), s);
  }
  return jresp;
}

std::vector<Pool> DomeAdapterPoolManager::getPools(PoolAvailability availability) throw (DmException) {
  if(availability == kForBoth) {
    availability = kForWrite;
  }

  DomeTalker talker(factory_->davixPool_, creds_, factory_->domehead_,
                    "GET", "dome_getspaceinfo");

  if(!talker.execute()) {
    throw DmException(talker.dmlite_code(), talker.err());
  }

  std::vector<Pool> ret;
  try {
    // extract pools
    ptree poolinfo = talker.jresp().get_child("poolinfo");
    for(ptree::const_iterator it = poolinfo.begin(); it != poolinfo.end(); it++) {
      Pool p = deserializePool(it);
      if(availability == kAny || availability == getAvailability(p)) {
        ret.push_back(p);
      }
    }
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(EINVAL, SSTR("Error when parsing json response: " << talker.response()));
  }

  return ret;
}

Pool DomeAdapterPoolManager::getPool(const std::string& poolname) throw (DmException) {
  DomeTalker talker(factory_->davixPool_, creds_, factory_->domehead_,
                    "GET", "dome_statpool");

  if(!talker.execute("poolname", poolname)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }

  try {
    return deserializePool(talker.jresp().get_child("poolinfo").begin());
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(DMLITE_NO_SUCH_POOL, SSTR("Pool " + poolname + " not found. (" << e.what() << ")"));
  }
}

void DomeAdapterPoolManager::newPool(const Pool& pool) throw (DmException) {
  DomeTalker talker(factory_->davixPool_, creds_, factory_->domehead_,
                    "POST", "dome_addpool");

  if(!talker.execute("poolname", pool.name)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

// void DomeAdapterPoolManager::updatePool(const Pool& pool) throw (DmException) {

// }

void DomeAdapterPoolManager::deletePool(const Pool& pool) throw (DmException) {
  DomeTalker talker(factory_->davixPool_, creds_, factory_->domehead_,
                    "POST", "dome_rmpool");

  if(!talker.execute("poolname", pool.name)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

Location DomeAdapterPoolManager::whereToRead(const std::string& path) throw (DmException)
{
  DomeTalker talker(factory_->davixPool_, creds_, factory_->domehead_,
                    "GET", "dome_get");

  if(!talker.execute("lfn", path)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }

  if(talker.status() == 202) {
    throw DmException(EINPROGRESS, talker.response());
  }

  try {
    Location loc;

    // extract pools
    ptree::const_iterator begin = talker.jresp().begin();
    ptree::const_iterator end = talker.jresp().end();
    for(ptree::const_iterator it = begin; it != end; it++) {
      std::string host = it->second.get<std::string>("server");
      std::string pfn = it->second.get<std::string>("pfn");

      Chunk chunk(host + ":" + pfn, 0, 0);
      chunk.url.query["token"] = dmlite::generateToken(userId_, pfn, factory_->tokenPasswd_, factory_->tokenLife_);
      loc.push_back(chunk);
    }
    return loc;
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(EINVAL, SSTR("Error when parsing json response: " << talker.response()));
  }
}

Location DomeAdapterPoolManager::whereToWrite(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering. (PoolManager) Path: " << path);
  DomeTalker talker(factory_->davixPool_, creds_, factory_->domehead_,
                    "POST", "dome_put");

  if(!talker.execute("lfn", path)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }

  try {
    std::string host = talker.jresp().get<std::string>("host");
    std::string pfn = talker.jresp().get<std::string>("pfn");

    Chunk chunk(host+":"+pfn, 0, 0);
    chunk.url.query["sfn"] = path;
    chunk.url.query["token"] = dmlite::generateToken(userId_, pfn, factory_->tokenPasswd_, factory_->tokenLife_, true);
    return Location(1, chunk);
  }
  catch(boost::property_tree::ptree &err) {
    throw DmException(EINVAL, SSTR("Error when parsing json response: " << talker.response()));
  }
}