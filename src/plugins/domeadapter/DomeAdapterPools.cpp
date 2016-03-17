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
  secCtx_ = secCtx;

  // Id mechanism
  if (factory_->tokenUseIp_)
    userId_ = secCtx_->credentials.remoteAddress;
  else
    userId_ = secCtx_->credentials.clientName;
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

  std::vector<Pool> ret;

  DavixGrabber grabber(factory_->davixPool_);
  DavixStuff *ds(grabber);

  Davix::DavixError *err = NULL;
  Davix::Uri uri(factory_->domehead_ + "/");
  Davix::GetRequest req(*ds->ctx, uri, &err);
  req.addHeaderField("cmd", "dome_getspaceinfo");
  req.addHeaderField("remoteclientdn", this->secCtx_->credentials.clientName);
  req.addHeaderField("remoteclientaddr", this->secCtx_->credentials.remoteAddress);
  req.setParameters(*ds->parms);
  req.executeRequest(&err);

  if(err) {
    throw DmException(EINVAL, "Error when sending dome_getspaceinfo to headnode: %s", err->getErrMsg().c_str());
  }

  std::vector<char> body = req.getAnswerContentVec();

  // parse json response
  boost::property_tree::ptree jresp = parseJSON(&body[0]);

  // extract pools
  using boost::property_tree::ptree;
  ptree poolinfo = jresp.get_child("poolinfo");

  ptree::const_iterator begin = poolinfo.begin();
  ptree::const_iterator end = poolinfo.end();
  for(ptree::const_iterator it = begin; it != end; it++) {
    Pool p = deserializePool(it);
    if(availability == kAny || availability == getAvailability(p)) {
      ret.push_back(p);
    }
  }

  return ret;
}

Pool DomeAdapterPoolManager::getPool(const std::string& poolname) throw (DmException) {
  Davix::Uri uri(factory_->domehead_ + "/");
  DomeTalker talker(factory_->davixPool_, secCtx_,
                    "GET", uri, "dome_statpool");

  if(!talker.execute("poolname", poolname)) {
    throw DmException(EINVAL, talker.err());
  }

  try {
    return deserializePool(talker.jresp().get_child("poolinfo").begin());
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(DMLITE_NO_SUCH_POOL, SSTR("Pool " + poolname + " not found. (" << e.what() << ")"));
  }
}

void DomeAdapterPoolManager::newPool(const Pool& pool) throw (DmException) {
  Davix::Uri uri(factory_->domehead_ + "/");
  DomeTalker talker(factory_->davixPool_, secCtx_,
                    "POST", uri, "dome_addpool");

  if(!talker.execute("poolname", pool.name)) {
    throw DmException(EINVAL, talker.err());
  }
}

// void DomeAdapterPoolManager::updatePool(const Pool& pool) throw (DmException) {

// }
  
void DomeAdapterPoolManager::deletePool(const Pool& pool) throw (DmException) {
  Davix::Uri uri(factory_->domehead_ + "/");
  DomeTalker talker(factory_->davixPool_, secCtx_,
                    "POST", uri, "dome_rmpool");

  if(!talker.execute("poolname", pool.name)) {
    throw DmException(EINVAL, talker.err());
  }
}

Location DomeAdapterPoolManager::whereToRead(const std::string& path) throw (DmException)
{
  Davix::Uri uri(factory_->domehead_ + "/" + path);
  DomeTalker talker(factory_->davixPool_, secCtx_,
                    "GET", uri, "dome_get");

  if(!talker.execute()) {
    throw DmException(EINVAL, talker.err());
  }

  if(talker.status() == 202) {
    throw DmException(EINPROGRESS, &talker.response()[0]);
  }

  try {
    Location loc;

    // extract pools
    using boost::property_tree::ptree;

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
    throw DmException(EINVAL, SSTR("Error when parsing json response: " << &talker.response()[0]));
  }
}

// Location DomeAdapterPoolManager::whereToRead (ino_t inode)             throw (DmException) {
// }

Location DomeAdapterPoolManager::whereToWrite(const std::string& path) throw (DmException) 
{
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering. Path: " << path);
  Davix::Uri uri(factory_->domehead_ + "/" + path);
  DomeTalker talker(factory_->davixPool_, secCtx_,
                    "POST", uri, "dome_put");

  if(!talker.execute("lfn", path)) {
    throw DmException(EINVAL, talker.err());
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
    throw DmException(EINVAL, SSTR("Error when parsing json response: " << &talker.response()[0]));
  }
}

// void DomeAdapterPoolManager::cancelWrite(const Location& loc) throw (DmException) {

// }

