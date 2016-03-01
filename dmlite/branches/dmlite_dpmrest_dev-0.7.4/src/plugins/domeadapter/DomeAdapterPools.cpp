/// @file   DomeAdapterIO.cpp
/// @brief  Filesystem IO with dome
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/pooldriver.h>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
    
#include "DomeAdapterPools.h"
#include "DomeAdapterDriver.h"

using namespace dmlite;
#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

void DomeAdapterPoolsFactory::configure(const std::string& key, const std::string& value) throw (DmException) 
{
  std::cout << "in DomeAdapterFactory::configure with " << key << " = " << value << std::endl;
  LogCfgParm(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, key, value);

  if(key == "DomeHead") {
    domehead = value;
  }
  else if( key.find("Davix") != std::string::npos) {
    std::cout << "sending param to davix" << std::endl;
    davixFactory_.configure(key, value);
  }
}

PoolManager* DomeAdapterPoolsFactory::createPoolManager(PluginManager*) throw (DmException)
{
  return new DomeAdapterPoolManager(this);
}

PoolDriver* DomeAdapterPoolsFactory::createPoolDriver(PluginManager*) throw (DmException)
{
  return new DomeAdapterPoolDriver(this);
}

DomeAdapterPoolsFactory::DomeAdapterPoolsFactory() throw (DmException) : davixPool_(&davixFactory_, 10)
{

}

DomeAdapterPoolsFactory::~DomeAdapterPoolsFactory()
{
  
}

DomeAdapterPoolManager::DomeAdapterPoolManager(DomeAdapterPoolsFactory *factory) {
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
}

static PoolManager::PoolAvailability getAvailability(const Pool &p) {
  return PoolManager::kNone; // TODO
}

static Pool deserializePool(boost::property_tree::ptree::const_iterator it) {
    Pool p;
    p.name = it->first;
    p.type = "filesystem";
    p["freespace"] = it->second.get<std::string>("freespace", "-1").c_str();
    p["physicalsize"] = it->second.get<std::string>("physicalsize", "-1").c_str();

    std::stringstream ss;
    boost::property_tree::write_json(ss, it->second.get_child("fsinfo"));

    Extensible fsinfo;
    fsinfo.deserialize(ss.str());
    p["fsinfo"] = fsinfo;

    return p;
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
  Davix::Uri uri(factory_->domehead + "/");
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
  DavixGrabber grabber(factory_->davixPool_);
  DavixStuff *ds(grabber);

  Davix::DavixError *err = NULL;
  Davix::Uri uri(factory_->domehead + "/");
  Davix::GetRequest req(*ds->ctx, uri, &err);
  req.addHeaderField("cmd", "dome_statpool");
  req.addHeaderField("remoteclientdn", this->secCtx_->credentials.clientName);
  req.addHeaderField("remoteclientaddr", this->secCtx_->credentials.remoteAddress);

  boost::property_tree::ptree params;
  params.put("poolname", poolname);

  std::ostringstream os;
  boost::property_tree::write_json(os, params);

  req.setParameters(*ds->parms);
  req.setRequestBody(os.str());
  req.executeRequest(&err);

   if(err) {
     throw DmException(EINVAL, "Error when sending dome_getstatpool to headnode: %s", err->getErrMsg().c_str());
   }

  std::vector<char> body = req.getAnswerContentVec();

  // parse json response
  boost::property_tree::ptree jresp = parseJSON(&body[0]);

  try {
    boost::property_tree::ptree poolinfo = jresp.get_child("poolinfo");
    Pool p = deserializePool(poolinfo.begin());
    return p;
  }
  catch(boost::property_tree::ptree_bad_path &e) {
    Err(domeadapterlogname, " Pool poolname: " << poolname << " not found.");
    throw DmException(DMLITE_NO_SUCH_POOL, "Pool " + poolname + " not found");
  }
}

void DomeAdapterPoolManager::newPool(const Pool& pool) throw (DmException) {
  DavixGrabber grabber(factory_->davixPool_);
  DavixStuff *ds(grabber);

  Davix::DavixError *err = NULL;
  Davix::Uri uri(factory_->domehead + "/");
  Davix::PostRequest req(*ds->ctx, uri, &err);
  req.addHeaderField("cmd", "dome_addpool");
  req.addHeaderField("remoteclientdn", this->secCtx_->credentials.clientName);
  req.addHeaderField("remoteclientaddr", this->secCtx_->credentials.remoteAddress);

  boost::property_tree::ptree params;
  params.put("poolname", pool.name);

  std::ostringstream os;
  boost::property_tree::write_json(os, params);

  req.setParameters(*ds->parms);
  req.setRequestBody(os.str());
  int rc = req.executeRequest(&err);

  if(rc || err) {
     std::string msg = SSTR("Error when sending dome_addpool to headnode: " << err->getErrMsg());
     Davix::DavixError::clearError(&err);
     throw DmException(EINVAL, msg);
   }
}

// void DomeAdapterPoolManager::updatePool(const Pool& pool) throw (DmException) {

// }
  
void DomeAdapterPoolManager::deletePool(const Pool& pool) throw (DmException) {
  DavixGrabber grabber(factory_->davixPool_);
  DavixStuff *ds(grabber);

  Davix::DavixError *err = NULL;
  Davix::Uri uri(factory_->domehead + "/");
  Davix::PostRequest req(*ds->ctx, uri, &err);
  req.addHeaderField("cmd", "dome_rmpool");
  req.addHeaderField("remoteclientdn", this->secCtx_->credentials.clientName);
  req.addHeaderField("remoteclientaddr", this->secCtx_->credentials.remoteAddress);

  boost::property_tree::ptree params;
  params.put("poolname", pool.name);

  std::ostringstream os;
  boost::property_tree::write_json(os, params);

  req.setParameters(*ds->parms);
  req.setRequestBody(os.str());
  int rc = req.executeRequest(&err);

  if(rc || err) {
     std::string msg = SSTR("Error when sending dome_rmpool to headnode: " << err->getErrMsg());
     Davix::DavixError::clearError(&err);
     throw DmException(EINVAL, msg);
   }
}

Location DomeAdapterPoolManager::whereToRead(const std::string& path) throw (DmException)
{
  DavixGrabber grabber(factory_->davixPool_);
  DavixStuff *ds(grabber);

  Davix::DavixError *err = NULL;
  Davix::Uri uri(factory_->domehead + "/" + path);
  Davix::GetRequest req(*ds->ctx, uri, &err);
  req.addHeaderField("cmd", "dome_get");
  req.addHeaderField("remoteclientdn", this->secCtx_->credentials.clientName);
  req.addHeaderField("remoteclientaddr", this->secCtx_->credentials.remoteAddress);

  req.setParameters(*ds->parms);
  req.executeRequest(&err);

  if(err) {
    throw DmException(EINVAL, "Error when sending dome_get to headnode: %s", err->getErrMsg().c_str());
   }

  // parse json response
  std::vector<char> body = req.getAnswerContentVec();
  boost::property_tree::ptree jresp = parseJSON(&body[0]);
  std::cout << &body[0] << std::endl;

  try {
    Location loc;

    // extract pools
    using boost::property_tree::ptree;

    ptree::const_iterator begin = jresp.begin();
    ptree::const_iterator end = jresp.end();
    for(ptree::const_iterator it = begin; it != end; it++) {
      std::string host = it->second.get<std::string>("host");
      std::string pfn = it->second.get<std::string>("pfn");

      Chunk chunk(host + ":" + pfn, 0, 0);
      loc.push_back(chunk);
    }
    return loc;
  }
  catch(boost::property_tree::ptree_bad_path &e) {
    Log(Logger::Lvl1, domeadapterlogmask, domeadapterlogname, " Unable to interpret dome_get response:" << &body[0]);
    throw DmException(EINVAL, " Unable to interpret dome_get response: %s", &body[0]);
  }
}

// Location DomeAdapterPoolManager::whereToRead (ino_t inode)             throw (DmException) {
// }

Location DomeAdapterPoolManager::whereToWrite(const std::string& path) throw (DmException) 
{
  DavixGrabber grabber(factory_->davixPool_);
  DavixStuff *ds(grabber);

  Davix::DavixError *err = NULL;
  Davix::Uri uri(factory_->domehead + "/" + path);
  Davix::PostRequest req(*ds->ctx, uri, &err);
  req.addHeaderField("cmd", "dome_put");
  req.addHeaderField("remoteclientdn", this->secCtx_->credentials.clientName);
  req.addHeaderField("remoteclientaddr", this->secCtx_->credentials.remoteAddress);

  boost::property_tree::ptree params;
  params.put("lfn", path);

  std::ostringstream os;
  boost::property_tree::write_json(os, params);

  req.setParameters(*ds->parms);
  req.setRequestBody(os.str());
  req.executeRequest(&err);

  if(err) {
    throw DmException(EINVAL, "Error when sending dome_put to headnode: %s", err->getErrMsg().c_str());
   }

  // parse json response
  std::vector<char> body = req.getAnswerContentVec();
  boost::property_tree::ptree jresp = parseJSON(&body[0]);

  try {
    std::string host = jresp.get<std::string>("host");
    std::string pfn = jresp.get<std::string>("pfn");
    std::string url = host + ":" + pfn;

    Chunk chunk(url, 0, 0);
    Location loc;
    loc.push_back(chunk);
    return loc;
  }
  catch(boost::property_tree::ptree_bad_path &e) {
    Log(Logger::Lvl1, domeadapterlogmask, domeadapterlogname, " Unable to interpret dome_put response:" << &body[0]);
    throw DmException(EINVAL, " Unable to interpret dome_put response: %s", &body[0]);
  }
}

// void DomeAdapterPoolManager::cancelWrite(const Location& loc) throw (DmException) {

// }

static void registerDomeAdapterPools(PluginManager* pm) throw (DmException)
{
  pm->registerPoolManagerFactory(new DomeAdapterPoolsFactory());
  pm->registerPoolDriverFactory(new DomeAdapterPoolsFactory());
}


PluginIdCard plugin_domeadapter_pools = {
  PLUGIN_ID_HEADER,
  registerDomeAdapterPools
};
