/// @file   DomeAdapterIO.cpp
/// @brief  Filesystem IO with dome
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/pooldriver.h>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
    

#include "DomeAdapterPools.h"

using namespace dmlite;

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
  return NULL;
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
  return PoolManager::kNone;
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
  std::cout << &body[0] << std::endl;

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

// void DomeAdapterPoolManager::newPool(const Pool& pool) throw (DmException) {

// }

// void DomeAdapterPoolManager::updatePool(const Pool& pool) throw (DmException) {

// }
  
// void DomeAdapterPoolManager::deletePool(const Pool& pool) throw (DmException) {

// }

Location DomeAdapterPoolManager::whereToRead (const std::string& path) throw (DmException) {

}

Location DomeAdapterPoolManager::whereToRead (ino_t inode)             throw (DmException) {

}

Location DomeAdapterPoolManager::whereToWrite(const std::string& path) throw (DmException) 
{
  DavixGrabber grabber(factory_->davixPool_);
  DavixStuff *ds(grabber);

  Davix::DavixError *err = NULL;
  Davix::Uri uri(factory_->domehead + "/" + path);
  Davix::GetRequest req(*ds->ctx, uri, &err);
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

  std::vector<char> body = req.getAnswerContentVec();
  std::cout << &body[0] << std::endl;

}

// void DomeAdapterPoolManager::cancelWrite(const Location& loc) throw (DmException) {

// }

static void registerDomeAdapterPools(PluginManager* pm) throw (DmException)
{
  pm->registerPoolManagerFactory(new DomeAdapterPoolsFactory());
}


PluginIdCard plugin_domeadapter_pools = {
  PLUGIN_ID_HEADER,
  registerDomeAdapterPools
};