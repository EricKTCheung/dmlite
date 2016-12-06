/// @file   DomeAdapterDriver.cpp
/// @brief  Dome adapter catalog
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>

#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/catalog.h>
#include <iostream>

#include "DomeAdapter.h"
#include "DomeAdapterDriver.h"
#include "DomeAdapterPools.h"
#include "utils/DomeTalker.h"
#include "utils/DomeUtils.h"

#include "DomeAdapterUtils.h"
#include <boost/property_tree/json_parser.hpp>

using namespace dmlite;
#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

DomeAdapterPoolDriver::DomeAdapterPoolDriver(DomeAdapterFactory *factory) {
  factory_ = factory;
  secCtx_ = NULL;
}

DomeAdapterPoolDriver::~DomeAdapterPoolDriver() {

}

std::string DomeAdapterPoolDriver::getImplId() const throw() {
  return "DomeAdapterPoolDriver";
}

void DomeAdapterPoolDriver::setStackInstance(StackInstance* si) throw (DmException) {
  si_ = si;
}

void DomeAdapterPoolDriver::setSecurityContext(const SecurityContext* secCtx) throw (DmException) {
  secCtx_ = secCtx;

  // Id mechanism
  if (factory_->tokenUseIp_)
    userId_ = secCtx_->credentials.remoteAddress;
  else
    userId_ = secCtx_->credentials.clientName;
}

PoolHandler* DomeAdapterPoolDriver::createPoolHandler(const std::string& poolname) throw (DmException) {
  return new DomeAdapterPoolHandler(this, poolname);
}


DomeAdapterPoolHandler::DomeAdapterPoolHandler(DomeAdapterPoolDriver *driver, const std::string& poolname) {
  driver_ = driver;
  poolname_ = poolname;
}

DomeAdapterPoolHandler::~DomeAdapterPoolHandler() {

}

std::string DomeAdapterPoolHandler::getPoolType(void) throw (DmException) {
  return "filesystem";
}

std::string DomeAdapterPoolHandler::getPoolName(void) throw (DmException) {
  return this->poolname_;
}

void DomeAdapterPoolDriver::toBeCreated(const Pool& pool) throw (DmException) {
  {
  DomeTalker talker(factory_->davixPool_, secCtx_, factory_->domehead_,
                    "POST", "dome_addpool");

  if(!talker.execute("poolname", pool.name)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
  }

  std::vector<boost::any> filesystems = pool.getVector("filesystems");
  for(unsigned i = 0; i < filesystems.size(); ++i) {
    Extensible fs = boost::any_cast<Extensible>(filesystems[i]);

    DomeTalker talker(factory_->davixPool_, secCtx_, factory_->domehead_,
                      "POST", "dome_addfstopool");

    boost::property_tree::ptree params;
    params.put("server", fs.getString("server"));
    params.put("fs", fs.getString("fs"));
    params.put("poolname", pool.name);

    if(!talker.execute(params)) {
      throw DmException(talker.dmlite_code(), talker.err());
    }
  }
}

void DomeAdapterPoolDriver::justCreated(const Pool& pool) throw (DmException) {
  // nothing to do here
}

bool contains_filesystem(std::vector<boost::any> filesystems, std::string server, std::string filesystem) {
  for(unsigned i = 0; i < filesystems.size(); ++i) {
    Extensible fs = boost::any_cast<Extensible>(filesystems[i]);
    if(fs.getString("server") == server && fs.getString("fs") == filesystem) {
      return true;
    }
  }
  return false;
}

void DomeAdapterPoolDriver::update(const Pool& pool) throw (DmException) {
  DomeTalker talker(factory_->davixPool_, secCtx_, factory_->domehead_,
                    "GET", "dome_statpool");

  if(!talker.execute("poolname", pool.name)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }

  try {
    Pool oldpool = deserializePool(talker.jresp().get_child("poolinfo").begin());
    std::vector<boost::any> oldfilesystems = oldpool.getVector("filesystems");
    std::vector<boost::any> filesystems = pool.getVector("filesystems");

    // detect which filesystems have been removed
    for(unsigned i = 0; i < oldfilesystems.size(); i++) {
      Extensible ext = boost::any_cast<Extensible>(oldfilesystems[i]);
      std::string server = ext.getString("server");
      std::string fs = ext.getString("fs");
      if(!contains_filesystem(filesystems, server, fs)) {
        // send rmfs to dome to remove this filesystem
        Log(Logger::Lvl1, domeadapterlogmask, domeadapterlogname, "Removing filesystem '" << fs << "' on server '" << server << "'");

        DomeTalker rmfs(factory_->davixPool_, secCtx_, factory_->domehead_,
                        "POST", "dome_rmfs");

        boost::property_tree::ptree params;
        params.put("server", server);
        params.put("fs", fs);

        if(!rmfs.execute(params)) {
          throw DmException(rmfs.dmlite_code(), rmfs.err());
        }
      }
    }

    // detect which filesystems need to be added
    for(unsigned i = 0; i < filesystems.size(); i++) {
      Extensible ext = boost::any_cast<Extensible>(filesystems[i]);
      std::string server = ext.getString("server");
      std::string fs = ext.getString("fs");

      std::cout << "checking " << server << " - " << fs << std::endl;
      if(!contains_filesystem(oldfilesystems, server, fs)) {
        // send addfs to dome to add this filesystem
        Log(Logger::Lvl1, domeadapterlogmask, domeadapterlogname, "Adding filesystem '" << fs << "' on server '" << server << "'");
        DomeTalker addfs(factory_->davixPool_, secCtx_, factory_->domehead_,
                         "POST", "dome_addfstopool");

        boost::property_tree::ptree params;
        params.put("server", server);
        params.put("fs", fs);
        params.put("poolname", pool.name);

        if(!addfs.execute(params)) {
          throw DmException(addfs.dmlite_code(), addfs.err());
        }
      }
    }
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(EINVAL, SSTR("Error when parsing json response: " << talker.response()));
  }
}

void DomeAdapterPoolDriver::toBeDeleted(const Pool& pool) throw (DmException) {
  DomeTalker talker(factory_->davixPool_, secCtx_, factory_->domehead_,
                    "POST", "dome_rmpool");

  if(!talker.execute("poolname", pool.name)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

uint64_t DomeAdapterPoolHandler::getPoolField(const std::string &field, uint64_t def) throw (DmException) {
  DomeTalker talker(driver_->factory_->davixPool_, driver_->secCtx_, driver_->factory_->domehead_,
                    "GET", "dome_statpool");

  if(!talker.execute("poolname", poolname_)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }

  try {
    return talker.jresp().get_child("poolinfo").begin()->second.get<uint64_t>(field, def);
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(DMLITE_SYSERR(DMLITE_MALFORMED), SSTR("Error parsing json response when retrieving field '" << field << "'. Error: '" << e.what() << "' Response: '" << talker.response() << "'"));
  }
}

uint64_t DomeAdapterPoolHandler::getTotalSpace(void) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " Entering ");
  return this->getPoolField("physicalsize", 0);
}

uint64_t DomeAdapterPoolHandler::getFreeSpace(void) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " Entering ");
  return this->getPoolField("freespace", 0);
}

bool DomeAdapterPoolHandler::poolIsAvailable(bool write) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " Entering ");
  uint64_t poolstatus = this->getPoolField("poolstatus", std::numeric_limits<uint64_t>::max());

  if(poolstatus == FsStaticActive) {
    return true;
  }
  if(poolstatus == FsStaticDisabled) {
    return false;
  }
  if(poolstatus == FsStaticReadOnly) {
    return ! write;
  }

  throw DmException(EINVAL, SSTR("Received invalid value from Dome for poolstatus: " << poolstatus));
}

bool DomeAdapterPoolHandler::replicaIsAvailable(const Replica& replica) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " rfn: " << replica.rfn);

  if (replica.status != dmlite::Replica::kAvailable) {
    Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, " poolname:" << poolname_ << " replica: " << replica.rfn << " has status " << replica.status << " . returns false");
    return false;
  }

  DomeTalker talker(driver_->factory_->davixPool_, driver_->secCtx_, driver_->factory_->domehead_,
                    "GET", "dome_statpool");

  if(!talker.execute("poolname", poolname_)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }

  try {
    using namespace boost::property_tree;
    std::string filesystem = Extensible::anyToString(replica["filesystem"]);
    ptree fsinfo = talker.jresp().get_child("poolinfo").get_child(poolname_).get_child("fsinfo");

    ptree::const_iterator begin = fsinfo.begin();
    ptree::const_iterator end = fsinfo.end();
    for(ptree::const_iterator it = begin; it != end; it++) {
      if(it->first == replica.server) {
        for(ptree::const_iterator it2 = it->second.begin(); it2 != it->second.end(); it2++) {
          if(it2->first == filesystem) {
            bool active = it2->second.get<int>("fsstatus") != FsStaticDisabled;
            return active;
          }
        }
      }
    }
    return false;
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(EINVAL, SSTR("Error when parsing json response: " << talker.response()));
  }
}

void DomeAdapterPoolHandler::removeReplica(const Replica& replica) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " rfn: " << replica.rfn);
  DomeTalker talker(driver_->factory_->davixPool_, driver_->secCtx_, driver_->factory_->domehead_,
                    "POST", "dome_delreplica");

  boost::property_tree::ptree params;
  params.put("server", DomeUtils::server_from_rfio_syntax(replica.rfn));
  params.put("pfn", DomeUtils::pfn_from_rfio_syntax(replica.rfn));

  if(!talker.execute(params)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

void DomeAdapterPoolHandler::cancelWrite(const Location& loc) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " Entering. ");
  Replica replica;
  replica.rfn = loc[0].url.domain + ":" + loc[0].url.path;
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " rfn: " << replica.rfn);
  this->removeReplica(replica);
}

Location DomeAdapterPoolHandler::whereToWrite(const std::string& lfn) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " lfn: " << lfn);
  DomeTalker talker(driver_->factory_->davixPool_, driver_->secCtx_, driver_->factory_->domehead_,
                    "POST", "dome_put");

  if(!talker.execute("pool", poolname_, "lfn", lfn)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }

  try {
    Chunk single;

    single.url.domain = talker.jresp().get<std::string>("host");
    single.url.path   = talker.jresp().get<std::string>("pfn");
    single.offset = 0;
    single.size   = 0;

    single.url.query["sfn"]      = lfn;
    
    std::string userId1;
    if (driver_->si_->contains("replicate"))
      userId1 = dmlite::kGenericUser;
    else
      userId1 = driver_->userId_;
    
    single.url.query["token"]    = dmlite::generateToken(userId1, single.url.path,
                                                         driver_->factory_->tokenPasswd_,
                                                         driver_->factory_->tokenLife_, true);
    return Location(1, single);
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(EINVAL, SSTR("Error when parsing json response: " << e.what() << " - " << talker.response()));
  }
}

Location DomeAdapterPoolHandler::whereToRead(const Replica& replica) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " poolname:" << poolname_ << " replica:" << replica.rfn);

  Url rloc(replica.rfn);

  Chunk single;

  single.url.domain = rloc.domain;
  single.url.path   = rloc.path;
  single.offset = 0;

  try {
    single.size   = this->driver_->si_->getCatalog()->extendedStatByRFN(replica.rfn).stat.st_size;
  }
  catch (DmException& e) {
    switch (DMLITE_ERRNO(e.code())) {
      case ENOSYS: case DMLITE_NO_INODE:
        break;
      default:
        throw;
    }
    single.size = 0;
  }

  single.url.query["token"] = dmlite::generateToken(driver_->userId_, rloc.path,
                                                    driver_->factory_->tokenPasswd_,
                                                    driver_->factory_->tokenLife_);

  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " poolname:" << poolname_ << " replica:" << replica.rfn << " returns" << single.toString());
  return Location(1, single);
}
