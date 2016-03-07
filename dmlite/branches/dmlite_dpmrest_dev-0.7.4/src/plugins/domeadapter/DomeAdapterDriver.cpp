/// @file   DomeAdapterDriver.cpp
/// @brief  Dome adapter catalog
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>

#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/catalog.h>
#include <iostream>

#include "DomeAdapterDriver.h"
#include "DomeAdapterPools.h"
#include "utils/DomeTalker.h"
#include "utils/DomeUtils.h"

#include <boost/property_tree/json_parser.hpp>

using namespace dmlite;
#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

DomeAdapterPoolDriver::DomeAdapterPoolDriver(DomeAdapterPoolsFactory *factory) {
  factory_ = factory;
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
  std::cout << "inside pool handler constructor" << std::endl;
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

uint64_t DomeAdapterPoolHandler::getPoolField(std::string field) throw (DmException) {
  Davix::Uri uri(driver_->factory_->domehead_ + "/");
  DomeTalker talker(driver_->factory_->davixPool_, driver_->secCtx_,
                    "GET", uri, "dome_statpool");

  if(!talker.execute("poolname", poolname_)) {
    throw DmException(EINVAL, talker.err());
  }

  try {
    return talker.jresp().get_child("poolinfo").begin()->second.get<uint64_t>(field);
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(DMLITE_NO_SUCH_POOL, SSTR("Pool " + poolname_ + " not found. (" << e.what() << ")"));
  }
}

uint64_t DomeAdapterPoolHandler::getTotalSpace(void) throw (DmException) {
  return this->getPoolField("physicalsize");
}

uint64_t DomeAdapterPoolHandler::getFreeSpace(void) throw (DmException) {
  return this->getPoolField("freespace");
}

bool DomeAdapterPoolHandler::poolIsAvailable(bool write) throw (DmException) {
  uint64_t poolstatus = this->getPoolField("poolstatus");

  enum DomeFsStatus {
    FsStaticActive = 0,
    FsStaticDisabled,
    FsStaticReadOnly
  };

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

void DomeAdapterPoolHandler::removeReplica(const Replica& replica) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " rfn: " << replica.rfn);
  Davix::Uri uri(driver_->factory_->domehead_ + "/");
  DomeTalker talker(driver_->factory_->davixPool_, driver_->secCtx_,
                    "POST", uri, "dome_delreplica");

  boost::property_tree::ptree params;
  params.put("server", DomeUtils::server_from_rfio_syntax(replica.rfn));
  params.put("pfn", DomeUtils::pfn_from_rfio_syntax(replica.rfn));

  if(!talker.execute(params)) {
    throw DmException(EINVAL, talker.err());
  }
}

Location DomeAdapterPoolHandler::whereToWrite(const std::string& lfn) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, " lfn: " << lfn);
  Davix::Uri uri(driver_->factory_->domehead_ + "/" + lfn);
  DomeTalker talker(driver_->factory_->davixPool_, driver_->secCtx_,
                    "POST", uri, "dome_put");

  if(!talker.execute("pool", poolname_)) {
    throw DmException(EINVAL, talker.err());
  }

  try {
    Chunk single;

    single.url.domain = talker.jresp().get<std::string>("host");
    single.url.path   = talker.jresp().get<std::string>("pfn");
    single.offset = 0;
    single.size   = 0;
    
    single.url.query["sfn"]      = lfn;
    single.url.query["token"]    = dmlite::generateToken(driver_->userId_, single.url.path,
                                                         driver_->factory_->tokenPasswd_,
                                                         driver_->factory_->tokenLife_, true);
    return Location(1, single);
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(EINVAL, SSTR("Error when parsing json response: " << e.what() << " - " << &talker.response()[0]));
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
