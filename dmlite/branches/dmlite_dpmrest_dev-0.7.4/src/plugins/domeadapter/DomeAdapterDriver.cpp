/// @file   DomeAdapterDriver.cpp
/// @brief  Dome adapter catalog
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>

#include <dmlite/cpp/dmlite.h>
#include <iostream>

#include "utils/DavixPool.h"
#include "DomeAdapterDriver.h"

using namespace dmlite;

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
