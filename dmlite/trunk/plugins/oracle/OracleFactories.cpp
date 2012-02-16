/// @file    plugins/oracle/OracleFactories.cpp
/// @brief   Oracle backend for libdm.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstring>
#include "OracleFactories.h"
#include "NsOracle.h"

#include <stdlib.h>

using namespace dmlite;
using namespace oracle;


NsOracleFactory::NsOracleFactory() throw(DmException):
  nsDb_("cns_db"), user_("root"), passwd_(""), pool_(0x00), symLinkLimit_(3)
{
  this->env_ = occi::Environment::createEnvironment(occi::Environment::DEFAULT);
}



NsOracleFactory::~NsOracleFactory() throw(DmException)
{
  if (this->pool_ != 0x00)
    this->env_->terminateConnectionPool(this->pool_);
  occi::Environment::terminateEnvironment(this->env_);
}



void NsOracleFactory::configure(const std::string& key, const std::string& value) throw(DmException)
{
  if (key == "OracleUsername")
    this->user_ = value;
  else if (key == "OraclePassword")
    this->passwd_ = value;
  else if (key == "OracleDatabase")
    this->nsDb_ = value;
  else if (key == "SymLinkLimit")
    this->symLinkLimit_ = atoi(value.c_str());
  else
    throw DmException(DM_UNKNOWN_OPTION, std::string("Unknown option ") + key);
}



Catalog* NsOracleFactory::createCatalog() throw(DmException)
{
  try {
    if (this->pool_ == 0x00)
      this->pool_ = this->env_->createConnectionPool(this->user_, this->passwd_,
                                                     this->nsDb_);

    return new NsOracleCatalog(this->pool_, this->pool_->createConnection(this->user_, this->passwd_),
                               this->symLinkLimit_);
  }
  catch (occi::SQLException& ex) {
    throw DmException(DM_INTERNAL_ERROR, ex.getMessage());
  }
}



static void registerPluginNs(PluginManager* pm) throw(DmException)
{
  pm->registerCatalogFactory(new NsOracleFactory());
}



/// This is what the PluginManager looks for
PluginIdCard plugin_oracle_ns = {
  API_VERSION,
  registerPluginNs
};
