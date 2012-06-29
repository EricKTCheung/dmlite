// @file    plugins/oracle/OracleFactories.cpp
/// @brief   Oracle backend for libdm.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstring>
#include <cstdlib>
#include "NsOracle.h"
#include "OracleFactories.h"
#include "UserGroupDbOracle.h"


using namespace dmlite;
using namespace oracle;


NsOracleFactory::NsOracleFactory() throw(DmException):
  nsDb_("cns_db"), user_("root"), passwd_(""), pool_(0x00),
  minPool_(0), maxPool_(1), symLinkLimit_(3)
{
  this->env_ = occi::Environment::createEnvironment(occi::Environment::THREADED_MUTEXED);
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
  else if (key == "OraclePoolMin")
    this->minPool_ = atoi(value.c_str());
  else if (key == "OraclePoolMax")
    this->maxPool_ = atoi(value.c_str());
  else
    throw DmException(DM_UNKNOWN_OPTION, std::string("Unknown option ") + key);
}



INode* NsOracleFactory::createINode(PluginManager*) throw(DmException)
{
  try {
    if (this->pool_ == 0x00)
      this->pool_ = this->env_->createConnectionPool(this->user_, this->passwd_,
                                                     this->nsDb_,
                                                     this->minPool_, this->maxPool_);

    return new INodeOracle(this->pool_,
                           this->pool_->createConnection(this->user_, this->passwd_));
  }
  catch (occi::SQLException& ex) {
    throw DmException(DM_INTERNAL_ERROR, ex.getMessage());
  }
}



UserGroupDb* NsOracleFactory::createUserGroupDb(PluginManager*) throw (DmException)
{
  try {
    if (this->pool_ == 0x00)
      this->pool_ = this->env_->createConnectionPool(this->user_, this->passwd_,
                                                     this->nsDb_,
                                                     this->minPool_, this->maxPool_);

    return new UserGroupDbOracle(this->pool_,
                                 this->pool_->createConnection(this->user_, this->passwd_));
  }
  catch (occi::SQLException& ex) {
    throw DmException(DM_INTERNAL_ERROR, ex.getMessage());
  }
}



static void registerPluginNs(PluginManager* pm) throw(DmException)
{
  pm->registerFactory(static_cast<UserGroupDbFactory*>(new NsOracleFactory()));
  pm->registerFactory(static_cast<INodeFactory*>(new NsOracleFactory()));
}



/// This is what the PluginManager looks for
PluginIdCard plugin_oracle_ns = {
  PLUGIN_ID_HEADER,
  registerPluginNs
};
