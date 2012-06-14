/// @file   core/PluginManager.cpp
/// @brief  Implementation of dm::StackInstance
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <dmlite/dmlite++.h>

using namespace dmlite;

StackInstance::StackInstance(PluginManager* pm) throw (DmException):
    pluginManager_(pm), secCtx_(0)
{
  try {
    this->ugDb_ = pm->getUserGroupDbFactory()->createUserGroupDb(this);
  }
  catch (DmException e) {
    if (e.code() != DM_NO_FACTORY)
      throw;
    this->ugDb_ = 0;
  }
  
  try {
    this->inode_ = pm->getINodeFactory()->createINode(this);
  }
  catch (DmException e) {
    if (e.code() != DM_NO_FACTORY)
      throw;
    this->inode_ = 0;
  }
  
  try {
    this->catalog_ = pm->getCatalogFactory()->createCatalog(this);
  }
  catch (DmException e) {
    if (e.code() != DM_NO_FACTORY)
      throw;
    this->catalog_ = 0;
  }
  
  try {
    this->poolManager_ = pm->getPoolManagerFactory()->createPoolManager(this);
  }
  catch (DmException e) {
    if (e.code() != DM_NO_FACTORY)
      throw;
    this->poolManager_ = 0;
  }
  
}



StackInstance::~StackInstance() throw ()
{
  if (this->ugDb_)        delete this->ugDb_;
  if (this->inode_)       delete this->inode_;
  if (this->catalog_)     delete this->catalog_;
  if (this->poolManager_) delete this->poolManager_;
  if (this->secCtx_)      delete this->secCtx_;
  
  // Clean pool handlers
  std::map<std::string, PoolHandler*>::iterator i;
  for (i = this->poolHandlers_.begin();
       i != this->poolHandlers_.end();
       ++i) {
    delete i->second;
  }
  this->poolHandlers_.clear();
}



void StackInstance::set(const std::string& key, Value& value) throw (DmException)
{
  this->stackMsg_.insert(std::pair<std::string, Value>(key, value));
}



Value StackInstance::get(const std::string& key) throw (DmException)
{
  std::map<std::string, Value>::const_iterator i;
  i = this->stackMsg_.find(key);
  if (i == this->stackMsg_.end())
    throw DmException(DM_UNKNOWN_KEY, "Key " + key + " not found in the Stack configuration");
  return i->second;
}



void StackInstance::erase(const std::string& key) throw (DmException)
{
  this->stackMsg_.erase(key);
}



PluginManager* StackInstance::getPluginManager() throw (DmException)
{
  return this->pluginManager_;
}



UserGroupDb* StackInstance::getUserGroupDb() throw (DmException)
{
  if (this->ugDb_ == 0)
    throw DmException(DM_NO_USERGROUPDB, "No plugin provides UserGroupDb");
  return this->ugDb_;
}



INode* StackInstance::getINode() throw (DmException)
{
  if (this->inode_ == 0)
    throw DmException(DM_NO_INODE, "No plugin provides INode");
  return this->inode_;
}



Catalog* StackInstance::getCatalog() throw (DmException)
{
  if (this->catalog_ == 0)
    throw DmException(DM_NO_CATALOG, "No plugin provides Catalog");
  return this->catalog_;
}



bool StackInstance::isTherePoolManager() throw ()
{
  return this->poolManager_ != 0x00;
}



PoolManager* StackInstance::getPoolManager() throw (DmException)
{
  if (this->poolManager_ == 0)
    throw DmException(DM_NO_POOL_MANAGER, "No plugin provides PoolManager");
  return this->poolManager_;
}



PoolHandler* StackInstance::getPoolHandler(const Pool& pool) throw (DmException)
{
  // Try from dictionary first
  std::map<std::string, PoolHandler*>::iterator i;
  i = this->poolHandlers_.find(pool.pool_name);
  if (i != this->poolHandlers_.end())
    return i->second;
  
  // Instantiate
  PoolHandlerFactory* phf = this->pluginManager_->getPoolHandlerFactory(pool.pool_type);
  PoolHandler* ph = phf->createPoolHandler(this, pool);
  
  this->poolHandlers_[pool.pool_name] = ph;
  
  return ph;
}



void StackInstance::setSecurityCredentials(const SecurityCredentials& cred) throw (DmException)
{
  if (this->catalog_ == 0)
    throw DmException(DM_NO_CATALOG, "No catalog to initialize the security context");

  if (this->ugDb_ == 0)
    throw DmException(DM_NO_USERGROUPDB, "There is no plugin that provides user and group mapping");
  
  this->secCtx_ = this->ugDb_->createSecurityContext(cred);
  this->catalog_->setSecurityContext(this->secCtx_);
  
  if (this->poolManager_ != 0)
    this->poolManager_->setSecurityContext(this->secCtx_);
}



void StackInstance::setSecurityContext(const SecurityContext& ctx) throw (DmException)
{
  if (this->secCtx_) delete this->secCtx_;
  this->secCtx_ = new SecurityContext(ctx);
  
  if (this->catalog_ != 0)
    this->catalog_->setSecurityContext(this->secCtx_);
  if (this->poolManager_ != 0)
    this->poolManager_->setSecurityContext(this->secCtx_);
}



const SecurityContext* StackInstance::getSecurityContext() throw ()
{
  return this->secCtx_;
}

