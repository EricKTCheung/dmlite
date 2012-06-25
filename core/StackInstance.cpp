/// @file   core/PluginManager.cpp
/// @brief  Implementation of dm::StackInstance
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <dmlite/dmlite++.h>

using namespace dmlite;

StackInstance::StackInstance(PluginManager* pm) throw (DmException):
    pluginManager_(pm), secCtx_(0)
{
  try {
    this->ugDb_ = pm->getUserGroupDbFactory()->createUserGroupDb(pm);
  }
  catch (DmException e) {
    if (e.code() != DM_NO_FACTORY)
      throw;
    this->ugDb_ = 0;
  }
  
  try {
    this->inode_ = pm->getINodeFactory()->createINode(pm);
  }
  catch (DmException e) {
    if (e.code() != DM_NO_FACTORY)
      throw;
    this->inode_ = 0;
  }
  
  try {
    this->catalog_ = pm->getCatalogFactory()->createCatalog(pm);
  }
  catch (DmException e) {
    if (e.code() != DM_NO_FACTORY)
      throw;
    this->catalog_ = 0;
  }
  
  try {
    this->poolManager_ = pm->getPoolManagerFactory()->createPoolManager(pm);
  }
  catch (DmException e) {
    if (e.code() != DM_NO_FACTORY)
      throw;
    this->poolManager_ = 0;
  }
  
  // Everything is up, so pass this to the stacks
  if (this->inode_)       this->inode_->setStackInstance(this);
  if (this->catalog_)     this->catalog_->setStackInstance(this);
  if (this->poolManager_) this->poolManager_->setStackInstance(this);
}



StackInstance::~StackInstance() throw ()
{
  if (this->ugDb_)        delete this->ugDb_;
  if (this->inode_)       delete this->inode_;
  if (this->catalog_)     delete this->catalog_;
  if (this->poolManager_) delete this->poolManager_;
  if (this->secCtx_)      delete this->secCtx_;
  
  // Clean pool handlers
  std::map<std::string, PoolDriver*>::iterator i;
  for (i = this->poolDrivers_.begin();
       i != this->poolDrivers_.end();
       ++i) {
    delete i->second;
  }
  this->poolDrivers_.clear();
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



PoolDriver* StackInstance::getPoolDriver(const std::string& poolType) throw (DmException)
{
  // Try from dictionary first
  std::map<std::string, PoolDriver*>::iterator i;
  i = this->poolDrivers_.find(poolType);
  if (i != this->poolDrivers_.end())
    return i->second;
  
  // Instantiate
  PoolDriverFactory* phf = this->pluginManager_->getPoolDriverFactory(poolType);
  PoolDriver* ph = phf->createPoolDriver();
  ph->setStackInstance(this);
  ph->setSecurityContext(this->secCtx_);
  
  this->poolDrivers_[poolType] = ph;
  
  return ph;
}



void StackInstance::setSecurityCredentials(const SecurityCredentials& cred) throw (DmException)
{
  if (this->ugDb_ == 0)
    throw DmException(DM_NO_USERGROUPDB, "There is no plugin that provides createSecurityContext");
  
  if (this->secCtx_) delete this->secCtx_;
  this->secCtx_ = this->ugDb_->createSecurityContext(cred);
  
  if (this->inode_ != 0)
    this->inode_->setSecurityContext(this->secCtx_);
  if (this->catalog_ != 0)
    this->catalog_->setSecurityContext(this->secCtx_);
  if (this->poolManager_ != 0)
    this->poolManager_->setSecurityContext(this->secCtx_);
}



void StackInstance::setSecurityContext(const SecurityContext& ctx) throw (DmException)
{
  if (this->secCtx_) delete this->secCtx_;
  this->secCtx_ = new SecurityContext(ctx);
  
  if (this->inode_ != 0)
    this->inode_->setSecurityContext(this->secCtx_);
  if (this->catalog_ != 0)
    this->catalog_->setSecurityContext(this->secCtx_);
  if (this->poolManager_ != 0)
    this->poolManager_->setSecurityContext(this->secCtx_);
}



const SecurityContext* StackInstance::getSecurityContext() const throw ()
{
  if (this->secCtx_ == 0)
    throw DmException(DM_NO_SECUTIRY_CONTEXT, "The security context has not been initialized");
  return this->secCtx_;
}
