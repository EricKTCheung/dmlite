/// @file    plugins/memcache/MemcachePoolManager.cpp
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>

#include <vector>

#include "MemcachePoolManager.h"
#include "MemcacheFunctions.h"

using namespace dmlite;

MemcachePoolManager::MemcachePoolManager(PoolContainer<memcached_st*>& connPool,
          PoolManager* decorates,
          MemcacheFunctionCounter* funcCounter,
          bool doFuncCount,
          time_t memcachedExpirationLimit) throw(DmException):
  MemcacheCommon(connPool, funcCounter, doFuncCount, memcachedExpirationLimit),
  si_(0x00)
{
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "MemcachePoolManager started.");

  this->decorated_   = decorates;
  this->decoratedId_ = strdup( decorates->getImplId().c_str() );
}



MemcachePoolManager::~MemcachePoolManager()
{
  if (this->decorated_ != 0x00)
    delete this->decorated_;

  if (this->decoratedId_ != 0x00)
    free(this->decoratedId_);
}



std::string MemcachePoolManager::getImplId() const throw ()
{
  std::string implId = "MemcachePoolManager";
  implId += " over ";
  implId += std::string(this->decoratedId_);

  return implId;
}



void MemcachePoolManager::setStackInstance(StackInstance* si) throw (DmException)
{
  BaseInterface::setStackInstance(this->decorated_, si);
  this->si_ = si;
}



void MemcachePoolManager::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  BaseInterface::setSecurityContext(this->decorated_, ctx);
}



std::vector<Pool> MemcachePoolManager::getPools(PoolAvailability availability) throw (DmException)
{
  /*
  std::vector<Pool> pools;
  std::string valMemc;

  const std::string key = keyFromString(key_prefix[PRE_POOL_LIST],
                                        available_pool_key_string[availability]);

  valMemc = safeGetValFromMemcachedKey(key);
  if (!valMemc.empty()) {
    deserializePoolList(valMemc, pools);
  }
  if (pools.size() == 0) {
    DELEGATE_ASSIGN(pools, getPools, availability);

    valMemc = serializePoolList(pools);
    safeSetMemcachedFromKeyValue(key, valMemc);
  }

  return pools;
  */
  incrementFunctionCounter(GETPOOLS_DELEGATE);
  DELEGATE_RETURN(getPools, availability);
}



Pool MemcachePoolManager::getPool(const std::string& poolname) throw (DmException)
{
  /*
  Pool pool;
  std::string valMemc;

  const std::string key = keyFromString(key_prefix[PRE_POOL],
                                        poolname);

  valMemc = safeGetValFromMemcachedKey(key);
  if (!valMemc.empty()) {
    deserializePool(valMemc, pool);
  } else {
    DELEGATE_ASSIGN(pool, getPool, poolname);

    valMemc = serializePool(pool);
    safeSetMemcachedFromKeyValue(key, valMemc);
  }

  return pool;
  */
  incrementFunctionCounter(GETPOOL_DELEGATE);
  DELEGATE_RETURN(getPool, poolname);
}



void MemcachePoolManager::newPool(const Pool& pool) throw (DmException)
{
  incrementFunctionCounter(NEWPOOL_DELEGATE);
  DELEGATE(newPool, pool);
}



void MemcachePoolManager::updatePool(const Pool& pool) throw (DmException)
{
  incrementFunctionCounter(UPDATEPOOL_DELEGATE);
  DELEGATE(updatePool, pool);
}



void MemcachePoolManager::deletePool(const Pool& pool) throw (DmException)
{
  incrementFunctionCounter(DELETEPOOL_DELEGATE);
  DELEGATE(deletePool, pool);
}



Location MemcachePoolManager::whereToRead(const std::string& path) throw (DmException)
{
    incrementFunctionCounter(WHERETOREAD_DELEGATE);
    DELEGATE_RETURN(whereToRead, path);


  /* The Location should not (ever) be cached, as it holds the dmlite security token
   * that must be regenerated on every access.
   * In case that ever changes, here's the code :)
   */

  /*
  incrementFunctionCounter(WHERETOREAD);

  Location loc;
  Chunk chunk;
  SerialChunk pb_chunk;
  std::string valMemc;

  const std::string key = keyFromString(key_prefix[PRE_LOCATION],
                                        path);

  valMemc = safeGetValFromMemcachedKey(key);
  if (!valMemc.empty()) {
    pb_chunk.ParseFromString(valMemc);
    chunk.offset = pb_chunk.offset();
    chunk.size = pb_chunk.size();
    const SerialUrl *p_url = &pb_chunk.url();
    chunk.url.scheme = p_url->scheme();
    chunk.url.domain = p_url->domain();
    chunk.url.port = p_url->port();
    chunk.url.path = p_url->path();
    chunk.url.query["token"] = p_url->token();
    loc = Location(1, chunk);
  } else {
    incrementFunctionCounter(WHERETOREAD_DELEGATE);
    DELEGATE_ASSIGN(loc, whereToRead, path);

    chunk = loc[0];

    pb_chunk.set_offset(chunk.offset);
    pb_chunk.set_size(chunk.size);
    SerialUrl *p_url = pb_chunk.mutable_url();
    p_url->set_scheme(chunk.url.scheme);
    p_url->set_domain(chunk.url.domain);
    p_url->set_port(chunk.url.port);
    p_url->set_path(chunk.url.path);
    p_url->set_token(chunk.url.query.getString("token"));

    valMemc = pb_chunk.SerializeAsString();
    safeSetMemcachedFromKeyValue(key, valMemc);
  }

  return loc;
  */
}



Location MemcachePoolManager::whereToRead(ino_t inode) throw (DmException)
{
  incrementFunctionCounter(WHERETOREAD_DELEGATE);
  DELEGATE_RETURN(whereToRead, inode);
}



Location MemcachePoolManager::whereToWrite(const std::string& path) throw (DmException)
{
  incrementFunctionCounter(WHERETOWRITE_DELEGATE);
  
  Location ret;
  DELEGATE_ASSIGN(ret, whereToWrite, path);
  
  return ret;
}



void MemcachePoolManager::cancelWrite(const Location& loc) throw (DmException)
{
  incrementFunctionCounter(CANCELWRITE_DELEGATE);
  DELEGATE_RETURN(cancelWrite, loc);
}

void MemcachePoolManager::getDirSpaces(const std::string& path, int64_t &totalfree, int64_t &used) throw (DmException)
{
  DELEGATE(getDirSpaces, path, totalfree, used);
}

inline void MemcachePoolManager::incrementFunctionCounter(const int funcName)
{
  if (this->funcCounter_ != 0x00) this->funcCounter_->incr(funcName, &this->randomSeed_);
}








