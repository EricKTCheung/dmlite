/// @file    plugins/memcache/Memcache.cpp
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
#include <algorithm>
#include <cstring>
#include <stdlib.h>
#include <sstream>
#include <vector>
#include <boost/algorithm/string.hpp>

#include "Memcache.h"
#include "MemcacheCatalog.h"
#include "MemcachePoolManager.h"
#include "MemcacheFunctionCounter.h"

Logger::bitmask dmlite::memcachelogmask = 0;
Logger::component dmlite::memcachelogname = "Memcache";

using namespace dmlite;


MemcacheConnectionFactory::MemcacheConnectionFactory(std::set<std::string> hosts,
    bool useBinaryProtocol,
    std::string dist):
  hosts_(hosts),
  useBinaryProtocol_(useBinaryProtocol),
  dist_(dist)
{
  memcachelogmask = Logger::get()->getMask(memcachelogname);

  // Nothing
}

MemcacheConnectionFactory::~MemcacheConnectionFactory()
{
  // Nothing
}

memcached_st* MemcacheConnectionFactory::create() throw ()
{
  memcached_st* c;
  memcached_return memc_return_val;

  // Passing NULL means dynamically allocating space
  c = memcached_create(NULL);

  // Configure the memcached behaviour
  if (useBinaryProtocol_)
    memc_return_val =  memcached_behavior_set(c, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1);
  else
    memc_return_val =  memcached_behavior_set(c, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 0);

  if (memc_return_val != MEMCACHED_SUCCESS) {
    Err(memcachelogname, "configuring a memcache connection failed: " <<
        "setting binary/ascii protocol: " <<
        memcached_strerror(c, memc_return_val));
  }

  if (dist_ == "consistent")
    memc_return_val =  memcached_behavior_set(c, MEMCACHED_BEHAVIOR_DISTRIBUTION, MEMCACHED_DISTRIBUTION_CONSISTENT);

  if (memc_return_val != MEMCACHED_SUCCESS) {
    Err(memcachelogname, "configuring a memcache connection failed: " <<
        "setting the distribution: " <<
        memcached_strerror(c, memc_return_val));
  }

  // make sure that NOREPLY is deactivated, otherwise deletes will hang
  memc_return_val =  memcached_behavior_set(c, MEMCACHED_BEHAVIOR_NOREPLY, 0);

  if (memc_return_val != MEMCACHED_SUCCESS) {
    Err(memcachelogname, "configuring a memcache connection failed: " <<
        "unsetting noreply behaviour: " <<
        memcached_strerror(c, memc_return_val));
  }

  memc_return_val =  memcached_behavior_set(c, MEMCACHED_BEHAVIOR_NO_BLOCK, 1);

  if (memc_return_val != MEMCACHED_SUCCESS) {
    Err(memcachelogname, "configuring a memcache connection failed: " <<
        "setting no block behaviour: " <<
        memcached_strerror(c, memc_return_val));
  }

  /* comment out: not supported in the libmemcached version on EPEL, compile will fail
  memc_return_val =  memcached_behavior_set(c, MEMCACHED_BEHAVIOR_KEEPALIVE, 1);

  if (memc_return_val != MEMCACHED_SUCCESS) {
    Err(memcachelogname, "configuring a memcache connection failed: " <<
        "setting keepalive behaviour: " <<
        memcached_strerror(c, memc_return_val));
  }
  */

  // Add memcached TCP hosts
  std::set<std::string>::iterator i;
  for (i = this->hosts_.begin(); i != this->hosts_.end(); i++) {
    // split host and port
    const char* host;
    unsigned int port = MEMCACHED_DEFAULT_PORT;
    double weight;
    std::vector<std::string> server;
    boost::split(server, *i, boost::is_any_of(":/?"));

    if (server.size() > 0) {
      host = server[0].c_str();
    } else {
      Err(memcachelogname, "creating a memcache connection failed: " <<
          "adding a server failed: " <<
          "could not parse value: " <<
          i->c_str());
      continue;
    }
    if (server.size() > 1) {
      port = atoi(server[1].c_str());
    }
    if (server.size() > 2) {
      weight = atof(server[2].c_str());
    }

    memc_return_val = memcached_server_add(c, host, port);
    if (memc_return_val != MEMCACHED_SUCCESS) {
      Err(memcachelogname, "creating a memcache connection failed: " <<
          "adding a server failed: " <<
          memcached_strerror(c, memc_return_val));
    }
  }

  return c;
}

void MemcacheConnectionFactory::destroy(memcached_st* c) throw ()
{
  memcached_free(c);
}

bool MemcacheConnectionFactory::isValid(memcached_st* c) throw ()
{
  // libmemcached will automatically initiate a new connection.
  return true;
}

MemcacheFactory::MemcacheFactory(CatalogFactory* catalogFactory,
                                 PoolManagerFactory* poolManagerFactory) throw (DmException):
  nestedCatalogFactory_(catalogFactory),
  nestedPoolManagerFactory_(poolManagerFactory),
  connectionFactory_(std::set<std::string>(), true, "default"),
  connectionPool_(&connectionFactory_, 250),
  funcCounter_(0x00),
  doFuncCount_(false),
  funcCounterLogFreq_(18),
  symLinkLimit_(3),
  memcachedExpirationLimit_(60),
  memcachedPOSIX_(false)
{
  memcachelogmask = Logger::get()->getMask(memcachelogname);
  Log(Logger::Lvl0, memcachelogmask, memcachelogname, "MemcacheFactory started.");
}

MemcacheFactory::~MemcacheFactory()
{
  if (this->funcCounter_ != 0x00)
    delete this->funcCounter_;

}



void MemcacheFactory::configure(const std::string& key, const std::string& value) throw(DmException)
{

  LogCfgParm(Logger::Lvl4,  memcachelogmask, memcachelogname, key, value);

  bool gotit = true;

  if (key == "MemcachedServer") {

    this->connectionFactory_.hosts_.insert(value);

  }
  else if (key == "SymLinkLimit") {

    this->symLinkLimit_ = atoi(value.c_str());

  }
  else if (key == "MemcachedExpirationLimit") {

    unsigned int expLimit = atoi(value.c_str());
    // 60*60*24*30 = 30 days from which on the expiration limit
    // will be treated as a timestamp by memcached
    // >= 0 is implicit because it's an unsigned value
    if (expLimit < 60*60*24*30) {
      Log(Logger::Lvl1, memcachelogmask, memcachelogname, "Setting MemcachedExpirationLimit :" << expLimit);
      this->memcachedExpirationLimit_ = expLimit;
    }
    else {
      Log(Logger::Lvl1, memcachelogmask, memcachelogname, "Setting MemcachedExpirationLimit :" << DEFAULT_MEMCACHED_EXPIRATION);
      this->memcachedExpirationLimit_ = DEFAULT_MEMCACHED_EXPIRATION;
    }

  } else if (key == "MemcachedHashDistribution") {
    if (value == "consistent" || value == "default") {

      this->connectionFactory_.dist_ = value;
    }
    else
      throw DmException(DMLITE_CFGERR(EINVAL),
          std::string("Unknown option value ") + value);

  } else if (key == "MemcachedProtocol") {

    if (value == "ascii")
      this->connectionFactory_.useBinaryProtocol_ = false;
    else
      this->connectionFactory_.useBinaryProtocol_ = true;
  }
  else if (key == "MemcachedPOSIX") {

    if (value == "on")
      this->memcachedPOSIX_ = true;
    else if (value == "off")
      this->memcachedPOSIX_ = false;
    else
      throw DmException(DMLITE_CFGERR(EINVAL),
          std::string("Unknown option value ") + value);

  } else if (key == "MemcachedFunctionCounter") {

    if (value == "on") {
      this->doFuncCount_ = true;
    }

  } else if (key == "MemcachedFunctionCounterLogFrequency") {

    this->funcCounterLogFreq_ = atoi(value.c_str());

  } else if (key == "MemcachedPoolSize") {

    this->connectionPool_.resize(atoi(value.c_str()));

  } else if (key == "LocalCacheSize") {

    MemcacheCommon::localCacheMaxSize = atoi(value.c_str());

  }
  else gotit = false;

  if (gotit)
    LogCfgParm(Logger::Lvl4,  memcachelogmask, memcachelogname, key, value);
}



Catalog* MemcacheFactory::createCatalog(PluginManager* pm) throw(DmException)
{
  Catalog* nested = 0x00;

  if (this->nestedCatalogFactory_ != 0x00)
    nested = CatalogFactory::createCatalog(this->nestedCatalogFactory_, pm);
  else
    return 0x00;

  if (this->funcCounter_ == 0x00 && this->doFuncCount_)
    this->funcCounter_ = new MemcacheFunctionCounter(this->funcCounterLogFreq_);

  return new MemcacheCatalog(this->connectionPool_,
      nested,
      this->funcCounter_,
      this->doFuncCount_,
      this->symLinkLimit_,
      this->memcachedExpirationLimit_,
      this->memcachedPOSIX_);
}



PoolManager* MemcacheFactory::createPoolManager(PluginManager* pm) throw (DmException)
{
  PoolManager *nested;
  if (this->nestedPoolManagerFactory_ != 0x00)
    nested = PoolManagerFactory::createPoolManager(this->nestedPoolManagerFactory_, pm);
  else
    return 0x00;

  if (this->funcCounter_ == 0x00 && this->doFuncCount_)
    this->funcCounter_ = new MemcacheFunctionCounter(this->funcCounterLogFreq_);

  Log(Logger::Lvl4, memcachelogmask, memcachelogname,
      "Creating MemcachePoolManager");

  return new MemcachePoolManager(this->connectionPool_,
      nested,
      this->funcCounter_,
      this->doFuncCount_,
      this->memcachedExpirationLimit_);
}


static void registerPluginMemcache(PluginManager* pm) throw(DmException)
{
  CatalogFactory* nestedCAT = pm->getCatalogFactory();

  if (nestedCAT == NULL)
    throw DmException(DMLITE_SYSERR(DMLITE_NO_CATALOG),
        std::string("Memcache cannot be loaded first"));

  PoolManagerFactory* nestedPM = pm->getPoolManagerFactory();

  if (nestedPM == NULL)
    throw DmException(DMLITE_SYSERR(DMLITE_NO_POOL_MANAGER),
        std::string("Memcache cannot be loaded first"));

  MemcacheFactory *mf = new MemcacheFactory(nestedCAT, nestedPM);
  pm->registerCatalogFactory(mf);
  pm->registerPoolManagerFactory(mf);
}


static void registerPluginMemcacheNs(PluginManager* pm) throw(DmException)
{
  CatalogFactory* nestedCAT = pm->getCatalogFactory();

  if (nestedCAT == NULL)
    throw DmException(DMLITE_SYSERR(DMLITE_NO_CATALOG),
        std::string("Memcache cannot be loaded first"));

  MemcacheFactory *mf = new MemcacheFactory(nestedCAT, 0x00);
  pm->registerCatalogFactory(mf);
}


static void registerPluginMemcachePm(PluginManager* pm) throw(DmException)
{
  PoolManagerFactory* nestedPM = pm->getPoolManagerFactory();

  if (nestedPM == NULL)
    throw DmException(DMLITE_SYSERR(DMLITE_NO_POOL_MANAGER),
        std::string("Memcache cannot be loaded first"));

  MemcacheFactory *mf = new MemcacheFactory(0x00, nestedPM);
  pm->registerPoolManagerFactory(mf);
}



/// This is what the PluginManager looks for
PluginIdCard plugin_memcache = {
  PLUGIN_ID_HEADER,
  registerPluginMemcache
};


PluginIdCard plugin_memcache_ns = {
  PLUGIN_ID_HEADER,
  registerPluginMemcacheNs
};


PluginIdCard plugin_memcache_pm = {
  PLUGIN_ID_HEADER,
  registerPluginMemcachePm
};
