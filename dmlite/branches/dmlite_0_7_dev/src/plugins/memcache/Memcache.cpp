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
#include "MemcacheFunctionCounter.h"

using namespace dmlite;

MemcacheConnectionFactory::MemcacheConnectionFactory(std::vector<std::string> hosts,
    bool useBinaryProtocol,
    std::string dist):
  hosts_(hosts),
  useBinaryProtocol_(useBinaryProtocol),
  dist_(dist)
{
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
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: %s: %s",
        "configuring a memcache connection failed",
        "setting binary/ascii protocol",
        memcached_strerror(c, memc_return_val));
  }

  if (dist_ == "consistent")
    memc_return_val =  memcached_behavior_set(c, MEMCACHED_BEHAVIOR_DISTRIBUTION, MEMCACHED_DISTRIBUTION_CONSISTENT);

  if (memc_return_val != MEMCACHED_SUCCESS) {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: %s: %s",
        "configuring a memcache connection failed",
        "setting the distribution",
        memcached_strerror(c, memc_return_val));
  }

  // make sure that NOREPLY is deactivated, otherwise deletes will hang
  memc_return_val =  memcached_behavior_set(c, MEMCACHED_BEHAVIOR_NOREPLY, 0);

  if (memc_return_val != MEMCACHED_SUCCESS) {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: %s: %s",
        "configuring a memcache connection failed",
        "unsetting noreply behaviour",
        memcached_strerror(c, memc_return_val));
  }

  memc_return_val =  memcached_behavior_set(c, MEMCACHED_BEHAVIOR_NO_BLOCK, 1);

  if (memc_return_val != MEMCACHED_SUCCESS) {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: %s: %s",
        "configuring a memcache connection failed",
        "setting no block behaviour",
        memcached_strerror(c, memc_return_val));
  }

  /* comment out: not supported in the libmemcached version on EPEL, compile will fail
  memc_return_val =  memcached_behavior_set(c, MEMCACHED_BEHAVIOR_KEEPALIVE, 1);

  if (memc_return_val != MEMCACHED_SUCCESS) {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: %s: %s",
        "configuring a memcache connection failed",
        "setting keepalive (prob version of libmemcached too old",
        memcached_strerror(c, memc_return_val));
  }
  */

  // Add memcached TCP hosts
  std::vector<std::string>::iterator i;
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
      syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: %s: %s = %s",
          "creating a memcache connection failed",
          "adding a server failed",
          "could not parse value",
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
      syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: %s: %s",
          "creating a memcache connection failed",
          "adding a server failed",
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

MemcacheFactory::MemcacheFactory(CatalogFactory* catalogFactory) throw (DmException):
  nestedFactory_(catalogFactory),
  connectionFactory_(std::vector<std::string>(), true, "default"),
  connectionPool_(&connectionFactory_, 50),
  funcCounter_(0x00),
  doFuncCount_(false),
  funcCounterLogFreq_(18),
  symLinkLimit_(3),
  memcachedExpirationLimit_(60),
  memcachedPOSIX_(false)
{
  /*
  this->funcCounter_ = new MemcacheFunctionCounter();
  */
}

MemcacheFactory::~MemcacheFactory() throw(DmException)
{
  if (this->funcCounter_ != 0x00)
    delete this->funcCounter_;

}



void MemcacheFactory::configure(const std::string& key, const std::string& value) throw(DmException)
{
  if (key == "MemcachedServer")
    this->connectionFactory_.hosts_.push_back(value);
  else if (key == "SymLinkLimit")
    this->symLinkLimit_ = atoi(value.c_str());
  else if (key == "MemcachedExpirationLimit") {
    unsigned int expLimit = atoi(value.c_str());
    // 60*60*24*30 = 30 days from which on the expiration limit
    // will be treated as a timestamp by memcached
    if (expLimit >= 0 && expLimit < 60*60*24*30)
      this->memcachedExpirationLimit_ = expLimit;
    else
      this->memcachedExpirationLimit_ = DEFAULT_MEMCACHED_EXPIRATION;

  } else if (key == "MemcachedHashDistribution") {
    if (value == "consistent" || value == "default")
      this->connectionFactory_.dist_ = value;
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
  } else
    throw DmException(DMLITE_CFGERR(DMLITE_UNKNOWN_KEY),
        std::string("Unknown option ") + key);
}



Catalog* MemcacheFactory::createCatalog(PluginManager* pm) throw(DmException)
{
  Catalog* nested = 0x00;

  if (this->nestedFactory_ != 0x00)
    nested = CatalogFactory::createCatalog(this->nestedFactory_, pm);
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



static void registerPluginMemcache(PluginManager* pm) throw(DmException)
{
  CatalogFactory* nested = pm->getCatalogFactory();

  if (nested == NULL)
    throw DmException(DMLITE_SYSERR(DMLITE_NO_CATALOG),
        std::string("Memcache cannot be loaded first"));

  pm->registerCatalogFactory(new MemcacheFactory(nested));
}



/// This is what the PluginManager looks for
PluginIdCard plugin_memcache = {
  PLUGIN_ID_HEADER,
  registerPluginMemcache
};
