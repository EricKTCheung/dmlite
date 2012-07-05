/// @file    plugins/memcache/Memcache.h
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
#include <algorithm>
#include <cstring>
#include <stdlib.h>
#include "Memcache.h"

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

memcached_st* MemcacheConnectionFactory::create()
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

	if (memc_return_val != MEMCACHED_SUCCESS)
			throw MemcacheException(memc_return_val, c);

  if (dist_ == "consistent")
    memc_return_val =  memcached_behavior_set(c, MEMCACHED_BEHAVIOR_DISTRIBUTION, MEMCACHED_DISTRIBUTION_CONSISTENT);

	if (memc_return_val != MEMCACHED_SUCCESS)
			throw MemcacheException(memc_return_val, c);

  // make sure that NOREPLY is deactivated, otherwise deletes will hang
  memc_return_val =  memcached_behavior_set(c, MEMCACHED_BEHAVIOR_NOREPLY, 0);

	if (memc_return_val != MEMCACHED_SUCCESS)
			throw MemcacheException(memc_return_val, c);

  memc_return_val =  memcached_behavior_set(c, MEMCACHED_BEHAVIOR_NO_BLOCK, 1);

	if (memc_return_val != MEMCACHED_SUCCESS)
			throw MemcacheException(memc_return_val, c);

	// Add memcached TCP hosts
	std::vector<std::string>::iterator i;
	for (i = this->hosts_.begin(); i != this->hosts_.end(); i++) {
		// split host and port
		char* host;
		unsigned int port;
		double weight;
		char server[i->length()+1];
		std::memcpy(server, i->c_str(),i->length());
		char* token;

		token = strtok(server, ":/?");
		if (token != NULL) {
			host = token;
		}
		token = strtok(NULL, ":/?");
		if (token != NULL) {
			port = atoi(token);
		}
		token = strtok(NULL, ":/?");
		if (token != NULL) {
			weight = atof(token);
		}

		memc_return_val = memcached_server_add(c, host, port);
		if (memc_return_val != MEMCACHED_SUCCESS)
			throw MemcacheException(memc_return_val, c);
	}


	return c;
}

void MemcacheConnectionFactory::destroy(memcached_st* c)
{
	memcached_free(c);
}

bool MemcacheConnectionFactory::isValid(memcached_st* c)
{
  // libmemcached will automatically initiate a new connection.
	return true;
}

MemcacheFactory::MemcacheFactory(CatalogFactory* catalogFactory) throw (DmException):
	nestedFactory_(catalogFactory),
	connectionFactory_(std::vector<std::string>(), false, "default"),
	connectionPool_(&connectionFactory_, 25),
	symLinkLimit_(3),
	memcachedExpirationLimit_(60),
  updateATime_(true)
{
	// Nothing
}

MemcacheFactory::~MemcacheFactory() throw(DmException)
{
  // Nothing
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

	} else if (key == "MemcachedPoolSize")
    this->connectionPool_.resize(atoi(value.c_str()));
  else if (key == "MemcachedProtocol") {
    if (value == "binary")
      this->connectionFactory_.useBinaryProtocol_ = true;
    else
      this->connectionFactory_.useBinaryProtocol_ = false;

  } else if (key == "MemcachedHashDistribution") {
    if (value == "consistent" || value == "default")
      this->connectionFactory_.dist_ = value;
    else
  	  throw DmException(DM_UNKNOWN_OPTION,
                        std::string("Unknown option value ") + value);

  } else if (key == "UpdateAccessTime") {
    std::string lower;
    std::transform(value.begin(), value.end(), lower.begin(), tolower);
    this->updateATime_ = (value == "yes");

  } else if (key == "MemcachedStrictConsistency") {
    if (value == "yes")
      this->memcachedStrict_ = true;
    else if (value == "no")
      this->memcachedStrict_ = false;
    else
  	  throw DmException(DM_UNKNOWN_OPTION,
                        std::string("Unknown option value ") + value);

  }
  else if (key == "MemcachedPOSIX") {
    if (value == "yes")
      this->memcachedPOSIX_ = true;
    else if (value == "no")
      this->memcachedPOSIX_ = false;
    else
  	  throw DmException(DM_UNKNOWN_OPTION,
                        std::string("Unknown option value ") + value);

  }
  else
  	throw DmException(DM_UNKNOWN_OPTION, std::string("Unknown option ") + key);
}



Catalog* MemcacheFactory::createCatalog(PluginManager* pm) throw(DmException)
{
  Catalog* nested = 0x00;

  GOOGLE_PROTOBUF_VERIFY_VERSION;

  if (this->nestedFactory_ != 0x00)
    nested = CatalogFactory::createCatalog(this->nestedFactory_, pm);

  return new MemcacheCatalog(&this->connectionPool_,
                             nested,
                             this->symLinkLimit_,
                             (time_t)this->memcachedExpirationLimit_,
                             this->memcachedStrict_,
                             this->memcachedPOSIX_,
                             this->updateATime_);
}



static void registerPluginMemcache(PluginManager* pm) throw(DmException)
{
  try {
    pm->registerFactory(new MemcacheFactory(pm->getCatalogFactory()));
  }
  catch (DmException e) {
    if (e.code() == DM_NO_FACTORY)
      throw DmException(DM_NO_FACTORY, std::string("Memcache can not be loaded first"));
    throw;
  }
}



/// This is what the PluginManager looks for
PluginIdCard plugin_memcache = {
  PLUGIN_ID_HEADER,
  registerPluginMemcache
};
