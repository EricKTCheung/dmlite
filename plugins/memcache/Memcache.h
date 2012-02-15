/// @file    plugins/memcache/Memcache.h
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
#ifndef MEMCACHED_H
#define	MEMCACHED_H

#include <vector>
#include <sstream>

//#include <fstream>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/back_inserter.hpp>

#include <libmemcached/memcached.h>
#include <dmlite/dmlite++.h>
#include <dmlite/common/PoolContainer.h>
#include <dmlite/dummy/Dummy.h>


namespace dmlite {

#define MEMCACHE_EXPIRATION 60
#define SERIALIZE_VERSION 1

/// Memcache plugin
class MemcacheCatalog: public DummyCatalog {
public:
  /// Constructor
  /// @param decorates The underlying decorated catalog.
  MemcacheCatalog(PoolContainer<memcached_st*>* connPool,
									Catalog* decorates) throw (DmException);

  /// Destructor
  ~MemcacheCatalog() throw (DmException);

  // Overloading 
	void setUserId(uid_t, gid_t, const std::string&) throw (DmException);

  struct stat  stat        (const std::string&) throw (DmException);
  struct stat  stat        (ino_t)              throw (DmException);

  void removeDir(const std::string&) throw (DmException);
  void unlink (const std::string&) throw (DmException);

protected:
  /// The Memcached connection
  memcached_st* conn_;

  /// The connection pool.
  PoolContainer<memcached_st*>* connectionPool_;

	/// TODO: temporary user credentials
	uid_t uid_;
	gid_t gid_;
private:
	const std::string keyFromAny(const char* preKey, ino_t inode);
	const std::string keyFromAny(const char* preKey, const std::string&);

	const std::string valFromMemcachedKey(const std::string key); 
	void 							setMemcachedFromKeyValue(const std::string key,
																						 const std::string value);
	void  						delMemcachedFromKey(const std::string key);
};

class MemcacheConnectionFactory: public PoolElementFactory<memcached_st*> {
public:
  MemcacheConnectionFactory(std::vector<std::string> hosts);
  ~MemcacheConnectionFactory();

  memcached_st* create();
  void   destroy(memcached_st*);
  bool   isValid(memcached_st*);

	// Attributes
  std::vector<std::string>  hosts;

protected:
private:
};


/// Concrete factory for the Memcache plugin.
class MemcacheFactory: public CatalogFactory {
public:
  /// Constructor
  MemcacheFactory(CatalogFactory* catalogFactory) throw (DmException);
  /// Destructor
  ~MemcacheFactory() throw (DmException);

  void configure(const std::string& key, const std::string& value) throw (DmException);
  Catalog* createCatalog() throw (DmException);
protected:
  /// Decorated
  CatalogFactory* nestedFactory_;

  /// Connection factory.
  MemcacheConnectionFactory connectionFactory_;

  /// Connection pool.
  PoolContainer<memcached_st*> connectionPool_;
private:
};

};

#endif	// MEMCACHED_H
