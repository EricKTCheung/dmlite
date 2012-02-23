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
#include <dmlite/common/Security.h>
#include <dmlite/dummy/Dummy.h>
#include <dmlite/common/Uris.h>


namespace dmlite {

#define MEMCACHE_EXPIRATION 60
#define SERIALIZE_VERSION 1

/// Memcache plugin
class MemcacheCatalog: public DummyCatalog {
public:
  /// Constructor
	/// @param connPool The memcached connection pool.
  /// @param decorates The underlying decorated catalog.
  MemcacheCatalog(PoolContainer<memcached_st*>* connPool,
									Catalog* decorates,
									unsigned int symLinkLimit) throw (DmException);

  /// Destructor
  ~MemcacheCatalog() throw (DmException);

  // Overloading 
	void setUserId(uid_t, gid_t, const std::string&) throw (DmException);
  void        changeDir    (const std::string&) throw (DmException);

  ExtendedStat extendedStat(const std::string&, bool = true) throw (DmException);
  ExtendedStat extendedStat(ino_t)              throw (DmException);
  ExtendedStat extendedStat(ino_t, const std::string&) throw (DmException);

  void makeDir  (const std::string&, mode_t) throw (DmException);
  void removeDir(const std::string&) throw (DmException);

  void symlink(const std::string&, const std::string&) throw (DmException);
  void unlink (const std::string&) throw (DmException);

  void rename(const std::string&, const std::string&) throw (DmException);

  void create(const std::string&, mode_t) throw (DmException);

  void   changeMode     (const std::string&, mode_t)       throw (DmException);
  void   changeOwner    (const std::string&, uid_t, gid_t) throw (DmException);
  void   linkChangeOwner(const std::string&, uid_t, gid_t) throw (DmException);

protected:
  /// The Memcached connection
  memcached_st* conn_;

  /// The connection pool.
  PoolContainer<memcached_st*>* connectionPool_;

  UserInfo  user_;  ///< User.
  GroupInfo group_; ///< User main group.

  /// User secondary groups.
  std::vector<GroupInfo> groups_;

  /// Current working dir (path)
  std::string cwdPath_;

  /// Current working dir
  ino_t cwd_;
private:
	/// Converts a key prefix and an inode into a key for memcached.
	/// @param preKey key prefix string.
	/// @param inode  inode number.
	/// @return				The key as string.
	const std::string keyFromAny(const char* preKey, ino_t inode);

	/// Converts a key prefix and a path string into a key for memcached.
	/// @param preKey key prefix string.
	/// @param path   path string.
	/// @return				The key as string.
	const std::string keyFromAny(const char* preKey, const std::string& path);
	/// Converts a key prefix, the inode of the parent dir and the name of the dile/dir/link into a key for memcached.
	/// @param preKey key prefix string.
	/// @param parent inode of parent.
	/// @param name   file/dir/link name-string.
	/// @return				The key as string.
	const std::string keyFromAny(const char* preKey, uint64_t parent, const std::string& name);

	/// Return the value to a given key from memcached.
	/// @param key   The memcached key as string.
	/// @return      The value from memcached.
	const std::string valFromMemcachedKey(const std::string key); 

	/// Store a key,value pair on memcached.
	/// @param key   The memcached key as string.
	/// @param value The memcached value serialized as string.
	void setMemcachedFromKeyValue(const std::string key,
																						 const std::string value);

	/// Delete a key,value pair on memcached.
	/// @param key   The memcached key as string.
	void delMemcachedFromKey(const std::string key);

	/// Get the parent of a directory.
	/// @param The path to split.
	/// @return The parent path as string.
	std::string getParent(const std::string& path) throw (DmException);

  /// Get the parent of a directory.
  /// @param path       The path to split.
  /// @param parentPath Where to put the parent path.
  /// @param name       Where to put the file name (stripping last /).
  /// @return           The parent metadata.
  ExtendedStat getParent(const std::string& path, std::string* parentPath,
                         std::string* name) throw (DmException);

	/// Remove cached entries related to path.
	/// @param preKey			key prefix string.
	/// @param path 			path as string.
	void delMemcachedFromPath(const char* preKey, const std::string& path);

  /// Symlink limit
  unsigned int symLinkLimit_;
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

  /// The recursion limit following symbolic links.
  unsigned int symLinkLimit_;
private:
};

};

#endif	// MEMCACHED_H
