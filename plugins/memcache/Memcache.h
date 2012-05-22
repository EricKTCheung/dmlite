/// @file    plugins/memcache/Memcache.h
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
#ifndef MEMCACHE_H
#define	MEMCACHE_H

#include <vector>
#include <sstream>

#include <libmemcached/memcached.h>
#include <dmlite/dmlite++.h>
#include <dmlite/common/PoolContainer.h>
#include <dmlite/common/Security.h>
#include <dmlite/dummy/Dummy.h>
#include <dmlite/common/Uris.h>

namespace dmlite {

#define DEFAULT_MEMCACHED_EXPIRATION 60
#define SHORT_MEMCACHED_EXPIRATION 5

#define DIR_NOTCACHED 0
#define DIR_NOTCOMPLETE 1
#define DIR_CACHED 2

#define DEL_DIRLIST true
#define KEEP_DIRLIST false

#define FETCH_COMBINED 30
#define FETCH_COMBINED_MIN  1
#define FETCH_COMBINED_MAX 50
#define FETCH_COMBINED_MUL  2
#define PROB_CACHE 1.0

struct MemcacheDir {
  uint64_t      dirId;
  ExtendedStat  current;
  struct dirent ds;

  Directory     *dirp;
  int           isCached;
  int           curKeysSegment;
  int           fetchCombined;

  std::list<std::string>   keys;
  size_t                   keysOrigSize;
  std::list<ExtendedStat>  xstats;
  int                      keysPntr; 
};

class MemcacheException: public DmException {
public:
  MemcacheException(memcached_return rc, memcached_st *conn)
  {
    this->errorCode_ = (int) rc;
    this->errorMsg_ = std::string(memcached_strerror(conn, rc));
  };
protected:
private:
};

/// Memcache plugin
class MemcacheCatalog: public DummyCatalog {
public:
  /// Constructor
	/// @param connPool The memcached connection pool.
  /// @param decorates The underlying decorated catalog.
  MemcacheCatalog(PoolContainer<memcached_st*>* connPool,
									Catalog* decorates,
									unsigned int symLinkLimit,
									time_t memcachedExpirationLimit,
                  bool memcachedStrict,
                  bool memcachedPOSIX,
                  bool updateATime)
											throw (DmException);

  /// Destructor
  ~MemcacheCatalog() throw (DmException);

  // Overloading 
  std::string getImplId(void) throw ();

  void set(const std::string& key, va_list varg) throw (DmException);

  SecurityContext* createSecurityContext(const SecurityCredentials&) throw (DmException);
  void setSecurityContext(const SecurityContext*) throw (DmException);

//  void        changeDir    (const std::string&) throw (DmException);

  ExtendedStat extendedStat(const std::string&, bool = true) throw (DmException);
  ExtendedStat extendedStat(ino_t)              throw (DmException);
  ExtendedStat extendedStat(ino_t, const std::string&) throw (DmException);
  SymLink readLink(ino_t) throw (DmException);

  void addReplica(const std::string&, int64_t, const std::string&,
                  const std::string&, char, char,
                  const std::string&, const std::string&) throw (DmException);

  void deleteReplica(const std::string&, int64_t,
                     const std::string&) throw (DmException);

  std::vector<FileReplica> getReplicas(const std::string&) throw (DmException);
  
  Uri              get        (const std::string&) throw (DmException);

  void symlink(const std::string&, const std::string&) throw (DmException);
  void unlink (const std::string&) throw (DmException);

  void create(const std::string&, mode_t) throw (DmException);

  // missing put functions 

  Directory* openDir (const std::string&) throw (DmException);
  void       closeDir(Directory*)         throw (DmException);

  struct dirent* readDir (Directory*) throw (DmException);
  ExtendedStat*  readDirx(Directory*) throw (DmException);


  void changeMode     (const std::string&, mode_t)       throw (DmException);
  void changeOwner    (const std::string&, uid_t, gid_t) throw (DmException);
  void linkChangeOwner(const std::string&, uid_t, gid_t) throw (DmException);

  void setAcl(const std::string&, const std::vector<Acl>&) throw (DmException);

  void utime(const std::string&, const struct utimbuf*) throw (DmException);
  void utime(ino_t, const struct utimbuf*)              throw (DmException);
  
  std::string getComment(const std::string&)                     throw (DmException);
  void        setComment(const std::string&, const std::string&) throw (DmException);

  void setGuid(const std::string& path, const std::string &guid) throw (DmException);
  
  void makeDir  (const std::string&, mode_t) throw (DmException);
  void removeDir(const std::string&) throw (DmException);

  void rename(const std::string&, const std::string&) throw (DmException);

  void replicaSetLifeTime  (const std::string&, time_t) throw (DmException);
  void replicaSetAccessTime(const std::string&)         throw (DmException);
  void replicaSetType      (const std::string&, char)   throw (DmException);
  void replicaSetStatus    (const std::string&, char)   throw (DmException);
  
  // missing User and Group Info

protected:
  /// The Memcached connection
  memcached_st* conn_;

  /// The connection pool.
  PoolContainer<memcached_st*>* connectionPool_;

  /// Security context
  SecurityContext secCtx_;

  /// User secondary groups.
  std::vector<GroupInfo> groups_;

  /// Get a list of replicas by their memcached key 
  std::vector<FileReplica> getReplicas(const std::string&, ino_t inode) throw (DmException);
  
  /// Update access time.
  bool updateATime_;
private:
  /// Serialize an ExtendedStat object into a string.
  /// @param var The object to serialize.
  /// @return The serialized object as string.
  std::string serialize(const ExtendedStat& var);

  /// Create an object from a serialized string.
  /// @param serial_str The serialized object as string.
  /// @param var The deserialized object.
  void deserialize(const std::string& serial_str, ExtendedStat& var);

  /// Serialize a SymLink object into a string.
  /// @param var The object to serialize.
  /// @return The serialized object as string.
  std::string serializeLink(const SymLink& var);

  /// Create an object from a serialized string.
  /// @param serial_str The serialized object as string.
  /// @param var The deserialized object.
  void deserializeLink(const std::string& serial_str, SymLink& var);

  /// Serialize a Comment object into a string.
  /// @param var The object to serialize.
  /// @return The serialized object as string.
  std::string serializeComment(const std::string& var);

  /// Create an object from a serialized string.
  /// @param serial_str The serialized object as string.
  /// @param var The deserialized object.
  void deserializeComment(std::string& serial_str, std::string& var);

  /// Serialize a list of keys (strings) into a string.
  /// The keys can be either 'white' items or
  /// 'black' items. The black items form a blacklist which
  /// should be considered when deserializing the list.
  /// @param keyList      The list of keys.
  /// @param isWhite      Marks all elements of the list. 
  /// @param isComplete.  Is the list complete or still being extended. 
  /// @return The serialized object.
  std::string serializeList(std::vector<std::string>& keyList, const bool isWhite = true, const bool isComplete = false);

  /// Serialize a list of keys (strings) into a string.
  /// The keys can be either 'white' items or
  /// 'black' items. The black items form a blacklist which
  /// should be considered when deserializing the list.
  /// @param keyList      The list of keys.
  /// @param isWhite      Marks all elements of the list. 
  /// @param isComplete   Is the list complete or still being extended. 
  /// @param mtime        Marks the last modification to the directory.
  /// @return The serialized object.
  std::string serializeDirList(std::vector<std::string>& keyList, const time_t mtime, const bool isWhite = true, const bool isComplete = false);
 
  /// Deserialize a list of keys.
  /// This function does not take blacklisted items
  /// into consideration.
  /// @param serialList The serialized List as string.
  /// @return           The List.
  std::vector<std::string> deserializeList(std::string& serialList);

  /// Deserialize a list of keys.
  /// This function returns the isComplete bit of the list.
  /// It does not take blacklisted items into consideration.
  /// @param serialList The serialized List as string.
  /// @param keyList    The List to write the result into.
  /// @param mtime      Marks the last modification to the directory.
  /// @return           The isComplete value.
  int deserializeDirList(std::string& serialList,
                         std::vector<std::string> &keyList,
                         time_t &mtime);

  /// Deserialize a list of keys.
  /// This function returns the isComplete bit of the list.
  /// It does not take blacklisted items into consideration.
  /// @param serialList The serialized List as string.
  /// @param keyList    The List to write the result into.
  /// @return           The isComplete value.
  int deserializeList(std::string& serialList,
                                      std::vector<std::string> &keyList);

  /// Deserialize a list of keys.
  /// This function takes blacklisted item into account
  /// and subtracts those item from the output list.
  /// @param serialList The serialized List as string.
  /// @return           The List.
  std::vector<std::string> deserializeBlackList(std::string& serialList);

  /// Serialize a FileReplica to a string.
  /// @param replica    The replica to serialize.
  /// @return           The serialized object.
  std::string serializeFileReplica(const FileReplica& replica);

  /// Deserialize a FileReplica.
  /// @param serialRepl The serialized replica.
  /// @return           The FileReplica.
  FileReplica deserializeFileReplica(std::string& serialRepl);

  /// Convert a memcached key to a versioned key.
  /// @param version  The version to attach
  /// @param key      The original key.
  /// @return         The versioned key.
  const std::string versionedKeyFromAny(uint64_t version, const std::string key);

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

	/// Converts a key prefix and a URI string into a key for memcached.
	/// @param preKey key prefix string.
	/// @param uri    URI string.
	/// @return				The key as string.
	const std::string keyFromURI(const char* preKey, const std::string& uri);
	/// Converts a key prefix, the inode of the parent dir and the name of the dile/dir/link into a key for memcached.
	/// @param preKey key prefix string.
	/// @param parent inode of parent.
	/// @param name   file/dir/link name-string.
	/// @return				The key as string.
	const std::string keyFromAny(const char* preKey, ino_t parent, const std::string& name);

	/// Return the value to a given key from memcached.
	/// @param key   The memcached key as string.
	/// @return      The value from memcached.
	const std::string getValFromMemcachedKey(const std::string key) throw (MemcacheException);

	/// Return the value to a given key from memcached.
	/// @param key   The memcached key as string.
	/// @return      The value from memcached.
	const std::string safeGetValFromMemcachedKey(const std::string key);

	/// Return the value to a given versioned key from memcached.
	/// @param key   The memcached key as string.
	/// @return      The value from memcached.
  const std::string getValFromMemcachedVersionedKey(const std::string key) throw (MemcacheException);

	/// Return the value to a given versioned key from memcached.
	/// @param key   The memcached key as string.
	/// @return      The value from memcached.
  const std::string safeGetValFromMemcachedVersionedKey(const std::string key);
  
  const std::string getDListValFromMemcachedKey(const std::string key) throw (MemcacheException);

  const std::string safeGetDListValFromMemcachedKey(const std::string key) throw (MemcacheException);
  /// Return a list from memcached.
  /// This function returns a vector with the elements still
  /// serialized in it.
  /// @param       listKey.
  std::vector<std::string> getListFromMemcachedKey(const std::string& listKey) throw (MemcacheException);

  std::vector<std::string> safeGetListFromMemcachedKey(const std::string& listKey) throw (MemcacheException);

  /// Add an item to a list on memcached.
  /// This function uses memcached_append() to add the element,
  /// so it does not have to download the list from
  /// memcached.
  /// This function allows deletions from the list by
  /// using the isWhite value to append a blacklist item.
  /// @param listKey     The key of the list.
  /// @param key         The key to add.  
  /// @param isWhite     Marks the element a white- or blacklisted. 
  /// @param isComplete  Marks if the list is complete.
  void addToListFromMemcachedKey(const std::string& listKey, const std::string& key, const bool isWhite = true, const bool isComplete = true);

  void safeAddToListFromMemcachedKey(const std::string& listKey, const std::string& key, const bool isWhite = true, const bool isComplete = true);

  /// Add an item to a list with a distributed index on memcached.
  /// This function uses memcached_append() to add the element,
  /// so it does not have to download the list from
  /// memcached.
  /// This function allows deletions from the list by
  /// using the isWhite value to append a blacklist item.
  /// @param listKey     The key of the list.
  /// @param key         The key to add.  
  /// @param isWhite     Marks the element a white- or blacklisted. 
  /// @param isComplete  Marks if the list is complete.
  void addToDListFromMemcachedKey(const std::string& listKey, const std::string& key, const bool isWhite = true, const bool isComplete = true);

  void safeAddToDListFromMemcachedKey(const std::string& listKey, const std::string& key, const bool isWhite = true, const bool isComplete = true);

  /// Add an item to a list with a distributed index on memcached.
  /// This function uses memcached_append() to add the element,
  /// so it does not have to download the list from memcached.
  /// Element deletions are performed by
  /// using the isWhite value to append a blacklist item.
  /// This function does not retrieve the current segment, but needs it
  /// as an argument. If a segment is full, a new one is created and
  /// the entry inserted. The new current segment after appending is returned.
  /// @param listKey     The key of the list.
  /// @param key         The key to add.  
  /// @param isWhite     Marks the element a white- or blacklisted. 
  /// @param isComplete  Marks if the list is complete.
  /// @param curSegment  The number of the current segment.
   int addToDListFromMemcachedKey(const std::string& listKey,
                                  const std::string& key, 
                                  const bool isWhite = true,
                                  const bool isComplete = true,
                                  int curSegment = 0);

   int safeAddToDListFromMemcachedKey(const std::string& listKey,
                                  const std::string& key, 
                                  const bool isWhite = true,
                                  const bool isComplete = true,
                                  int curSegment = 0);

  /// Remove an item from a list on memcached.
  /// This function removes an element by adding it to
  /// the list's blacklist. Like addToListFromMemcachedKey
  /// it uses memcached_append.
  /// @param listKey  The key of the list.
  /// @param key      The key to remove.
  void removeListItemFromMemcachedKey(const std::string& listKey, std::string& key);

  void safeRemoveListItemFromMemcachedKey(const std::string& listKey, std::string& key);

  /// Fetch an ExtendedStat from memcached and store it in a Directory.
  /// Fetches a value from memcached and stores the result
  /// in the provided MemcacheDir* structure.
  /// @param dirp     The MemcacheDir pointer.
  /// @return         A pointer to the ExtendedStat.
  ExtendedStat* fetchExtendedStatFromMemcached(MemcacheDir *dirp) 
                            throw (DmException);

  /// Fetch an ExtendedStat from a delegate and store it in a Directory.
  /// Fetches a value from a delegate plugin and stores the result
  /// in the provided MemcacheDir* structure.
  /// The second parameter specifies i f the results should be also
  /// stored on memcached.
  /// @param dirp         The MemcacheDir pointer.
  /// @param saveToMemc   The switch, if the value should be cached.
  /// @return             A pointer to the ExtendedStat.
  ExtendedStat* fetchExtendedStatFromDelegate(MemcacheDir *dirp, const bool saveToMemc) 
                            throw (DmException);
  /// Retrieve a list of values from a list of keys.
  /// The function uses memcached_mget to get several
  /// keys in one memcached bulk access.
  /// If not all keys are found, it returns a empty list.
  /// @param keyList  The list of keys to get.
  /// @return         The list of serialized values.
  std::vector<std::string> 
         getValListFromMultipleMemcachedKeys(const std::vector<std::string>& keyList);

  /// Store a list of FileReplicas on memcached.
  /// @param replicas The list of replicas.
  /// @param inode    The list key.
  void setMemcachedFromReplicas(std::vector<FileReplica>& replicas, ino_t inode);

  /// Store a list of FileReplicas on memcached.
  /// @param replicas The list of replicas.
  /// @param inode    The list key.
  void safeSetMemcachedFromReplicas(std::vector<FileReplica>& replicas, ino_t inode);
  
  /// Store a key,value pair on memcached with a versioned key.
  /// @param key   The versioned key as string.
  /// @param value The value serialized as string. 
  void setMemcachedFromVersionedKeyValue(const std::string key,
                                         const std::string value);
  
  /// Store a key,value pair on memcached with a versioned key.
  /// @param key   The versioned key as string.
  /// @param value The value serialized as string. 
  void safeSetMemcachedFromVersionedKeyValue(const std::string key,
                                         const std::string value);

	/// Store a key,value pair on memcached.
	/// @param key   The memcached key as string.
	/// @param value The memcached value serialized as string.
	void setMemcachedFromKeyValue(const std::string key,
																						 const std::string value) throw (MemcacheException);

	/// Store a key,value pair on memcached.
	/// @param key   The memcached key as string.
	/// @param value The memcached value serialized as string.
	void safeSetMemcachedFromKeyValue(const std::string key,
																						 const std::string value);

  int setMemcachedDListFromKeyValue(const std::string key, const std::string value);

  /// Safe version of setMemcachedDListFromKeyValue.
  /// Safe functions catch all MemcacheExceptions.

  int safeSetMemcachedDListFromKeyValue(const std::string key, const std::string value);

  /// Delete a versioned key from memcached.
  /// This function actually only increases the version number
  /// of the item. Nothing gets deleted.
	/// @param key   The memcached key as string.
  void delMemcachedFromVersionedKey(const std::string key);

	/// Delete a key,value pair on memcached.
	/// @param key   The memcached key as string.
	void delMemcachedFromKey(const std::string key);

  void delMemcachedFromDListKey(const std::string key);
	/// Get the parent of a directory.
	/// @param The path to split.
	/// @return The parent path as string.
	std::string getParent(const std::string& path) throw (DmException);

	/// Remove cached entries related to a path.
	/// @param path 	    		The path as string.
  /// @param removeDirEntry Flag if the dir list should also be deleted.
	void delMemcachedFromPath(const std::string& path,
                            const bool removeDirEntry = true) throw (MemcacheException);

	/// Remove cached entries related to an inode. 
	/// @param inode          The inode.
  /// @param removeDirEntry Flag if the dir list should also be deleted.
	void delMemcachedFromInode(const ino_t inode,
                            const bool removeDirEntry = true) throw (MemcacheException);

  /// Convert a charr array of given size to an int.
  /// @param text           The char array.
  /// @param length         The length to read.
  /// @return               The integer representation.
  int atoi(char *text, size_t length);

  /// Convert an integer into a std::string.
  /// Conversion is done using a stringstream.
  /// @param                The int.
  /// @param                The string.
  std::string toString(int i);

  /// Extract the inode from a ExtendedStat key.
  /// This function depends on the representation of the
  /// stat key and has to be changed with it.
  /// @param key            The key as string.
  /// @return               The inode.
  ino_t getInodeFromStatKey(const std::string key);

  /// Fetch values for a list of keys from memcached.
  /// This function returns the values still serialized in a
  /// vector.
  /// @param keyList        The key list as vector of strings.
  /// @return               The values as vector of strings.
  std::vector<std::string> getValListFromMemcachedKeyList(
                            const std::vector<std::string>& keyList);

  /// Prepare a multiget from memcached.
  /// This function calls the memcached_mget API function
  /// to prepare for memcached_fetch calls.
  /// @param keyList        The key list.
  void prepareMemcachedMultiGetFromKeyList(
                            const std::vector<std::string>& keyList) throw (MemcacheException);

  /// Fetch multiple values from memcached.
  /// Uses memcached_fetch to retrieve multiple values efficiently.
  /// If usePlaceholder is set, the returned list has empty elements
  /// to signal values which were not found on memcached.
  /// The keys can be found by copmaring it to the key list. 
  /// Without placeholders,   missing elements renders all results invalid
  /// and the function returns an empty list.
  /// prepareMemcachedMultiGetFromKeyList has to be called
  /// before.
  /// @param keyListSize      The size of the key list.
  /// @param usePlaceholder   Use placeholders for missing values.
  /// @return                 The list of serialized values.
  std::vector<std::string>
          doMemcachedMultiGet(int keyListSize, const bool usePlaceholder) throw (MemcacheException);

  /// Fetch a list of ExtendedStats from memcached keys.
  /// The function tries to get all values from memcached and
  /// delegates to the next plugin if not.
  /// Assuming that the underlying plugins have acces to all data,
  /// all values are retrieved in any case.
  /// @param keyList          The list of keys.
  /// @return                 The list of ExtendedStats.
  std::vector<ExtendedStat>
              getExtendedStatListFromMemcachedKeyList(
                            const std::vector<std::string>& keyList) throw (DmException);


  /// Symlink limit
  unsigned int symLinkLimit_;

	/// The expiration limit for cached data on memcached in seconds.
	time_t memcachedExpirationLimit_;

  /// The config parameter to specify desired consistency.
  bool memcachedStrict_;

  /// Enable POSIX-like behaviour.
  bool memcachedPOSIX_;
};

class MemcacheConnectionFactory: public PoolElementFactory<memcached_st*> {
public:
  MemcacheConnectionFactory(std::vector<std::string> hosts,
                            std::string protocol,
                            std::string dist);
  ~MemcacheConnectionFactory();

  memcached_st* create();
  void   destroy(memcached_st*);
  bool   isValid(memcached_st*);

	// Attributes
  std::vector<std::string>  hosts;

  /// The memcached protocol (binary/ascii) to use.
  std::string protocol;

  /// The hash distribution algorithm
  std::string dist;
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
  Catalog* createCatalog(StackInstance*) throw (DmException);
protected:
  /// Decorated
  CatalogFactory* nestedFactory_;

  /// Connection factory.
  MemcacheConnectionFactory connectionFactory_;

  /// Connection pool.
  PoolContainer<memcached_st*> connectionPool_;

  /// The recursion limit following symbolic links.
  unsigned int symLinkLimit_;

	/// The expiration limit for cached data on memcached in seconds.
	unsigned int memcachedExpirationLimit_;

  /// The config parameter to specify desired consistency.
  bool memcachedStrict_;

  /// Enable POSIX-like behaviour.
  bool memcachedPOSIX_;
  
  /// Update access time.
  bool updateATime_;
private:
};

  
};

#endif	// MEMCACHED_H
