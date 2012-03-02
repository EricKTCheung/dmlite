/// @file    plugins/memcache/Memcache.h
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
#include <vector>

#include "Memcache.h"

namespace boost {
namespace serialization {

template<class Archive>
void serialize(Archive& ar, struct stat& stStat, const unsigned int version)
{
  ar & stStat.st_atim.tv_sec;
  ar & stStat.st_ctim.tv_sec;
  ar & stStat.st_mtim.tv_sec;
  ar & stStat.st_gid;
  ar & stStat.st_uid;
  ar & stStat.st_nlink;
  ar & stStat.st_ino;
  ar & stStat.st_mode;
  ar & stStat.st_size;
}

template<class Archive>
void serialize(Archive& ar, ExtendedStat& xStat, const unsigned int version)
{
  ar & xStat.stat;
  ar & xStat.parent;
  ar & xStat.type;
  ar & xStat.status;
  ar & xStat.name;
  ar & xStat.guid;
  ar & xStat.csumtype;
  ar & xStat.csumvalue;
  ar & xStat.acl;
}

template<class Archive>
void serialize(Archive& ar, SymLink& link, const unsigned int version)
{
	ar & link.fileId;
	ar & link.link;
}
} // namespace serialization
} // namespace boost

using namespace dmlite;

/// Used to keep the Key Prefixes
enum {
	PRE_STAT = 0,
  PRE_REPL,
  PRE_REPL_LIST
};

/// Used internally to define Key Prefixes.
/// Must match with PRE_* constants!
static const char* key_prefix[] = {
	"STAT",
  "REPL",
  "RPLI"
};

/// Little of help here to avoid redundancy
#define DELEGATE(method, ...) \
if (this->decorated_ == 0x00)\
  throw DmException(DM_NOT_IMPLEMENTED, "There is no plugin in the stack that implements "#method);\
this->decorated_->method(__VA_ARGS__);


/// Little of help here to avoid redundancy
#define DELEGATE_RETURN(method, ...) \
if (this->decorated_ == 0x00)\
  throw DmException(DM_NOT_IMPLEMENTED, "There is no plugin in the stack that implements "#method);\
return this->decorated_->method(__VA_ARGS__);


/// Little of help here to avoid redundancy
#define DELEGATE_ASSIGN(var, method, ...) \
if (this->decorated_ == 0x00)\
  throw DmException(DM_NOT_IMPLEMENTED, "There is no plugin in the stack that implements "#method);\
var = this->decorated_->method(__VA_ARGS__);

// Serialize anything with a serialize function into an std::string
#define SERIALIZE(serial_str, var, version) \
	serial_str.clear(); \
	boost::iostreams::back_insert_device<std::string> inserter(serial_str); \
	boost::iostreams::stream<boost::iostreams::back_insert_device<std::string> > s(inserter); \
	boost::archive::text_oarchive oa(s); \
	boost::serialization::serialize(oa, var, version); \
	s.flush();

// Deserialize anything with a serialize function from an std::string
#define DESERIALIZE(serial_str, var, version) \
	boost::iostreams::basic_array_source<char> device(serial_str.data(), serial_str.size()); \
	boost::iostreams::stream<boost::iostreams::basic_array_source<char> > s(device); \
	boost::archive::text_iarchive ia(s); \
	boost::serialization::serialize(ia, var, version);


MemcacheCatalog::MemcacheCatalog(PoolContainer<memcached_st*>* connPool, 
																 Catalog* decorates,
																 unsigned int symLinkLimit,
																 time_t memcachedExpirationLimit) throw (DmException):
   DummyCatalog(decorates),
	 symLinkLimit_(symLinkLimit),
	 cwd_(0), 
	 memcachedExpirationLimit_(memcachedExpirationLimit)
{
  this->connectionPool_ = connPool;
  this->conn_           = connPool->acquire();
  std::memset(&this->user_,  0x00, sizeof(UserInfo));
  std::memset(&this->group_, 0x00, sizeof(GroupInfo));
}



MemcacheCatalog::~MemcacheCatalog() throw (DmException)
{
  this->connectionPool_->release(this->conn_);
}

void MemcacheCatalog::setUserId(uid_t uid, gid_t gid, const std::string& dn) throw (DmException)
{
	DELEGATE(setUserId, uid, gid, dn);

	DELEGATE_ASSIGN(this->user_, getUser, uid);
	DELEGATE_ASSIGN(this->group_, getGroup, gid);
  this->groups_.clear();
}

void MemcacheCatalog::changeDir(const std::string& path) throw (DmException)
{
	DELEGATE(changeDir, path);

  ExtendedStat cwd = this->extendedStat(path);
  this->cwdPath_ = path;
  this->cwd_     = cwd.stat.st_ino;
}


ExtendedStat MemcacheCatalog::extendedStat(const std::string& path, bool followSym) throw (DmException)
{
  // Split the path always assuming absolute
  std::list<std::string> components = splitPath(path);

  // Iterate starting from absolute root (parent of /) (0)
  uint64_t     parent       = 0;
  unsigned     symLinkLevel = 0;
  ExtendedStat meta;
  std::string  c;

  // If path is absolute OR cwd is empty, start in root
  if (path[0] == '/' || this->cwdPath_.empty()) {
    // Push "/", as we have to look for it too
    components.push_front("/");
    // Root parent "is" a dir and world-readable :)
    memset(&meta, 0x00, sizeof(ExtendedStat));
    meta.stat.st_mode = S_IFDIR | 0555 ;
  }
  // Relative, and cwd set, so start there
  else {
    parent = this->cwd_;
    meta   = this->extendedStat(parent);
  }
  
  while (!components.empty()) {
    // Check that the parent is a directory first
    if (!S_ISDIR(meta.stat.st_mode) && !S_ISLNK(meta.stat.st_mode))
      throw DmException(DM_NOT_DIRECTORY, "%s is not a directory", meta.name);
    // New element traversed! Need to check if it is possible to keep going.
    if (checkPermissions(this->user_, this->group_, this->groups_,
                         meta.acl, meta.stat, S_IEXEC) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to list '%s'", meta.name);

    // Pop next component
    c = components.front();
    components.pop_front();

    // Stay here
    if (c == ".") {
      continue;
    }
    // Up one level
    else if (c == "..") {
      meta   = this->extendedStat(parent);
      parent = meta.parent;
    }
    // Regular entry
    else {
      meta = this->extendedStat(parent, c);

      // Symbolic link!, follow that instead
      if (S_ISLNK(meta.stat.st_mode) && followSym) {
        SymLink link = this->readLink(meta.stat.st_ino);

        ++symLinkLevel;
        if (symLinkLevel > this->symLinkLimit_) {
          throw DmException(DM_TOO_MANY_SYMLINKS,
                           "Symbolic links limit exceeded: > %d",
                           this->symLinkLimit_);
        }

        // We have the symbolic link now. Split it and push
        // into the component
        std::list<std::string> symPath = splitPath(link.link);
        components.splice(components.begin(), symPath);
        // Push / if absolute
        if (link.link[0] == '/') {
          parent = 0;
          components.push_front("/");
        }
      }
      // Next one!
      else {
        parent = meta.stat.st_ino;
      }
    }
    
  }

  return meta;
}

ExtendedStat MemcacheCatalog::extendedStat(uint64_t fileId) throw (DmException)
{
 ExtendedStat meta; 

  memset(&meta, 0x00, sizeof(ExtendedStat));

	std::string valMemc;

	const std::string key = keyFromAny(key_prefix[PRE_STAT], fileId); 

	valMemc = valFromMemcachedKey(key);
	if (!valMemc.empty())
	{
		DESERIALIZE(valMemc, meta, SERIALIZE_VERSION);
	} else // valMemc was not in memcached
	{
		DELEGATE_ASSIGN(meta, extendedStat, fileId);
		SERIALIZE(valMemc, meta, SERIALIZE_VERSION);
		setMemcachedFromKeyValue(key, valMemc);
	}

  return meta;
}

ExtendedStat MemcacheCatalog::extendedStat(uint64_t parent, const std::string& name) throw (DmException)
{
  ExtendedStat meta;

  memset(&meta, 0x00, sizeof(ExtendedStat));

	std::string valMemc;

	const std::string key = keyFromAny(key_prefix[PRE_STAT], parent, name); 

	valMemc = valFromMemcachedKey(key);
	if (!valMemc.empty())
	{
		DESERIALIZE(valMemc, meta, SERIALIZE_VERSION);
	} else // valMemc was not in memcached
	{
		DELEGATE_ASSIGN(meta, extendedStat, parent, name);
		SERIALIZE(valMemc, meta, SERIALIZE_VERSION);
		setMemcachedFromKeyValue(key, valMemc);
	}

  return meta;
}

void MemcacheCatalog::removeDir(const std::string& path) throw (DmException)
{
	DELEGATE(removeDir, path);
	delMemcachedFromPath(key_prefix[PRE_STAT], path);
	delMemcachedFromPath(key_prefix[PRE_STAT], getParent(path));
}

void MemcacheCatalog::symlink(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
	DELEGATE(symlink, oldPath, newPath);
	delMemcachedFromPath(key_prefix[PRE_STAT], getParent(oldPath));
	delMemcachedFromPath(key_prefix[PRE_STAT], getParent(newPath));
}

void MemcacheCatalog::unlink(const std::string& path) throw (DmException)
{
	DELEGATE(unlink, path);
	delMemcachedFromPath(key_prefix[PRE_STAT], path);
	delMemcachedFromPath(key_prefix[PRE_STAT], getParent(path));
}

void MemcacheCatalog::rename(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
	DELEGATE(rename, oldPath, newPath);
	delMemcachedFromPath(key_prefix[PRE_STAT], oldPath);
	delMemcachedFromPath(key_prefix[PRE_STAT], getParent(oldPath));
	delMemcachedFromPath(key_prefix[PRE_STAT], getParent(newPath));
}

void MemcacheCatalog::makeDir(const std::string& path, mode_t mode) throw (DmException)
{
	DELEGATE(makeDir, path, mode);
	delMemcachedFromPath(key_prefix[PRE_STAT], getParent(path));
}

void MemcacheCatalog::create(const std::string& path, mode_t mode) throw (DmException)
{
	DELEGATE(create, path, mode);
	delMemcachedFromPath(key_prefix[PRE_STAT], getParent(path));
}

void MemcacheCatalog::changeMode(const std::string& path, mode_t mode) throw (DmException)
{
	DELEGATE(changeMode, path, mode);
	delMemcachedFromPath(key_prefix[PRE_STAT], path);
}

void MemcacheCatalog::changeOwner(const std::string& path, uid_t newUid, gid_t newGid)
  throw (DmException)
{
	DELEGATE(changeOwner, path, newUid, newGid);
	delMemcachedFromPath(key_prefix[PRE_STAT], path);
}

void MemcacheCatalog::linkChangeOwner(const std::string& path, uid_t newUid, gid_t newGid)
  throw (DmException)
{
	DELEGATE(linkChangeOwner, path, newUid, newGid);
	delMemcachedFromPath(key_prefix[PRE_STAT], path);
}
/*
void MemcacheCatalog::deleteReplica(const std::string& guid, int64_t id,
                                   const std::string& sfn) throw (DmException)
{
  DELEGATE(deleteReplica, guid, id, sfn);
  std::string key = keyFromAny(key_prefix[PRE_REPL], guid, id, sfn);
  delMemcachedFromKey(key);
}

std::vector<FileReplica> MemcacheCatalog::getReplicas(const std::string& path) throw(DmException)
{
  // Function copied from NsMySql.cpp
  ExtendedStat  meta;

  // Need to grab the file first
  meta = this->extendedStat(path, true);

  // The file exists, plus we have permissions to go there. Check we can read
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN,
                   "Not enough permissions to read " + path);

  try {
    return this->getReplicas(meta.stat.st_ino);
  }
  catch (DmException e) {
    if (e.code() == DM_NO_REPLICAS)
      throw DmException(DM_NO_REPLICAS, "No replicas available for " + path);
    throw;
  }
}

std::vector<FileReplica> MemcacheCatalog::getReplicas(ino_t ino) throw (DmException)
{
}

FileReplica MemcacheCatalog::get(const std::string& path) throw(DmException)
{

}
*/
std::string MemcacheCatalog::getParent(const std::string& path)
 																					throw (DmException)
{
	std::list<std::string> components = splitPath(path);
	std::string parentPath;

  while (components.size() > 1) {
    parentPath += components.front() + "/";
    components.pop_front();
  }
  if (path[0] == '/')
    parentPath = "/" + parentPath;

  // Get the files now
  if (!parentPath.empty())
    return parentPath;
  else if (!this->cwdPath_.empty())
    return this->cwdPath_;
  else
    return std::string("/");
}
	
ExtendedStat MemcacheCatalog::getParent(const std::string& path,
                                       std::string* parentPath,
                                       std::string* name) throw (DmException)
{
	std::list<std::string> components = splitPath(path);

  parentPath->clear();
  name->clear();

  // Build parent (this is, skipping last one)
  while (components.size() > 1) {
    *parentPath += components.front() + "/";
    components.pop_front();
  }
  if (path[0] == '/')
    *parentPath = "/" + *parentPath;

  *name = components.front();
  components.pop_front();

  // Get the files now
  if (!parentPath->empty())
    return this->extendedStat(*parentPath);
  else if (!this->cwdPath_.empty())
    return this->extendedStat(this->cwd_);
  else
    return this->extendedStat("/");
}

const std::string MemcacheCatalog::keyFromAny(const char* preKey,
																							ino_t inode)
{
	std::stringstream streamKey;
	// add prefix
	streamKey << preKey << ':';
	// add argument
	streamKey << inode;

	return streamKey.str();
}
 
const std::string MemcacheCatalog::keyFromAny(const char* preKey,
																							uint64_t parent,
																							const std::string& name)
{
	std::stringstream streamKey;
	// add prefix
	streamKey << preKey << ':';
	// add argument
	streamKey << name << ':' << parent;

	return streamKey.str();
}

const std::string MemcacheCatalog::keyFromAny(const char* preKey,
																							const std::string& path)
{
	std::stringstream streamKey;

	std::string key_path;
	const std::string cwd(this->decorated_->getWorkingDir());

	// add prefix
	streamKey << preKey << ':';
	
  if (path[0] == '/' || cwd.empty())
	{
	  key_path = path;
	} else
	{
		key_path = cwd;
		if (key_path[key_path.length()-1] != '/') 
			key_path.append("/");

		key_path.append(path);
	}
		
	const unsigned int strlen_path = key_path.length();
	int idx_path_substr = strlen_path - (250 - 50);

	if (idx_path_substr < 0)
		idx_path_substr = 0;

	streamKey << key_path.substr(idx_path_substr);

	return streamKey.str();
}

const std::string MemcacheCatalog::valFromMemcachedKey(const std::string key)
{
	memcached_return statMemc;
	size_t lenValue;
	uint32_t flags;
	char* valMemc;
	std::string valMemcStr;

	valMemc = memcached_get(this->conn_,
													 key.data(),
													 key.length(),
													 &lenValue,
													 &flags,
													 &statMemc);

	if (statMemc != MEMCACHED_SUCCESS &&
			statMemc != MEMCACHED_NOTFOUND)
	{
		throw DmException(DM_UNKNOWN_ERROR,
											std::string(memcached_strerror(this->conn_,
																										 statMemc)));
	}

	if (valMemc != NULL)
	{
		valMemcStr.assign(valMemc);
	}
	return valMemcStr;
}

void MemcacheCatalog::setMemcachedFromKeyValue(const std::string key,
							  						  const std::string strValue)
{
	memcached_return statMemc;
	statMemc = memcached_set(this->conn_,
													 key.data(),
													 key.length(),
													 strValue.data(), strValue.length(),
													 this->memcachedExpirationLimit_,
  												 (uint32_t)0);

	if (statMemc != MEMCACHED_SUCCESS)
	{
		throw DmException(DM_UNKNOWN_ERROR,
											std::string(memcached_strerror(this->conn_,
																										 statMemc)));
	}

	return;
}

void MemcacheCatalog::delMemcachedFromKey(const std::string key)
{
	memcached_return statMemc;
	statMemc = memcached_delete(this->conn_,
													 		key.data(),
														  key.length(),
													 		(time_t)0);

	if (statMemc != MEMCACHED_SUCCESS &&
			statMemc != MEMCACHED_NOTFOUND)
	{
		throw DmException(DM_UNKNOWN_ERROR,
											std::string(memcached_strerror(this->conn_,
																										 statMemc)));
	}
	if (statMemc != MEMCACHED_NOTFOUND)
	
	return;
}

void MemcacheCatalog::delMemcachedFromPath(const char* preKey, const std::string& path)
{
  std::string name;
  std::string parentPath;
	ExtendedStat parent;
	std::memset(&parent, 0x00, sizeof(ExtendedStat));

	// delete entry cached with inode
	const std::string key1(keyFromAny(preKey, path));
	delMemcachedFromKey(key1);

	// delete entry cached with parent_inode + filename
	parent = this->getParent(path, &parentPath, &name);
	const std::string key2(keyFromAny(preKey, parent.stat.st_ino, name));
	delMemcachedFromKey(key2);

	return;
}
