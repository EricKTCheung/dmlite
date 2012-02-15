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
} // namespace serialization
} // namespace boost


using namespace dmlite;


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
																 Catalog* decorates) throw (DmException):
   DummyCatalog(decorates)
{
  this->connectionPool_ = connPool;
  this->conn_           = connPool->acquire();

	// TODO: user credentials
	this->uid_ = -1;
	this->gid_ = -1;
}



MemcacheCatalog::~MemcacheCatalog() throw (DmException)
{
}

void MemcacheCatalog::setUserId(uid_t uid, gid_t gid, const std::string& dn) throw (DmException)
{
	this->uid_ = uid;
	this->gid_ = gid;

	DELEGATE(setUserId, uid, gid, dn);
}

struct stat MemcacheCatalog::stat(const std::string& path) throw (DmException)
{
//	printf("run stat:\n");
	struct stat stStat;
	std::string valMemc;

	const std::string key = keyFromAny("STAT", path);

	valMemc = valFromMemcachedKey(key);

	if (!valMemc.empty())
	{
		DESERIALIZE(valMemc, stStat, SERIALIZE_VERSION);
	} else // valMemc was not in memcached
	{
		DELEGATE_ASSIGN(stStat, stat, path);
		SERIALIZE(valMemc, stStat, SERIALIZE_VERSION);
		setMemcachedFromKeyValue(key, valMemc);
	}

	return stStat;
}

struct stat MemcacheCatalog::stat(ino_t inode) throw (DmException)
{
//	printf("run stat:\n");
	struct stat stStat;
	std::string valMemc;

	const std::string key = keyFromAny("STAT", inode);

	valMemc = valFromMemcachedKey(key);

	if (!valMemc.empty())
	{
		DESERIALIZE(valMemc, stStat, SERIALIZE_VERSION);
	} else // valMemc was not in memcached
	{
		DELEGATE_ASSIGN(stStat, stat, inode);
		SERIALIZE(valMemc, stStat, SERIALIZE_VERSION);
		setMemcachedFromKeyValue(key, valMemc);
	}

	return stStat;
}

void MemcacheCatalog::removeDir(const std::string& path) throw (DmException)
{
	const std::string key(keyFromAny("STAT", path));

	delMemcachedFromKey(key);

	DELEGATE(removeDir, path);
}

void MemcacheCatalog::unlink(const std::string& path) throw (DmException)
{
	const std::string key(keyFromAny("STAT", path));

	delMemcachedFromKey(key);

	DELEGATE(unlink, path);
}
 
const std::string MemcacheCatalog::keyFromAny(const char* preKey,
																							ino_t inode)
{
	std::stringstream streamKey;
	// add prefix
	streamKey << preKey << ':';
	// add userinfo
	if (this->uid_ != -1 && this->gid_ != -1)
		streamKey << this->uid_ << ':' << this->gid_ << ':';
	// add argument
	streamKey << inode;

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
	// add userinfo
	if (this->uid_ != -1 && this->gid_ != -1)
		streamKey << this->uid_ << ':' << this->gid_ << ':';
	
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
//		printf("	using cached value.\n");
	} else
	{
//		printf("	using db value.\n");
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
													 (time_t)MEMCACHE_EXPIRATION,
  												 (uint32_t)0);

	if (statMemc != MEMCACHED_SUCCESS)
	{
		throw DmException(DM_UNKNOWN_ERROR,
											std::string(memcached_strerror(this->conn_,
																										 statMemc)));
	}
//	printf("	set value to cache.\n");

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
//		printf("	deleted value %s from cache.\n", key.c_str());
	
	return;
}
