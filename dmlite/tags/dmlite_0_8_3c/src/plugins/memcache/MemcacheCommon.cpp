/// @file    plugins/memcache/MemcacheCommon.cpp
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
/// @author  Andrea Manzi <amanzi@cern.ch>

#include "MemcacheCommon.h"

using namespace dmlite;

LocalCacheList MemcacheCommon::localCacheList;
LocalCacheMap MemcacheCommon::localCacheMap;
int MemcacheCommon::localCacheEntryCount = 0;
int MemcacheCommon::localCacheMaxSize = 1000;
time_t MemcacheCommon::localCacheExpirationTimeout = 60;
boost::mutex MemcacheCommon::localCacheMutex;
MemcacheCommon::LocalCacheStats MemcacheCommon::localCacheStats;

MemcacheCommon::MemcacheCommon(PoolContainer<memcached_st*>& connPool,
    MemcacheFunctionCounter* funcCounter,
    bool doFuncCount,
    time_t memcachedExpirationLimit):
  connPool_(&connPool),
  connNoReply_(0x0),
  funcCounter_(funcCounter),
  doFuncCount_(doFuncCount),
  memcachedExpirationLimit_(memcachedExpirationLimit)
{
  // Nothing
}

const std::string MemcacheCommon::computeMd5(const std::string& key)
{
    unsigned char digest[16];
    MD5_CTX ctx;
    MD5_Init(&ctx);
    MD5_Update(&ctx, key.c_str(), key.size());
    MD5_Final(digest, &ctx);

    const size_t nbytes = 33;
    char  buffer[nbytes];
    char *p;

    p = buffer;
    for (size_t offset = 0; offset < nbytes; ++offset, p += 2)
	    sprintf(p, "%02x", digest[offset]);
     *p = '\0';

    return std::string(buffer);

}


const std::string MemcacheCommon::keyFromString(const char* preKey,
    const std::string& key)
{
  std::stringstream streamKey;
  std::string key_path;

  const unsigned int strlen_path = key.length();

  if ( strlen_path > 200){
        Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Long key, computing Md5 hash");
	std::string hash = computeMd5(key);
	Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Hash: "+ hash);
	key_path.append(hash);
  }else {
	key_path.append(key);
  }

  streamKey << preKey << ":" << key_path;

  return streamKey.str();
}

const std::string MemcacheCommon::keyFromURI(const char* preKey,
    const std::string& uri)
{
  std::stringstream streamKey;
  std::string key_path;

  key_path.append(uri);

  // add prefix
  streamKey << preKey << ':';

  const unsigned int strlen_path = key_path.length();
  int idx_path_substr = strlen_path - (250 - 50);

  if (idx_path_substr < 0)
    idx_path_substr = 0;

  streamKey << key_path.substr(idx_path_substr);

  return streamKey.str();
}


const std::string MemcacheCommon::getValFromMemcachedKey(const std::string& key)  throw (MemcacheException)
{
  memcached_return statMemc;
  size_t lenValue;
  uint32_t flags;
  char* valMemc;
  std::string valMemcStr;

  // try the local cache
  if (localCacheMaxSize > 0) {
    valMemcStr = getValFromLocalKey(key);
    if (!valMemcStr.empty()) {
      Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting with value from local cache.");
      return valMemcStr;
    }
  }

  PoolGrabber<memcached_st*> conn = PoolGrabber<memcached_st*>(*this->connPool_);

  Log(Logger::Lvl4, memcachelogmask, memcachelogname,
      "starting to retrieve value from memcached:" <<
      " key: " << key.data() <<
      " length: " << key.length());

  valMemc = memcached_get(conn,
      key.data(),
      key.length(),
      &lenValue,
      &flags,
      &statMemc);

  if (statMemc != MEMCACHED_SUCCESS &&
      statMemc != MEMCACHED_NOTFOUND) {
    Err(memcachelogname,
        "getting a value from memcache failed: " <<
        memcached_strerror(conn, statMemc));
    throw MemcacheException(statMemc, conn);
  }

  Log(Logger::Lvl3, memcachelogmask, memcachelogname,
      "successfully retrieved value from memcached, key: " <<
      key);

  if (lenValue > 0) {
    valMemcStr.assign(valMemc, lenValue);
    free(valMemc);
  }

  // add the element to the local cache
  //if (localCacheMaxSize > 0 && lenValue > 0) {
  //  setLocalFromKeyValue(key, valMemcStr);
  //}

  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting with value from memcached.");
  return valMemcStr;
}


const std::string MemcacheCommon::safeGetValFromMemcachedKey(const std::string& key) throw ()
{
  try {
    return getValFromMemcachedKey(key);
  } catch (MemcacheException) {
    Err(memcachelogname, "ignore memcached get failure");
    return std::string(); /* pass */
  }
}


void MemcacheCommon::setMemcachedFromKeyValue(const std::string& key,
    const std::string& value, const bool noreply) throw (MemcacheException)
{
  PoolGrabber<memcached_st*> conn = PoolGrabber<memcached_st*>(*this->connPool_);

  //memcached_st *conn;
  //if (noreply)
  //  conn = this->connNoReply_;
  //else
  //  conn = this->conn_;

  //unsigned int randExpLimit = rand() & 0x3F; // add up to 63 random seconds

  // add to local cache
  //if (localCacheMaxSize > 0) {
  //  setLocalFromKeyValue(key, value);
  //}

  Log(Logger::Lvl4, memcachelogmask, memcachelogname,
      "starting to set value to memcached:" <<
      " key: " << key.data() <<
      " length: " << key.length() <<
      " value: " << value.data() <<
      " vlength: " << value.length());

  memcached_return statMemc;
  statMemc = memcached_set(conn,
      key.data(),
      key.length(),
      value.data(), value.length(),
      this->memcachedExpirationLimit_, // + randExpLimit,
      (uint32_t)0);

  if (statMemc != MEMCACHED_SUCCESS) {
    Err(memcachelogname,
        "setting a value to memcache failed: " <<
        memcached_strerror(conn, statMemc));
    throw MemcacheException(statMemc, conn);
  }

  Log(Logger::Lvl3, memcachelogmask, memcachelogname,
      "successfully set value to memcached, key: " <<
      key);

  return;
}


void MemcacheCommon::safeSetMemcachedFromKeyValue(const std::string& key,
    const std::string& value) throw ()
{
  try {
    // set noreply true, since we don't look at the return anyway
    return setMemcachedFromKeyValue(key, value, true);
  } catch (MemcacheException) {
    Err(memcachelogname, "ignore memcached set failure");
    return; /* pass */
  }
}


void MemcacheCommon::addMemcachedFromKeyValue(const std::string& key,
    const std::string& value) throw (MemcacheException)
{
  PoolGrabber<memcached_st*> conn = PoolGrabber<memcached_st*>(*this->connPool_);

  Log(Logger::Lvl4, memcachelogmask, memcachelogname,
      "starting to add value to memcached:" <<
      " key: " << key.data() <<
      " length: " << key.length() <<
      " value: " << value.data() <<
      " vlength: " << value.length());

  memcached_return statMemc;
  statMemc = memcached_add(conn,
      key.data(),
      key.length(),
      value.data(), value.length(),
      this->memcachedExpirationLimit_,
      (uint32_t)0);

  if (statMemc != MEMCACHED_SUCCESS) {
    Err(memcachelogname,
        "adding a value to memcache failed: " <<
        memcached_strerror(conn, statMemc));
    throw MemcacheException(statMemc, conn);
  }

  Log(Logger::Lvl3, memcachelogmask, memcachelogname,
      "successfully added value to memcached, key: " <<
      key);

  return;
}


void MemcacheCommon::safeAddMemcachedFromKeyValue(const std::string& key,
    const std::string& value) throw ()
{
  try {
    return addMemcachedFromKeyValue(key, value);
  } catch (MemcacheException) {
    Err(memcachelogname, "ignore memcached add failure");
    return; /* pass */
  }
}


void MemcacheCommon::delMemcachedFromKey(const std::string& key, const bool noreply) throw (MemcacheException)
{
  PoolGrabber<memcached_st*> conn = PoolGrabber<memcached_st*>(*this->connPool_);

  //memcached_st *conn;
  //if (noreply)
  //  conn = this->connNoReply_;
  //else
  //  conn = this->conn_;

  // delete from local cache
  if (localCacheMaxSize > 0) {
    delLocalFromKey(key);
  }

  Log(Logger::Lvl4, memcachelogmask, memcachelogname,
      "starting to delete value to memcached:" <<
      " key: " << key.data() <<
      " length: " << key.length());

  memcached_return statMemc;
  statMemc = memcached_delete(conn,
      key.data(),
      key.length(),
      (time_t)0);

  if (statMemc != MEMCACHED_SUCCESS &&
      statMemc != MEMCACHED_NOTFOUND) {
    Err(memcachelogname,
        "deleting a value from memcache failed: " <<
        memcached_strerror(conn, statMemc));
    throw MemcacheException(statMemc, conn);
  }

  Log(Logger::Lvl3, memcachelogmask, memcachelogname,
      "successfully deleted value from memcached, key: " <<
      key);
}


void MemcacheCommon::safeDelMemcachedFromKey(const std::string& key) throw ()
{
  try {
    // set noreply true, since we don't look at the return anyway
    return delMemcachedFromKey(key, true);
  } catch (MemcacheException) {
    Err(memcachelogname, "ignore memcached delete failure");
    return; /* pass */
  }
}


/* serialization functions */

void MemcacheCommon::serializeExtendedStat(const ExtendedStat& var, std::string& serialString)
{
  //GOOGLE_PROTOBUF_VERIFY_VERSION;

  SerialStat* pntSerialStat;

  pntSerialStat = seStat.mutable_stat();
  seStat.set_parent(var.parent);
  seStat.set_type(var.getLong("type"));
  seStat.set_status(std::string(1,var.status));
  seStat.set_name(var.name);
  seStat.set_guid(var.guid);
  seStat.set_csumtype(var.csumtype);
  seStat.set_csumvalue(var.csumvalue);
  seStat.set_acl(var.acl.serialize());

  //serialize xtended attributes
  if (var.getKeys().size() > 0) {
	SerialExtendedAttribute* pExtendedAttr;
        SerialExtendedAttributeList* serialExtendedAttributeList;
	serialExtendedAttributeList = seStat.mutable_xattrlist();
	serialExtendedAttributeList->Clear();
  	std::vector<std::string> keys = var.getKeys();
        for (unsigned i = 0; i < keys.size(); ++i) {
                 Log(Logger::Lvl4, memcachelogmask, memcachelogname,
		      "serialize xattr to memcache: key: " << keys[i] << " value: " << var.getString(keys[i]));
		 pExtendedAttr = serialExtendedAttributeList->add_xattr();
		 pExtendedAttr->set_name(keys[i]);
		 pExtendedAttr->set_value(var.getString(keys[i]));
	}
  }

  pntSerialStat->set_st_dev(var.stat.st_dev);
  pntSerialStat->set_st_ino(var.stat.st_ino);
  pntSerialStat->set_st_mode(var.stat.st_mode);
  pntSerialStat->set_st_nlink(var.stat.st_nlink);
  pntSerialStat->set_st_uid(var.stat.st_uid);
  pntSerialStat->set_st_gid(var.stat.st_gid);
  pntSerialStat->set_st_rdev(var.stat.st_rdev);
  pntSerialStat->set_st_size(var.stat.st_size);
  pntSerialStat->set_st_access_time(var.stat.st_atime);
  pntSerialStat->set_st_modified_time(var.stat.st_mtime);
  pntSerialStat->set_st_change_time(var.stat.st_ctime);
  pntSerialStat->set_st_blksize(var.stat.st_blksize);
  pntSerialStat->set_st_blocks(var.stat.st_blocks);

  serialString = seStat.SerializeAsString();

  return;
}


void MemcacheCommon::deserializeExtendedStat(const std::string& serial_str, ExtendedStat& var)
{
  //GOOGLE_PROTOBUF_VERIFY_VERSION;

  const SerialStat* pntSerialStat;

  seStat.ParseFromString(serial_str);

  pntSerialStat = &seStat.stat();

  var.stat.st_dev = pntSerialStat->st_dev();
  var.stat.st_ino = pntSerialStat->st_ino();
  var.stat.st_mode = pntSerialStat->st_mode();
  var.stat.st_nlink = pntSerialStat->st_nlink();
  var.stat.st_uid = pntSerialStat->st_uid();
  var.stat.st_gid = pntSerialStat->st_gid();
  var.stat.st_rdev = pntSerialStat->st_rdev();
  var.stat.st_size = pntSerialStat->st_size();
  var.stat.st_atime = pntSerialStat->st_access_time();
  var.stat.st_mtime = pntSerialStat->st_modified_time();
  var.stat.st_ctime = pntSerialStat->st_change_time();
  var.stat.st_blksize = pntSerialStat->st_blksize();
  var.stat.st_blocks = pntSerialStat->st_blocks();

  var.parent  = seStat.parent();
  var["type"] = seStat.type();
  var.status  = static_cast<ExtendedStat::FileStatus>(seStat.status()[0]); // status is only one char

  var.name      = seStat.name();
  var.guid      = seStat.guid();
  var.csumtype  = seStat.csumtype();
  var.csumvalue = seStat.csumvalue();
  var.acl       = Acl(seStat.acl());

  //deserialize xattr
  if (seStat.has_xattrlist()) {
	  const SerialExtendedAttributeList* serialExtendedAttributeList;
	  serialExtendedAttributeList = &seStat.xattrlist();
	  SerialExtendedAttribute xattr;
	  Log(Logger::Lvl4, memcachelogmask, memcachelogname,"Found xattr on memcache");

	  for (int i = 0; i < serialExtendedAttributeList->xattr_size(); ++i) {
	      Log(Logger::Lvl4, memcachelogmask, memcachelogname,"deserialize xattr to memcache: key: " << xattr.name() <<  " value: " << xattr.value());
	      xattr = serialExtendedAttributeList->xattr(i);
	      var[xattr.name()] = xattr.value();
	  }
  	}
}


std::string MemcacheCommon::serializeReplica(const Replica& replica)
{
  serialReplica.set_replicaid(replica.replicaid);
  serialReplica.set_fileid(replica.fileid);
  serialReplica.set_nbaccesses(replica.nbaccesses);
  serialReplica.set_atime(replica.atime);
  serialReplica.set_ptime(replica.ptime);
  serialReplica.set_ltime(replica.ltime);
  serialReplica.set_status(std::string(1,replica.status));
  serialReplica.set_type(std::string(1,replica.type));
  serialReplica.set_setname(replica.setname);
  serialReplica.set_pool(replica.getString("pool"));
  serialReplica.set_server(replica.server);
  serialReplica.set_filesystem(replica.getString("filesystem"));
  serialReplica.set_url(replica.rfn);
  //serialize xtended attributes
  if (replica.getKeys().size() > 0) {
        SerialExtendedAttribute* pExtendedAttr;
        SerialExtendedAttributeList* serialExtendedAttributeList;
        serialExtendedAttributeList = serialReplica.mutable_xattrlist();
        serialExtendedAttributeList->Clear();
        std::vector<std::string> keys = replica.getKeys();
        for (unsigned i = 0; i < keys.size(); ++i) {
                 Log(Logger::Lvl4, memcachelogmask, memcachelogname,
                      "serialize xattr to memcache: key: " << keys[i] << " value: " << replica.getString(keys[i]));
                 pExtendedAttr = serialExtendedAttributeList->add_xattr();
                 pExtendedAttr->set_name(keys[i]);
                 pExtendedAttr->set_value(replica.getString(keys[i]));
        }
  }


  return serialReplica.SerializeAsString();
}


void MemcacheCommon::deserializeReplica(const std::string& serial_str, Replica& replica)
{
  serialReplica.ParseFromString(serial_str);

  replica.replicaid = serialReplica.replicaid();
  replica.fileid = serialReplica.fileid();
  replica.nbaccesses = serialReplica.nbaccesses();
  replica.atime  = serialReplica.atime();
  replica.ptime  = serialReplica.ptime();
  replica.ltime  = serialReplica.ltime();
  replica.status = static_cast<Replica::ReplicaStatus>(serialReplica.status()[0]);
  replica.type   = static_cast<Replica::ReplicaType>(serialReplica.type()[0]);
  replica.setname = serialReplica.setname();
  replica.server = serialReplica.server();
  replica.rfn    = serialReplica.url();

  replica["pool"]       = serialReplica.pool();
  replica["filesystem"] = serialReplica.filesystem();
  //deserialize xattr
  if (serialReplica.has_xattrlist()) {
          const SerialExtendedAttributeList* serialExtendedAttributeList;
          serialExtendedAttributeList = &serialReplica.xattrlist();
          SerialExtendedAttribute xattr;
          Log(Logger::Lvl4, memcachelogmask, memcachelogname,"Found xattr on memcache");

          for (int i = 0; i < serialExtendedAttributeList->xattr_size(); ++i) {
              Log(Logger::Lvl4, memcachelogmask, memcachelogname,"deserialize xattr to memcache: key: " << xattr.name() <<  " value: " << xattr.value());
              xattr = serialExtendedAttributeList->xattr(i);
              replica[xattr.name()] = xattr.value();
          }
        }

}


std::string MemcacheCommon::serializeReplicaList(const std::vector<Replica>& vecRepl)
{
  SerialReplica* pReplica;
  serialReplicaList.Clear();

  std::vector<Replica>::const_iterator itVecRepl;
  for (itVecRepl = vecRepl.begin();
       itVecRepl != vecRepl.end();
       ++itVecRepl) {

    // Files that have replicas that are under processing unfortunately cannot be cached at all
    if (itVecRepl->status != dmlite::Replica::kAvailable)
      return "";

    pReplica = serialReplicaList.add_replica();

    pReplica->set_replicaid(itVecRepl->replicaid);
    pReplica->set_fileid(itVecRepl->fileid);
    pReplica->set_nbaccesses(itVecRepl->nbaccesses);
    pReplica->set_atime(itVecRepl->atime);
    pReplica->set_ptime(itVecRepl->ptime);
    pReplica->set_ltime(itVecRepl->ltime);
    pReplica->set_status(std::string(1,itVecRepl->status));
    pReplica->set_type(std::string(1,itVecRepl->type));
    pReplica->set_setname(itVecRepl->setname);
    pReplica->set_pool(itVecRepl->getString("pool"));
    pReplica->set_server(itVecRepl->server);
    pReplica->set_filesystem(itVecRepl->getString("filesystem"));
    pReplica->set_url(itVecRepl->rfn);
    if (itVecRepl->getKeys().size() > 0) {
        SerialExtendedAttribute* pExtendedAttr;
        SerialExtendedAttributeList* serialExtendedAttributeList;
        serialExtendedAttributeList = pReplica->mutable_xattrlist();
        serialExtendedAttributeList->Clear();
        std::vector<std::string> keys = itVecRepl->getKeys();
        for (unsigned i = 0; i < keys.size(); ++i) {
                 Log(Logger::Lvl4, memcachelogmask, memcachelogname,
                      "serialize xattr to memcache: key: " << keys[i] << " value: " << itVecRepl->getString(keys[i]));
                 pExtendedAttr = serialExtendedAttributeList->add_xattr();
                 pExtendedAttr->set_name(keys[i]);
                 pExtendedAttr->set_value(itVecRepl->getString(keys[i]));
        }
    }
  }
  return serialReplicaList.SerializeAsString();
}



void MemcacheCommon::deserializeReplicaList(const std::string& serial_str, std::vector<Replica>& vecRepl)
{
  serialReplicaList.ParseFromString(serial_str);
  Replica replica;

  for (int i = 0; i < serialReplicaList.replica_size(); ++i) {
    serialReplica = serialReplicaList.replica(i);

    replica.replicaid = serialReplica.replicaid();
    replica.fileid = serialReplica.fileid();
    replica.nbaccesses = serialReplica.nbaccesses();
    replica.atime  = serialReplica.atime();
    replica.ptime  = serialReplica.ptime();
    replica.ltime  = serialReplica.ltime();
    replica.status = static_cast<Replica::ReplicaStatus>(serialReplica.status()[0]);
    replica.type   = static_cast<Replica::ReplicaType>(serialReplica.type()[0]);
    replica.setname = serialReplica.setname();
    replica.server = serialReplica.server();
    replica.rfn    = serialReplica.url();

    replica["pool"]       = serialReplica.pool();
    replica["filesystem"] = serialReplica.filesystem();

    //deserialize xattr
    if (serialReplica.has_xattrlist()) {
          const SerialExtendedAttributeList* serialExtendedAttributeList;
          serialExtendedAttributeList = &serialReplica.xattrlist();
          SerialExtendedAttribute xattr;
          Log(Logger::Lvl4, memcachelogmask, memcachelogname,"Found xattr on memcache");

          for (int i = 0; i < serialExtendedAttributeList->xattr_size(); ++i) {
              Log(Logger::Lvl4, memcachelogmask, memcachelogname,"deserialize xattr to memcache: key: " << xattr.name() <<  " value: " << xattr.value());
              xattr = serialExtendedAttributeList->xattr(i);
              replica[xattr.name()] = xattr.value();
          }
        }
    vecRepl.push_back(replica);
  }
}


std::string MemcacheCommon::serializeComment(const std::string& var)
{
  //GOOGLE_PROTOBUF_VERIFY_VERSION;
  seComment.set_comment(var);

  return seComment.SerializeAsString();
}


void MemcacheCommon::deserializeComment(std::string& serial_str,
                                         std::string& var)
{
  //GOOGLE_PROTOBUF_VERIFY_VERSION;

  seComment.ParseFromString(serial_str);

  var = seComment.comment();
}


std::string MemcacheCommon::serializePoolList(const std::vector<Pool>& vecPool)
{
  SerialPool* pPool;
  serialPoolList.Clear();

  std::vector<Pool>::const_iterator itVecPool;
  for (itVecPool = vecPool.begin();
       itVecPool != vecPool.end();
       ++itVecPool) {

    pPool = serialPoolList.add_pool();

    pPool->set_name(itVecPool->name);
    pPool->set_type(itVecPool->type);
  }

  return serialPoolList.SerializeAsString();
}


void MemcacheCommon::deserializePoolList(const std::string& serial_str, std::vector<Pool>& vecPool)
{
  serialPoolList.ParseFromString(serial_str);
  Pool pool;

  for (int i = 0; i < serialPoolList.pool_size(); ++i) {
    serialPool = serialPoolList.pool(i);

    pool.name = serialPool.name();
    pool.type = serialPool.type();

    vecPool.push_back(pool);
  }
}


std::string MemcacheCommon::serializePool(const Pool& pool)
{
  serialPool.set_name(pool.name);
  serialPool.set_type(pool.type);

  return serialPool.SerializeAsString();
}



void MemcacheCommon::deserializePool(const std::string& serial_str, Pool& pool)
{
  serialPool.ParseFromString(serial_str);

  pool.name = serialPool.name();
  pool.type = serialPool.type();
}


//std::string MemcacheCommon::normalizePath(const std::string& path)
//{
//  std::vector<std::string> components = Url::splitPath(path);
//  std::string              result;
//
//  result.reserve(path.length());
//
//  unsigned i;
//  if (components[0] == "/") {
//    i      = 1;
//    result = "/";
//  } else {
//    i = 0;
//  }
//
//  for ( ; i < components.size(); ++i) {
//    if (components[i] != ".." && components[i] != ".") {
//      result.append(components[i]);
//    }
//    if (i < components.size() - 1)
//      result.append("/");
//  }
//
//  if (components.size() > 1 && path[path.length() - 1] == '/')
//    result.append("/");
//
//  return result;
//}


std::string MemcacheCommon::getAbsolutePath(const std::string& path)
  throw (DmException)
{
  if (path == "/")
    return path;
  if (path[0] == '/') {
    std::string outPath = path;
    removeTrailingSlash(outPath);
    return outPath;
  } else {
    std::string cwd = this->cwd_;
    removeTrailingSlash(cwd);
    if (path.length() == 0) {
      return cwd;
    }
    if (path.length() == 1 && path[0] == '.') {
      return cwd;
    }
    return Url::normalizePath(cwd + "/" + path, false);
  }
}


void MemcacheCommon::removeTrailingSlash(std::string& path)
{
  if (*(path.end()-1) == '/') {
    path.erase(path.end()-1);
  }
}


std::string MemcacheCommon::getBasePath(const std::string& path)
  throw (std::bad_alloc, std::length_error)
{
  size_t lastPos = path.length()-1;
  if (path[lastPos] == '/') {
    --lastPos;
  }
  // do not copy the trailing slash
  size_t basepath_end = path.find_last_of('/', lastPos);

  if (basepath_end > 0)
    return std::string(path, 0, basepath_end);
  else
    return std::string("/");
}


std::string MemcacheCommon::concatPath(const std::string& basepath, const std::string& relpath)
  throw ()
{
  if (basepath[basepath.length()-1] == '/') {
    return basepath + relpath;
  } else {
    return basepath + "/" + relpath;
  }
}


void MemcacheCommon::setLocalFromKeyValue(const std::string& key, const std::string& value)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, key = " << key);
  LocalCacheEntry entry(key, value);
  unsigned int randomExpire = rand();
  const unsigned int expireIndicator = 4;
  // expire items with some probability: p = 1/2^(expireIndicator)
  randomExpire >>= sizeof(unsigned int)*8 - expireIndicator;
  {
    boost::lock_guard<boost::mutex> l(localCacheMutex);
    if (randomExpire == 0) {
      expireLocalItems();
      logLocalCacheStatistics();
      resetLocalCacheStats();
    }
    while (localCacheEntryCount > localCacheMaxSize) {
      purgeLocalItem();
    }
    localCacheList.push_front(std::make_pair(time(0), entry));
    localCacheMap[key] = localCacheList.begin();
    localCacheEntryCount++;
    localCacheStats.set++;
  }
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting. Entry added, key = " << key
                                                      << " # entries = " << localCacheEntryCount);
}

const std::string MemcacheCommon::getValFromLocalKey(const std::string& key)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, key = " << key);
  std::string value;
  LocalCacheMap::iterator it;
  bool val_found = false;
  {
    boost::lock_guard<boost::mutex> l(localCacheMutex);
    localCacheStats.get++;
    it = localCacheMap.find(key);
    if (it != localCacheMap.end()) {
      localCacheStats.hit++;
      val_found = true;
      //    (list::iterator)->Entry.value
      value = (it->second)->second.second;
      // move to front
      // splice keeps iterator-validity, so an already-moved item (by another thread)
      // can still be referenced. If it's deleted, though, the iterator is invalid, so this
      // has to be in the same lock section as .find(key).
      localCacheList.splice(localCacheList.begin(), localCacheList, it->second);
      localCacheMap[key] = localCacheList.begin();
    } else {
      localCacheStats.miss++;
    }
  }
  if (val_found) {
    Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting. Value found.");
  } else {
    Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting. No value found.");
  }
  return value;
}


void MemcacheCommon::delLocalFromKey(const std::string& key)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, key = " << key);
  {
    boost::lock_guard<boost::mutex> l(localCacheMutex);
    LocalCacheMap::iterator it = localCacheMap.find(key);
    if (it != localCacheMap.end()) {
      localCacheList.erase(it->second);
      localCacheMap.erase(it);
      localCacheEntryCount--;
      localCacheStats.del++;
    }  else {// else it's already been deleted by another thread
    Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Entry to delete did not exist, key = " << key);
    }
  }
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting. Entry deleted, key = " << key);
}


void MemcacheCommon::purgeLocalItem()
{
  /* Only use this within a unique_lock, otherwise
   * everyone will die!
   */
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering. Next to purge key = " << localCacheList.back().second.first);
  //                            ListItem.Entry.key
  localCacheMap.erase(localCacheList.back().second.first);
  localCacheList.pop_back();
  localCacheEntryCount--;
  localCacheStats.purged++;
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting. # entries = " << localCacheEntryCount);
}


void MemcacheCommon::expireLocalItems()
{
  /* Only use this within a unique_lock, otherwise
   * everyone will die!
   */
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering.");
  int expireCount = 0;
  time_t expirationTime = time(0) - localCacheExpirationTimeout;
  for (LocalCacheList::iterator it = localCacheList.begin(); it != localCacheList.end(); ) {
    if (it->first < expirationTime) {
      localCacheMap.erase(it->second.first);
      it = localCacheList.erase(it);
      localCacheEntryCount--;
      expireCount++;
    } else {
      it++;
    }
  }
  localCacheStats.expired += expireCount;
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting. Expired " << expireCount << " items."
      << localCacheEntryCount << " items left.");
}


void MemcacheCommon::logLocalCacheStatistics()
{
  /* Only use this within a unique_lock, otherwise
   * everyone will die!
   */
  if (Logger::get()->getLevel() >= Logger::Lvl4 && Logger::get()->isLogged(memcachelogmask)) {
    std::stringstream logStream;
    logStream << "local cache statistics:" << std::endl;
    logStream << "get: " << localCacheStats.get << std::endl;
    logStream << "set: " << localCacheStats.set << std::endl;
    logStream << "hit: " << localCacheStats.hit << std::endl;
    logStream << "miss: " << localCacheStats.miss << std::endl;
    logStream << "del: " << localCacheStats.del << std::endl;
    logStream << "purged: " << localCacheStats.purged << std::endl;
    logStream << "expired: " << localCacheStats.expired << std::endl;
    Log(Logger::Lvl4, memcachelogmask, memcachelogname, logStream.str());
  }
}


void MemcacheCommon::resetLocalCacheStats()
{
  /* Only use this within a unique_lock, otherwise
   * everyone will die!
   */
  bool doReset = false;
  if ((localCacheStats.get - (1LL << 40)) > 0) {
    doReset = true;
  }
  if ((localCacheStats.set - (1LL << 40)) > 0) {
    doReset = true;
  }
  if ((localCacheStats.hit - (1LL << 40)) > 0) {
    doReset = true;
  }
  if ((localCacheStats.miss - (1LL << 40)) > 0) {
    doReset = true;
  }
  if ((localCacheStats.del - (1LL << 40)) > 0) {
    doReset = true;
  }
  if ((localCacheStats.purged - (1LL << 40)) > 0) {
    doReset = true;
  }
  if ((localCacheStats.expired - (1LL << 40)) > 0) {
    doReset = true;
  }
  if (doReset) {
    localCacheStats.get = 0;
    localCacheStats.set = 0;
    localCacheStats.hit = 0;
    localCacheStats.miss = 0;
    localCacheStats.del = 0;
    localCacheStats.purged = 0;
    localCacheStats.expired = 0;
  }
}
