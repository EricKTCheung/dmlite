/// @file    plugins/memcache/MemcacheCommon.cpp
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>

#include "MemcacheCommon.h"

using namespace dmlite;



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



const std::string MemcacheCommon::keyFromString(const char* preKey,
    const std::string& key)
{
  std::stringstream streamKey;

  streamKey << preKey << ":" << key;

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

  PoolGrabber<memcached_st*> conn = PoolGrabber<memcached_st*>(*this->connPool_);

  Log(Logger::DEBUG, memcachelogmask, memcachelogname,
      "starting to retrieve value from memcached, key: " <<
      key);

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
    //syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s:: %s: %s", this->decoratedId_,
    //    "getting a value from memcache failed",
    //    memcached_strerror(conn, statMemc));
    throw MemcacheException(statMemc, conn);
  }

  Log(Logger::INFO, memcachelogmask, memcachelogname,
      "successfully retrieved value from memcached, key: " <<
      key);

  if (lenValue > 0) {
    valMemcStr.assign(valMemc, lenValue);
    free(valMemc);
  }
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

  Log(Logger::DEBUG, memcachelogmask, memcachelogname,
      "starting to set value to memcached, key: " <<
      key);

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
    //syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s:: %s: %s", this->decoratedId_,
    //    "setting a value to memcache failed",
    //    memcached_strerror(conn, statMemc));
    throw MemcacheException(statMemc, conn);
  }

  Log(Logger::INFO, memcachelogmask, memcachelogname,
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

  Log(Logger::DEBUG, memcachelogmask, memcachelogname,
      "starting to add value to memcached, key: " <<
      key);

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
    //syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s:: %s: %s", this->decoratedId_,
    //    "adding a value to memcache failed",
    //    memcached_strerror(conn, statMemc));
    throw MemcacheException(statMemc, conn);
  }

  Log(Logger::INFO, memcachelogmask, memcachelogname,
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

  Log(Logger::DEBUG, memcachelogmask, memcachelogname,
      "starting to delete value to memcached, key: " <<
      key);

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

  Log(Logger::INFO, memcachelogmask, memcachelogname,
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

  //std::string serialString;
  SerialExtendedStat seStat;
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
  //  seStat.PrintDebugString();

  return;
}


void MemcacheCommon::deserializeExtendedStat(const std::string& serial_str, ExtendedStat& var)
{
  //GOOGLE_PROTOBUF_VERIFY_VERSION;

  SerialExtendedStat seStat;
  const SerialStat* pntSerialStat;

  seStat.ParseFromString(serial_str);
  //  seStat.PrintDebugString();

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
}


std::string MemcacheCommon::serializeReplica(const Replica& replica)
{
  SerialReplica serialReplica;

  serialReplica.set_replicaid(replica.replicaid);
  serialReplica.set_fileid(replica.fileid);
  serialReplica.set_nbaccesses(replica.nbaccesses);
  serialReplica.set_atime(replica.atime);
  serialReplica.set_ptime(replica.ptime);
  serialReplica.set_ltime(replica.ltime);
  serialReplica.set_status(std::string(1,replica.status));
  serialReplica.set_type(std::string(1,replica.type));
  serialReplica.set_pool(replica.getString("pool"));
  serialReplica.set_server(replica.server);
  serialReplica.set_filesystem(replica.getString("filesystem"));
  serialReplica.set_url(replica.rfn);

  return serialReplica.SerializeAsString();
}


void MemcacheCommon::deserializeReplica(const std::string& serial_str, Replica& replica)
{
  SerialReplica serialReplica;
  serialReplica.ParseFromString(serial_str);

  replica.replicaid = serialReplica.replicaid();
  replica.fileid = serialReplica.fileid();
  replica.nbaccesses = serialReplica.nbaccesses();
  replica.atime  = serialReplica.atime();
  replica.ptime  = serialReplica.ptime();
  replica.ltime  = serialReplica.ltime();
  replica.status = static_cast<Replica::ReplicaStatus>(serialReplica.status()[0]);
  replica.type   = static_cast<Replica::ReplicaType>(serialReplica.type()[0]);
  replica.server = serialReplica.server();
  replica.rfn    = serialReplica.url();

  replica["pool"]       = serialReplica.pool();
  replica["filesystem"] = serialReplica.filesystem();
}


std::string MemcacheCommon::serializeReplicaList(const std::vector<Replica>& vecRepl)
{
  SerialReplica* pReplica;
  SerialReplicaList list;
  std::string serialList;

  std::vector<Replica>::const_iterator itVecRepl;
  for (itVecRepl = vecRepl.begin();
       itVecRepl != vecRepl.end();
       ++itVecRepl) {

    pReplica = list.add_replica();

    pReplica->set_replicaid(itVecRepl->replicaid);
    pReplica->set_fileid(itVecRepl->fileid);
    pReplica->set_nbaccesses(itVecRepl->nbaccesses);
    pReplica->set_atime(itVecRepl->atime);
    pReplica->set_ptime(itVecRepl->ptime);
    pReplica->set_ltime(itVecRepl->ltime);
    pReplica->set_status(std::string(1,itVecRepl->status));
    pReplica->set_type(std::string(1,itVecRepl->type));
    pReplica->set_pool(itVecRepl->getString("pool"));
    pReplica->set_server(itVecRepl->server);
    pReplica->set_filesystem(itVecRepl->getString("filesystem"));
    pReplica->set_url(itVecRepl->rfn);

  }

  serialList = list.SerializeAsString();

  return serialList;
}


void MemcacheCommon::deserializeReplicaList(const std::string& serial_str, std::vector<Replica>& vecRepl)
{
  SerialReplica serialReplica;
  SerialReplicaList list;
  list.ParseFromString(serial_str);
  Replica replica;

  for (int i = 0; i < list.replica_size(); ++i) {
    serialReplica = list.replica(i);

    replica.replicaid = serialReplica.replicaid();
    replica.fileid = serialReplica.fileid();
    replica.nbaccesses = serialReplica.nbaccesses();
    replica.atime  = serialReplica.atime();
    replica.ptime  = serialReplica.ptime();
    replica.ltime  = serialReplica.ltime();
    replica.status = static_cast<Replica::ReplicaStatus>(serialReplica.status()[0]);
    replica.type   = static_cast<Replica::ReplicaType>(serialReplica.type()[0]);
    replica.server = serialReplica.server();
    replica.rfn    = serialReplica.url();

    replica["pool"]       = serialReplica.pool();
    replica["filesystem"] = serialReplica.filesystem();

    vecRepl.push_back(replica);
  }
}


std::string MemcacheCommon::serializeComment(const std::string& var)
{
  //GOOGLE_PROTOBUF_VERIFY_VERSION;

  std::string serialString;
  SerialComment seComment;

  seComment.set_comment(var);

  return seComment.SerializeAsString();
}


void MemcacheCommon::deserializeComment(std::string& serial_str,
                                         std::string& var)
{
  //GOOGLE_PROTOBUF_VERIFY_VERSION;

  SerialComment seComment;

  seComment.ParseFromString(serial_str);
//  seStat.PrintDebugString();

  var = seComment.comment();
}


std::string MemcacheCommon::serializePoolList(const std::vector<Pool>& vecPool)
{
  SerialPool* pPool;
  SerialPoolList list;
  std::string serialList;

  std::vector<Pool>::const_iterator itVecPool;
  for (itVecPool = vecPool.begin();
       itVecPool != vecPool.end();
       ++itVecPool) {

    pPool = list.add_pool();

    pPool->set_name(itVecPool->name);
    pPool->set_type(itVecPool->type);
  }

  serialList = list.SerializeAsString();

  return serialList;
}


void MemcacheCommon::deserializePoolList(const std::string& serial_str, std::vector<Pool>& vecPool)
{
  SerialPool serialPool;
  SerialPoolList list;
  list.ParseFromString(serial_str);
  Pool pool;

  for (int i = 0; i < list.pool_size(); ++i) {
    serialPool = list.pool(i);

    pool.name = serialPool.name();
    pool.type = serialPool.type();

    vecPool.push_back(pool);
  }
}


std::string MemcacheCommon::serializePool(const Pool& pool)
{
  SerialPool serialPool;

  serialPool.set_name(pool.name);
  serialPool.set_type(pool.type);

  return serialPool.SerializeAsString();
}



void MemcacheCommon::deserializePool(const std::string& serial_str, Pool& pool)
{
  SerialPool serialPool;
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
  if (path[0] == '/') {
    return path;
  } else {
    std::string cwd = this->cwd_;
    if (path.length() == 0) {
      return cwd;
    }
    if (path.length() == 1 && path[0] == '.') {
      return cwd;
    }
    return Url::normalizePath(cwd + "/" + path);
  }
}


std::string MemcacheCommon::getBasePath(const std::string& path)
  throw (std::bad_alloc, std::length_error)
{
  size_t lastPos = path.length()-1;
  if (path[lastPos] == '/') {
    --lastPos;
  }
  // +1 to copy also the '/' later
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

