/// @file    plugins/memcache/Memcache.h
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
#include <vector>

#include "Memcache.h"
#include "MemcacheCatalog.pb.h"


using namespace dmlite;

/// Used to keep the Key Prefixes
enum {
	PRE_STAT = 0,
  PRE_REPL,
  PRE_REPL_LIST,
  PRE_REPL_BLACK_LIST,
  PRE_LINK,
  PRE_COMMENT
};

/// Used internally to define Key Prefixes.
/// Must match with PRE_* constants!
static const char* key_prefix[] = {
	"STAT",
  "REPL",
  "RPLI",
  "RPBL",
  "LINK",
  "CMNT"
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


MemcacheCatalog::MemcacheCatalog(PoolContainer<memcached_st*>* connPool, 
																 Catalog* decorates,
																 unsigned int symLinkLimit,
																 time_t memcachedExpirationLimit) throw (DmException):
   DummyCatalog(decorates),
	 symLinkLimit_(symLinkLimit),
//	 cwd_(0), 
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

std::string MemcacheCatalog::serialize(const ExtendedStat& var)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  std::string serialString;
  SerialExtendedStat seStat;
  SerialStat* pntSerialStat;

  pntSerialStat = seStat.mutable_stat();
  seStat.set_parent(var.parent);
  seStat.set_type(var.type);
  seStat.set_status(std::string(1,var.status));
  seStat.set_name(var.name);
  seStat.set_guid(var.guid);
  seStat.set_csumtype(var.csumtype);
  seStat.set_csumvalue(var.csumvalue);
  seStat.set_acl(var.acl);

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

  return serialString;
}

void MemcacheCatalog::deserialize(std::string& serial_str, ExtendedStat& var)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

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

  var.parent = seStat.parent();
  var.type = seStat.type();
  var.status = seStat.status()[0]; // status is only one char
  std::memcpy(&var.name, seStat.name().c_str(), seStat.name().length()+1);
  std::memcpy(&var.guid, seStat.guid().c_str(), seStat.guid().length()+1);
  std::memcpy(&var.csumtype, seStat.csumtype().c_str(), seStat.csumtype().length()+1);
  std::memcpy(&var.csumvalue, seStat.csumvalue().c_str(), seStat.csumvalue().length()+1);
  std::memcpy(&var.acl, seStat.acl().c_str(), seStat.acl().length()+1);
}

std::string MemcacheCatalog::serializeLink(const SymLink& var)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  std::string serialString;
  SerialSymLink seLink;

  seLink.set_fileid(var.fileId);
  seLink.set_link(var.link);

  return seLink.SerializeAsString();
}

void MemcacheCatalog::deserializeLink(std::string& serial_str,
                                      SymLink& var)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  SerialSymLink seLink;

  seLink.ParseFromString(serial_str);
//  seStat.PrintDebugString();

  var.fileId = seLink.fileid();
  std::memcpy(&var.link, seLink.link().c_str(),
              seLink.link().length()+1);
}

std::string MemcacheCatalog::serializeComment(const std::string& var)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  std::string serialString;
  SerialComment seComment;

  seComment.set_comment(var);

  return seComment.SerializeAsString();
}

void MemcacheCatalog::deserializeComment(std::string& serial_str,
                                         std::string& var)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  SerialComment seComment;

  seComment.ParseFromString(serial_str);
//  seStat.PrintDebugString();

  var = seComment.comment();
}

std::string MemcacheCatalog::serializeList(std::vector<std::string>& keyList, const bool isWhite)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  SerialKey* pntKey;
  SerialKeyList list;
  std::string serialList;
  std::vector<std::string>::iterator itKeyList;

  for (itKeyList = keyList.begin();
       itKeyList != keyList.end();
       itKeyList++)
  {
    pntKey = list.add_key(); 
    pntKey->set_key(*itKeyList);
    pntKey->set_white(isWhite);
  }

  serialList = list.SerializeAsString();
//  list.PrintDebugString();

  return serialList;
}

std::vector<std::string> MemcacheCatalog::deserializeList(std::string& serialList)
{
  std::vector<std::string> keyList;
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  SerialKey key;
  SerialKeyList list;
  list.ParseFromString(serialList);

  for (int i = 0; i < list.key_size(); i++) 
  {
    key = list.key(i);
    keyList.push_back(key.key());
  }
//  list.PrintDebugString();
  return keyList;
}

std::vector<std::string> MemcacheCatalog::deserializeBlackList(std::string& serialList)
{
  SerialKey key;
  std::set<std::string> bkeySet;
  std::set<std::string> keySet;
  std::vector<std::string> keyList;
  std::vector<std::string>::iterator keyListEnd; 

  GOOGLE_PROTOBUF_VERIFY_VERSION;

//  SerialBlackKey bkey;
  SerialKeyList list;
  list.ParseFromString(serialList);
//  list.PrintDebugString();

  for (int i = 0; i < list.key_size(); i++) 
  {
    key = list.key(i);
    if (key.white())
      keySet.insert(key.key());
    else
      bkeySet.insert(key.key());
  }

  keyList.resize(keySet.size());

  keyListEnd = std::set_difference(keySet.begin(), keySet.end(),
                                   bkeySet.begin(), bkeySet.end(),
                                   keyList.begin());  

  keyList.resize(keyListEnd - keyList.begin());

  return keyList;
}


std::string MemcacheCatalog::serializeFileReplica(const FileReplica& replica)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  SerialFileReplica repl;
  repl.set_replicaid(replica.replicaid);
  repl.set_fileid(replica.fileid);
  repl.set_nbaccesses(replica.nbaccesses);
  repl.set_atime(replica.atime);
  repl.set_ptime(replica.ptime);
  repl.set_ltime(replica.ltime);
  repl.set_status(std::string(1,replica.status));
  repl.set_type(std::string(1,replica.type));
  repl.set_pool(replica.pool);
  repl.set_server(replica.server);
  repl.set_filesystem(replica.filesystem);
  repl.set_url(replica.url);

//  repl.PrintDebugString();
  
  return repl.SerializeAsString();
}

FileReplica MemcacheCatalog::deserializeFileReplica(std::string& serial)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  FileReplica repl;
  SerialFileReplica serialRepl;

  serialRepl.ParseFromString(serial);
//  serialRepl.PrintDebugString();

  repl.replicaid = serialRepl.replicaid();
  repl.fileid = serialRepl.fileid();
  repl.nbaccesses = serialRepl.nbaccesses();
  repl.atime = serialRepl.atime();
  repl.ptime = serialRepl.ptime();
  repl.ltime = serialRepl.ltime();
  repl.status = serialRepl.status()[0];
  repl.type = serialRepl.type()[0];
  std::memcpy(&repl.pool, serialRepl.pool().c_str(), serialRepl.pool().length()+1); 
  std::memcpy(&repl.server, serialRepl.server().c_str(), serialRepl.server().length()+1); 
  std::memcpy(&repl.filesystem, serialRepl.filesystem().c_str(), serialRepl.filesystem().length()+1); 
  std::memcpy(&repl.url, serialRepl.url().c_str(), serialRepl.url().length()+1); 

  return repl;
}

std::string MemcacheCatalog::getImplId() throw ()
{
  return std::string("MemcacheCatalog");
}

void MemcacheCatalog::set(const std::string& key, va_list varg) throw(DmException)
{
  throw DmException(DM_UNKNOWN_OPTION, "Option " + key + " unknown");
}


ExtendedStat MemcacheCatalog::extendedStat(const std::string& path, bool followSym) throw (DmException)
{
	std::string cwdPath;
  DELEGATE_ASSIGN(cwdPath, getWorkingDir); 
	ino_t cwd;
  DELEGATE_ASSIGN(cwd, getWorkingDirI); 

  // Split the path always assuming absolute
  std::list<std::string> components = splitPath(path);

  // Iterate starting from absolute root (parent of /) (0)
  uint64_t     parent       = 0;
  unsigned     symLinkLevel = 0;
  ExtendedStat meta;
  std::string  c;

  // If path is absolute OR cwd is empty, start in root
  if (path[0] == '/' || cwdPath.empty()) {
    // Push "/", as we have to look for it too
    components.push_front("/");
    // Root parent "is" a dir and world-readable :)
    memset(&meta, 0x00, sizeof(ExtendedStat));
    meta.stat.st_mode = S_IFDIR | 0555 ;
  }
  // Relative, and cwd set, so start there
  else {
    parent = cwd;
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

	valMemc = getValFromMemcachedVersionedKey(key);
	if (!valMemc.empty())
	{
		deserialize(valMemc, meta);
	} else // valMemc was not in memcached
	{
		DELEGATE_ASSIGN(meta, extendedStat, fileId);
		valMemc = serialize(meta);
		setMemcachedFromVersionedKeyValue(key, valMemc);
	}

  return meta;
}

ExtendedStat MemcacheCatalog::extendedStat(uint64_t parent, const std::string& name) throw (DmException)
{
  ExtendedStat meta;

  memset(&meta, 0x00, sizeof(ExtendedStat));

	std::string valMemc;

	const std::string key = keyFromAny(key_prefix[PRE_STAT], parent, name);

	valMemc = getValFromMemcachedVersionedKey(key);
	if (!valMemc.empty())
	{
		deserialize(valMemc, meta);
	} else // valMemc was not in memcached
	{
		DELEGATE_ASSIGN(meta, extendedStat, parent, name);
		valMemc = serialize(meta);
		setMemcachedFromVersionedKeyValue(key, valMemc);
	}

  return meta;
}

SymLink MemcacheCatalog::readLink(ino_t linkId) throw(DmException)
{
  SymLink meta; 

  memset(&meta, 0x00, sizeof(SymLink));

	std::string valMemc;

	const std::string key = keyFromAny(key_prefix[PRE_LINK], linkId); 

	valMemc = getValFromMemcachedVersionedKey(key);
	if (!valMemc.empty())
	{
		deserializeLink(valMemc, meta);
	} else // valMemc was not in memcached
	{
		DELEGATE_ASSIGN(meta, readLink, linkId);
		valMemc = serializeLink(meta);
		setMemcachedFromVersionedKeyValue(key, valMemc);
	}

  return meta;
}

void MemcacheCatalog::removeDir(const std::string& path) throw (DmException)
{
	delMemcachedFromPath(key_prefix[PRE_STAT], path);
	DELEGATE(removeDir, path);
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
 	delMemcachedFromPath(key_prefix[PRE_STAT], path);
	DELEGATE(unlink, path);
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

void MemcacheCatalog::setAcl(const std::string& path, const std::vector<Acl>& acls) throw (DmException)
{
  DELEGATE(setAcl, path, acls);
	delMemcachedFromPath(key_prefix[PRE_STAT], path);
}

void MemcacheCatalog::utime(const std::string& path, const struct utimbuf* buf) throw (DmException)
{
  DELEGATE(utime, path, buf);
  delMemcachedFromPath(key_prefix[PRE_STAT], path);
}
/*
void MemcacheCatalog::utime(ino_t inode, const struct utimbuf* buf) throw (DmException)
{
  DELEGATE(utime, inode, buf);
  const std::string key = keyFromAny(key_prefix[PRE_STAT], inode);
  delMemcachedFromKey(key);
}
*/
std::string MemcacheCatalog::getComment(const std::string& path) throw(DmException)
{
  // Get the file and check we can read
  ExtendedStat meta = this->extendedStat(path);
  
  if (checkPermissions(this->user_, this->group_, this->groups_,
                       meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to read " + path);

  // Query
  std::string comment;
	std::string valMemc;

	const std::string key = keyFromAny(key_prefix[PRE_COMMENT], meta.stat.st_ino); 

	valMemc = getValFromMemcachedVersionedKey(key);
	if (!valMemc.empty())
	{
		deserializeComment(valMemc, comment);
	} else // valMemc was not in memcached
	{
		DELEGATE_ASSIGN(comment, getComment, path);
		valMemc = serializeComment(comment);
		setMemcachedFromVersionedKeyValue(key, valMemc);
	}
  return comment;
}

void MemcacheCatalog::setComment(const std::string& path, const std::string& comment) throw (DmException)
{
  DELEGATE(setComment, path, comment);
	delMemcachedFromPath(key_prefix[PRE_COMMENT], path);
}
/*
void MemcacheCatalog::setGuid(const std::string& path, const std::string& guid) throw (DmException)
{
  DELEGATE(setGuid, path, guid);
	delMemcachedFromPath(key_prefix[PRE_STAT], path);
}
*/
void MemcacheCatalog::addReplica(const std::string& guid, int64_t id,
                                 const std::string& server, const std::string& sfn,
                                 char status, char fileType,
                                 const std::string& poolName,
                                 const std::string& fileSystem) throw (DmException)
{
  DELEGATE(addReplica, guid, id, server, sfn, status,
           fileType, poolName, fileSystem);
  std::string key = keyFromURI(key_prefix[PRE_REPL], sfn);
  std::string listKey = keyFromAny(key_prefix[PRE_REPL_LIST], id);
  addToListFromMemcachedKey(listKey, key);
}

void MemcacheCatalog::deleteReplica(const std::string& guid, int64_t id,
                                   const std::string& sfn) throw (DmException)
{
  DELEGATE(deleteReplica, guid, id, sfn);
  std::string key = keyFromURI(key_prefix[PRE_REPL], sfn);
  std::string listKey = keyFromAny(key_prefix[PRE_REPL_LIST], id);
  delMemcachedFromKey(key);
  removeListItemFromMemcachedKey(listKey, key);
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
    return this->getReplicas(path, meta.stat.st_ino);
  }
  catch (DmException e) {
    if (e.code() == DM_NO_REPLICAS)
      throw DmException(DM_NO_REPLICAS, "No replicas available for " + path);
    throw;
  }
}

std::vector<FileReplica> MemcacheCatalog::getReplicas(const std::string& path, ino_t inode) throw (DmException)
{
  std::vector<FileReplica> replicas;
  std::vector<std::string> vecValMemc;
  FileReplica repl;

  // get replica list from memcached
  const std::string listKey = keyFromAny(key_prefix[PRE_REPL_LIST],
                                         inode);

  vecValMemc = getListFromMemcachedKey(listKey);

	if (vecValMemc.size() > 0)
	{
    std::vector<std::string>::iterator itRepl;
    for (itRepl = vecValMemc.begin();
         itRepl != vecValMemc.end();
         itRepl++)
    { 
      repl = deserializeFileReplica(*itRepl);
      replicas.push_back(repl);
    }
//    replicas = deserialize(vecValMemc);
  } else
  {
    // otherwise, get replicas from mysql
    DELEGATE_ASSIGN(replicas, getReplicas, path);

    // save replicas in memcached
    setMemcachedFromReplicas(replicas, inode);
  } 
  
  return replicas;
}

FileReplica MemcacheCatalog::get(const std::string& path) throw(DmException)
{
  // Get all the replicas
  std::vector<FileReplica> replicas = this->getReplicas(path);

  // Pick a random one
  int i = rand() % replicas.size();

  // Copy
  return replicas[i];
}

void MemcacheCatalog::replicaSetAccessTime(const std::string& replica) throw (DmException)
{
  DELEGATE(replicaSetAccessTime, replica);
  std::string key = keyFromURI(key_prefix[PRE_REPL], replica);
  delMemcachedFromKey(key);
}



void MemcacheCatalog::replicaSetLifeTime(const std::string& replica, time_t ltime) throw (DmException)
{
  DELEGATE(replicaSetLifeTime, replica, ltime);
  std::string key = keyFromURI(key_prefix[PRE_REPL], replica);
  delMemcachedFromKey(key);
}



void MemcacheCatalog::replicaSetStatus(const std::string& replica, char status) throw (DmException)
{
  DELEGATE(replicaSetStatus, replica, status);
  std::string key = keyFromURI(key_prefix[PRE_REPL], replica);
  delMemcachedFromKey(key);
}



void MemcacheCatalog::replicaSetType(const std::string& replica, char type) throw (DmException)
{
  DELEGATE(replicaSetType, replica, type);
  std::string key = keyFromURI(key_prefix[PRE_REPL], replica);
  delMemcachedFromKey(key);
}


std::string MemcacheCatalog::getParent(const std::string& path)
 																					throw (DmException)
{
	std::string cwdPath;
  DELEGATE_ASSIGN(cwdPath, getWorkingDir); 

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
  else if (!cwdPath.empty())
    return cwdPath;
  else
    return std::string("/");
}
	
ExtendedStat MemcacheCatalog::getParent(const std::string& path,
                                        std::string* parentPath,
                                        std::string* name)
                                        throw (DmException)
{
	std::string cwdPath;
  DELEGATE_ASSIGN(cwdPath, getWorkingDir); 
	ino_t cwd;
  DELEGATE_ASSIGN(cwd, getWorkingDirI); 

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
  else if (!cwdPath.empty())
    return this->extendedStat(cwd);
  else
    return this->extendedStat("/");
}

const std::string MemcacheCatalog::versionedKeyFromAny(uint64_t version,
                                                    const std::string key)
{
  std::stringstream streamKey;
  streamKey << version << ':' << key;

  return streamKey.str();
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

	std::string cwdPath;
  DELEGATE_ASSIGN(cwdPath, getWorkingDir); 

	// add prefix
	streamKey << preKey << ':';
	
  if (path[0] == '/' || cwdPath.empty())
	{
	  key_path = path;
	} else
	{
		key_path = cwdPath;
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

const std::string MemcacheCatalog::keyFromURI(const char* preKey,
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

const std::string MemcacheCatalog::getValFromMemcachedKey(const std::string key)
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

	if (lenValue > 0)
	{
		valMemcStr.assign(valMemc, lenValue);
	}
	return valMemcStr;
}

const std::string MemcacheCatalog::getValFromMemcachedVersionedKey(const std::string key)
{
	memcached_return statMemc;
	size_t lenValue;
	uint32_t flags;
	char* valMemc;
	std::string valMemcStr;
  uint64_t version;
  std::string versionedKey;

  statMemc = 
      memcached_increment(this->conn_,
                          key.data(),
                          key.length(),
                          0, // offset: only get value, don't incr
                          &version);

  // if the memcached call fails, the value does not exist,
  // error message is NOTFOUND,
  // thus we should not try to fetch it
  // return "" to fetch it later from the db
	if (statMemc != MEMCACHED_SUCCESS)
	{
    if (statMemc != MEMCACHED_NOTFOUND)
    {
  	  throw DmException(DM_UNKNOWN_ERROR,
	    								std::string(memcached_strerror(this->conn_,
		    																						 statMemc)));
    }
  } else // retrieve value 
  {
    versionedKey = versionedKeyFromAny(version, key);

  	valMemc = memcached_get(this->conn_,
	  												versionedKey.data(),
													  versionedKey.length(),
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

	  if (lenValue > 0)
	  {
		  valMemcStr.assign(valMemc, lenValue);
	  }
  }
	return valMemcStr;
}

std::vector<std::string> 
   MemcacheCatalog::getValListFromMultipleMemcachedKeys(
                            const std::vector<std::string>& keyList)
{
  std::vector<std::string> vecValMemc;
  std::vector<char*> keys;
  char* pntKey; 
  memcached_return statMemc;
	char* valMemc;
  size_t key_length[keyList.size()];

  // get size needed for keys, create key_length 
  // memcpy keys
  size_t keys_size = 0;
  for (int i = 0; i < keyList.size(); i++)
  {
    key_length[i] = keyList[i].size();
    keys_size += keyList[i].size();
    pntKey = (char*) malloc(keyList[i].size());
    std::memset(pntKey, 0x00, keyList[i].size());
    std::memcpy(pntKey, keyList[i].data(), keyList[i].size());
    keys.push_back(pntKey);
  }

  // mget results 
  statMemc = memcached_mget(this->conn_,
                            &keys[0],
                            key_length,
                            keyList.size());

	if (statMemc != MEMCACHED_SUCCESS)
	{
		throw DmException(DM_UNKNOWN_ERROR,
											std::string(memcached_strerror(this->conn_,
																										 statMemc)));
	}

  for (int i = 0; i < keyList.size(); i++)
  {
    free(keys[i]);
  }
 
  // actually fetch results in std::vector

  char return_key[MEMCACHED_MAX_KEY];
  size_t return_key_length;
  char *return_value;
  size_t lenValue;
	uint32_t flags;
	std::string valMemcStr;
  std::set<std::string> foundKeys;

  while (valMemc = memcached_fetch(this->conn_,
                                   return_key,
                                   &return_key_length,
                                   &lenValue,
                                   &flags,
                                   &statMemc))
  {
	  if (statMemc != MEMCACHED_SUCCESS)
	  {
		  throw DmException(DM_UNKNOWN_ERROR,
											std::string(memcached_strerror(this->conn_,
																										 statMemc)));
	  } else
    {
		  valMemcStr.assign(valMemc, lenValue);
      vecValMemc.push_back(valMemcStr);
      free(valMemc);
      foundKeys.insert(std::string(return_key, return_key_length));
    }
  }

  // invalidate result if not all keys are found
  if (foundKeys.size() < keyList.size())
    vecValMemc.resize(0);  

  //return vector
  return vecValMemc;
}

void MemcacheCatalog::setMemcachedFromKeyValue(const std::string key,
							  						  const std::string value)
{
	memcached_return statMemc;
	statMemc = memcached_set(this->conn_,
													 key.data(),
													 key.length(),
													 value.data(), value.length(),
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

void MemcacheCatalog::setMemcachedFromVersionedKeyValue(const std::string key,
							  						  const std::string strValue)
{
	memcached_return statMemc;
  uint64_t version;
  std::string versionedKey;

  // memcached_incr_with_initial does not work
  // with the memcached text protocol, thus it is
  // faked with the following calls
  /*
  statMemc = 
      memcached_increment_with_initial(this->conn_,
                                       key.data(),
                                       key.length(),
                                       (uint64_t) 1, // offset
                                       (uint64_t) 2, // initial value
                                       this->memcachedExpirationLimit_,
                                       &version);
  */
  statMemc = 
      memcached_increment(this->conn_,
                          key.data(),
                          key.length(),
                          1, // offset
                          &version);

  if (statMemc == MEMCACHED_NOTFOUND)
  {
    std::string strVersion("1");
    version = 1;
	  statMemc = memcached_set(this->conn_,
		  											 key.data(),
			  										 key.length(),
				  									 strVersion.data(), strVersion.length(),
					  								 this->memcachedExpirationLimit_,
    												 (uint32_t)0);
  }
  
	if (statMemc != MEMCACHED_SUCCESS)
	{
		throw DmException(DM_UNKNOWN_ERROR,
											std::string(memcached_strerror(this->conn_,
																										 statMemc)));
	}

  versionedKey = versionedKeyFromAny(version, key);

	statMemc = memcached_set(this->conn_,
													 versionedKey.data(),
													 versionedKey.length(),
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

void MemcacheCatalog::setMemcachedFromReplicas(std::vector<FileReplica> replicas, ino_t inode)
{
  std::string serialList;
  std::string serialReplica;
  std::vector<std::string> keys;

  for (int i = 0; i < replicas.size(); i++)
  {
    serialReplica =  serializeFileReplica(replicas[i]);
    keys.push_back(keyFromURI(key_prefix[PRE_REPL], replicas[i].url));

    setMemcachedFromKeyValue(keys.back(), serialReplica);
  }
  serialList = serializeList(keys);
  setMemcachedFromKeyValue(keyFromAny(key_prefix[PRE_REPL_LIST], inode),
                           serialList);
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
}

void MemcacheCatalog::delMemcachedFromVersionedKey(const std::string key)
{
	memcached_return statMemc;
  uint64_t version;

  statMemc = 
      memcached_increment(this->conn_,
                          key.data(),
                          key.length(),
                          1, // offset
                          &version);

	if (statMemc != MEMCACHED_SUCCESS &&
			statMemc != MEMCACHED_NOTFOUND)
	{
		throw DmException(DM_UNKNOWN_ERROR,
											std::string(memcached_strerror(this->conn_,
																										 statMemc)));
	}
}

void MemcacheCatalog::delMemcachedFromPath(const char* preKey, const std::string& path)
{
  std::string name;
  std::string parentPath;
  ino_t inode;
	ExtendedStat parent;
	std::memset(&parent, 0x00, sizeof(ExtendedStat));

	// delete entry cached with inode
  try
  {  
    inode = this->extendedStat(path).stat.st_ino;
  } catch (DmException e)
  {
    int code = e.code();
    if (code != DM_NO_SUCH_FILE)
      throw;
  }
  const std::string key1(keyFromAny(preKey, inode));
	delMemcachedFromVersionedKey(key1);

	// delete entry cached with parent_inode + filename
	parent = this->getParent(path, &parentPath, &name);
	const std::string key2(keyFromAny(preKey, parent.stat.st_ino, name));
	delMemcachedFromVersionedKey(key2);
}

std::vector<std::string> MemcacheCatalog::getListFromMemcachedKey(const std::string& listKey)
        throw (DmException)
{
  std::string valMemc;
  std::vector<std::string> keyList; 
  std::vector<std::string> vecValMemc; 

  valMemc = getValFromMemcachedKey(listKey);

  if (!valMemc.empty())
  {
    keyList = deserializeBlackList(valMemc);

    // get values
    if (keyList.size() > 0)
      vecValMemc = getValListFromMultipleMemcachedKeys(keyList);
  }

  return vecValMemc;
}

void MemcacheCatalog::addToListFromMemcachedKey(const std::string& listKey, const std::string& key)
{
	memcached_return statMemc;
  uint64_t version;
  std::string serialKey;

  std::vector<std::string> keyList;
  keyList.push_back(key);
  serialKey = serializeList(keyList);

  statMemc = 
      memcached_append(this->conn_,
                       listKey.data(),
                       listKey.length(),
                       serialKey.data(),
                       serialKey.length(),
                       this->memcachedExpirationLimit_, 
                       (uint32_t)0);

	if (statMemc != MEMCACHED_SUCCESS &&
			statMemc != MEMCACHED_NOTSTORED)
	{
		throw DmException(DM_UNKNOWN_ERROR,
											std::string(memcached_strerror(this->conn_,
																										 statMemc)));
	}
}

void MemcacheCatalog::removeListItemFromMemcachedKey(const std::string& listKey, std::string& key)
{
	memcached_return statMemc;
  uint64_t version;
  std::string serialKey;

  std::vector<std::string> keyList;
  keyList.push_back(key);
  serialKey = serializeList(keyList, false);

  statMemc = 
      memcached_append(this->conn_,
                       listKey.data(),
                       listKey.length(),
                       serialKey.data(),
                       serialKey.length(),
                       this->memcachedExpirationLimit_, 
                       (uint32_t)0);

	if (statMemc != MEMCACHED_SUCCESS &&
			statMemc != MEMCACHED_NOTSTORED)
	{
		throw DmException(DM_UNKNOWN_ERROR,
											std::string(memcached_strerror(this->conn_,
																										 statMemc)));
	}
}
