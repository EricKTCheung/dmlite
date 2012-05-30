/// @file    plugins/memcache/Memcache.h
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
#include <algorithm>
#include <cstring>
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
  PRE_COMMENT,
  PRE_DIR
};

/// Used internally to define Key Prefixes.
/// Must match with PRE_* constants!
static const char* key_prefix[] = {
	"STAT",
  "REPL",
  "RPLI",
  "RPBL",
  "LINK",
  "CMNT",
  "DIR"
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
																 time_t memcachedExpirationLimit,
                                 bool memcachedStrict,
                                 bool memcachedPOSIX,
                                 bool updateATime)
  throw (DmException):
   DummyCatalog(decorates),
	 symLinkLimit_(symLinkLimit),
	 memcachedExpirationLimit_(memcachedExpirationLimit),
   memcachedStrict_(memcachedStrict),
   memcachedPOSIX_(memcachedPOSIX),
   updateATime_(updateATime)
{
  this->connectionPool_ = connPool;
  this->conn_           = connPool->acquire();
}



MemcacheCatalog::~MemcacheCatalog() throw (DmException)
{
  this->connectionPool_->release(this->conn_);
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

void MemcacheCatalog::deserialize(const std::string& serial_str, ExtendedStat& var)
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

void MemcacheCatalog::deserializeLink(const std::string& serial_str,
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

std::string MemcacheCatalog::serializeList(std::vector<std::string>& keyList, const bool isWhite, const bool isComplete)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  SerialKey* pntKey;
  SerialKeyList list;
  std::string serialList;
  std::vector<std::string>::iterator itKeyList;

  for (itKeyList = keyList.begin();
       itKeyList != keyList.end();
       itKeyList++) {
    pntKey = list.add_key(); 
    pntKey->set_key(*itKeyList);
    pntKey->set_white(isWhite);
  }

  list.set_iscomplete(isComplete);

  serialList = list.SerializeAsString();
//  list.PrintDebugString();

  return serialList;
}

std::string MemcacheCatalog::serializeDirList(std::vector<std::string>& keyList, const time_t mtime, const bool isWhite, const bool isComplete)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  SerialKey* pntKey;
  SerialKeyList list;
  std::string serialList;
  std::vector<std::string>::iterator itKeyList;

  for (itKeyList = keyList.begin();
       itKeyList != keyList.end();
       itKeyList++) {
    pntKey = list.add_key(); 
    pntKey->set_key(*itKeyList);
    pntKey->set_white(isWhite);
  }

  list.set_iscomplete(isComplete);
  list.set_mtime(mtime);

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

int MemcacheCatalog::deserializeList(std::string& serialList,
                                     std::vector<std::string>& keyList)
{
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
  if (list.iscomplete())
    return DIR_CACHED;
  else
    return DIR_NOTCOMPLETE;
}

int MemcacheCatalog::deserializeDirList(std::string& serialList,
                                      std::vector<std::string>& keyList,
                                      time_t& mtime)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  SerialKey key;
  SerialKeyList list;
  list.ParseFromString(serialList);

  for (int i = 0; i < list.key_size(); i++) 
  {
    key = list.key(i);
    keyList.push_back(key.key());
  }
  
  mtime = list.mtime();

//  list.PrintDebugString();
  if (list.iscomplete())
    return DIR_CACHED;
  else
    return DIR_NOTCOMPLETE;
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


SecurityContext* MemcacheCatalog::createSecurityContext(const SecurityCredentials& cred) throw (DmException)
{
  DELEGATE_RETURN(createSecurityContext, cred);
}



void MemcacheCatalog::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  DELEGATE(setSecurityContext, ctx);
  this->secCtx_ = *ctx;
}

/*
ExtendedStat MemcacheCatalog::extendedStat(const std::string& path, bool followSym) throw (DmException)
{
  if (this->memcachedPOSIX_) {
    return this->extendedStatPOSIX(path, followSym);
  } else {
    return this->extendedStatDirect(path, followSym);
  }
}

ExtendedStat MemcacheCatalog::extendedStatDirect(const std::string& path, bool followSym) throw (DmException)
{
  // add cwd to relative path

  // get memcache key

  // get enrty from memcached

  // if not found, delegate

  return meta;
}

ExtendedStat MemcacheCatalog::extendedStatPOSIX(const std::string& path, bool followSym) throw (DmException)
{
*/
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
    if (checkPermissions(&this->secCtx_, meta.acl, meta.stat, S_IEXEC) != 0)
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

ExtendedStat MemcacheCatalog::extendedStat(ino_t fileId) throw (DmException)
{
  ExtendedStat meta; 

  memset(&meta, 0x00, sizeof(ExtendedStat));

	std::string valMemc;

	const std::string key = keyFromAny(key_prefix[PRE_STAT], fileId); 

	valMemc = safeGetValFromMemcachedKey(key);
	if (!valMemc.empty()) {
		deserialize(valMemc, meta);
	} else // valMemc was not in memcached
	{
		DELEGATE_ASSIGN(meta, extendedStat, fileId);
		valMemc = serialize(meta);
		safeSetMemcachedFromKeyValue(key, valMemc);
	}

  return meta;
}

ExtendedStat MemcacheCatalog::extendedStat(ino_t parent, const std::string& name) throw (DmException)
{
  ExtendedStat meta;

  memset(&meta, 0x00, sizeof(ExtendedStat));

	std::string valMemc;

	const std::string key = keyFromAny(key_prefix[PRE_STAT], parent, name);

	valMemc = safeGetValFromMemcachedKey(key);
	if (!valMemc.empty()) {
		deserialize(valMemc, meta);
	} else // valMemc was not in memcached
	{
		DELEGATE_ASSIGN(meta, extendedStat, parent, name);
		valMemc = serialize(meta);
		safeSetMemcachedFromKeyValue(key, valMemc);
	}

  return meta;
}

SymLink MemcacheCatalog::readLink(ino_t linkId) throw(DmException)
{
  SymLink meta; 

  memset(&meta, 0x00, sizeof(SymLink));

	std::string valMemc;

	const std::string key = keyFromAny(key_prefix[PRE_LINK], linkId); 

	valMemc = safeGetValFromMemcachedKey(key);
	if (!valMemc.empty()) {
		deserializeLink(valMemc, meta);
	} else // valMemc was not in memcached
	{
		DELEGATE_ASSIGN(meta, readLink, linkId);
		valMemc = serializeLink(meta);
		safeSetMemcachedFromKeyValue(key, valMemc);
	}

  return meta;
}

void MemcacheCatalog::removeDir(const std::string& path) throw (DmException)
{
	delMemcachedFromPath(path);
	DELEGATE(removeDir, path);
	delMemcachedFromPath(getParent(path));
}

void MemcacheCatalog::symlink(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
	DELEGATE(symlink, oldPath, newPath);
	delMemcachedFromPath(getParent(oldPath));
	delMemcachedFromPath(getParent(newPath));
}

void MemcacheCatalog::unlink(const std::string& path) throw (DmException)
{
 	delMemcachedFromPath(path);
	DELEGATE(unlink, path);
 	delMemcachedFromPath(getParent(path));  
}

void MemcacheCatalog::rename(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
	DELEGATE(rename, oldPath, newPath);
	delMemcachedFromPath(oldPath);
	delMemcachedFromPath(getParent(oldPath));
	delMemcachedFromPath(getParent(newPath));
}

void MemcacheCatalog::makeDir(const std::string& path, mode_t mode) throw (DmException)
{
	DELEGATE(makeDir, path, mode);
	delMemcachedFromPath(getParent(path));
}

void MemcacheCatalog::create(const std::string& path, mode_t mode) throw (DmException)
{
	DELEGATE(create, path, mode);
	delMemcachedFromPath(getParent(path));
}

Directory* MemcacheCatalog::openDir(const std::string& path) throw(DmException)
{
  Directory    *dir, *remote_dir; 
  MemcacheDir  *local_dir;
  ExtendedStat meta;
  std::string  valMemc;
  std::vector<std::string> keyList;
  time_t       mtime; 

  if (this->memcachedStrict_) {
    // Get the dir stat from a delegate to compare the utimes 
    DELEGATE_ASSIGN(meta, extendedStat, path);
  } else {
    // Get the directory
    meta = this->extendedStat(path);
  }

  // Can we read it?
  if (checkPermissions(&this->secCtx_, meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to read " + path);

  // Create new accessed time for the directory 
  struct utimbuf tim;
  tim.actime  = time(NULL);
  tim.modtime = meta.stat.st_mtime;
  // Touch
  this->utime(meta.stat.st_ino, &tim);


  // Create the handle
  local_dir = new MemcacheDir();
  local_dir->dirId = meta.stat.st_ino;
  local_dir->curKeysSegment = 0;
  local_dir->keysPntr = 0;
  local_dir->fetchCombined = FETCH_COMBINED_MIN;
  
  const std::string listKey = keyFromAny(key_prefix[PRE_DIR], 
                                         local_dir->dirId);

  valMemc = safeGetDListValFromMemcachedKey(listKey);

  if (!valMemc.empty()) {
    local_dir->isCached = deserializeDirList(valMemc, keyList, mtime);
    // Check if the directory has been modified after being cached
    if (mtime < meta.stat.st_mtime) {
      delMemcachedFromKey(listKey);
      local_dir->isCached = DIR_NOTCACHED;
    }
    if (local_dir->isCached == DIR_CACHED) { 
      local_dir->keys = std::list<std::string>(keyList.begin(), keyList.end());
      local_dir->keysOrigSize = local_dir->keys.size();
      local_dir->keysPntr = 0;
    } 
  } else {
    local_dir->isCached = DIR_NOTCACHED;
  }
  if (local_dir->isCached == DIR_NOTCACHED ||
      local_dir->isCached == DIR_NOTCOMPLETE) {
    DELEGATE_ASSIGN(remote_dir, openDir, path);
    local_dir->dirp = remote_dir;
  }

  if (local_dir->isCached == DIR_NOTCACHED)
    local_dir->mtime = tim.modtime;


  return (Directory *) local_dir;
}

void MemcacheCatalog::closeDir(Directory* dir) throw(DmException)
{
  MemcacheDir *dirp;
  dirp = (MemcacheDir *) dir;

  if (dirp->isCached == DIR_NOTCACHED ||
      dirp->isCached == DIR_NOTCOMPLETE) {
    DELEGATE(closeDir, dirp->dirp);
  }
  
  delete dirp;
}

struct dirent* MemcacheCatalog::readDir(Directory* dir) throw(DmException)
{
  if (this->readDirx(dir) == 0)
    return 0x00;
  else
    return &(((MemcacheDir*)dir)->ds);
}



ExtendedStat* MemcacheCatalog::readDirx(Directory* dir) throw(DmException)
{
  MemcacheDir *dirp;
  ExtendedStat *meta;

  if (dir == 0x00)
    throw DmException(DM_NULL_POINTER, "Tried to read a null dir");

  dirp = (MemcacheDir*)dir;

  if (dirp->isCached == DIR_NOTCACHED && dirp->keysPntr == 0) {
    // just set it to nonzero
    dirp->keysPntr++;
    std::string valMemc;
    const std::string listKey = keyFromAny(key_prefix[PRE_DIR], 
                                           dirp->dirId);
    // create an empty list with 'white' elements, which is not complete
    std::vector<std::string> empty;
    valMemc = serializeDirList(empty, dirp->mtime, true, false);
    try 
    {
      addMemcachedDListFromKeyValue(listKey, valMemc);
    } catch (...) {
      dirp->isCached = DIR_NOTCOMPLETE;
    }
  }

  switch (dirp->isCached) {
    case DIR_NOTCACHED:
      meta = this->fetchExtendedStatFromDelegate(dirp, true);
      break;
    case DIR_NOTCOMPLETE:
      meta = this->fetchExtendedStatFromDelegate(dirp, false);    
      break;
    case DIR_CACHED:
      meta = this->fetchExtendedStatFromMemcached(dirp);
  }
  // if an entry could be fetched ...
  if (meta != 0x00) {
      // copy the stat info into the dirent and touch 
      memset(&dirp->ds, 0x00, sizeof(struct dirent));
      dirp->ds.d_ino  = meta->stat.st_ino;
      strncpy(dirp->ds.d_name,
              meta->name,
              sizeof(dirp->ds.d_name));

    if (dirp->isCached == DIR_CACHED) {
      if (this->updateATime_) {
        // Touch
        struct utimbuf tim;
        tim.actime  = time(NULL);
        tim.modtime = dirp->current.stat.st_mtime;
        this->utime(dirp->dirId, &tim);
      }
    }

//    ENCODE_DIRCACHED(dirp, isCached);

    return meta;
  }
  else {
    if (dirp->isCached == DIR_NOTCACHED) {
      // mark the file list in Memcached as complete
      // only if it's notcached == saveToMemc is true
      std::string valMemc;
      const std::string listKey = keyFromAny(key_prefix[PRE_DIR], 
                                             dirp->dirId);
  
      // append an empty list with 'white' elements, which is complete
      // to mark the list as complete
      dirp->curKeysSegment = safeAddToDListFromMemcachedKey(listKey, 
                                                        std::string(),
                                                        true, true,
                                                        dirp->curKeysSegment);
    }
    
    return 0x00;
  }
}

void MemcacheCatalog::changeMode(const std::string& path, mode_t mode)
   throw (DmException)
{
	DELEGATE(changeMode, path, mode);
	delMemcachedFromPath(path, KEEP_DIRLIST);
}

void MemcacheCatalog::changeOwner(const std::string& path,
                                  uid_t newUid, gid_t newGid)
  throw (DmException)
{
	DELEGATE(changeOwner, path, newUid, newGid);
	delMemcachedFromPath(path, KEEP_DIRLIST);
}

void MemcacheCatalog::linkChangeOwner(const std::string& path,
                                      uid_t newUid, gid_t newGid)
  throw (DmException)
{
	DELEGATE(linkChangeOwner, path, newUid, newGid);
	delMemcachedFromPath(path, KEEP_DIRLIST);
}

void MemcacheCatalog::setAcl(const std::string& path, const std::vector<Acl>& acls) throw (DmException)
{
  DELEGATE(setAcl, path, acls);
	delMemcachedFromPath(path, KEEP_DIRLIST);
}

void MemcacheCatalog::utime(const std::string& path, const struct utimbuf* buf) throw (DmException)
{
  DELEGATE(utime, path, buf);
  if (this->memcachedStrict_) {
    delMemcachedFromPath(path, KEEP_DIRLIST);
  }
}

void MemcacheCatalog::utime(ino_t inode, const struct utimbuf* buf) throw (DmException)
{
  DELEGATE(utime, inode, buf);
  if (this->memcachedStrict_) {
    delMemcachedFromInode(inode, KEEP_DIRLIST);
  }
}

std::string MemcacheCatalog::getComment(const std::string& path) throw (DmException)
{
  // Get the file and check we can read
  ExtendedStat meta = this->extendedStat(path);
  
  if (checkPermissions(&this->secCtx_, meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to read " + path);

  // Query
  std::string comment;
	std::string valMemc;

	const std::string key = keyFromAny(key_prefix[PRE_COMMENT], meta.stat.st_ino); 

	valMemc = safeGetValFromMemcachedKey(key);
	if (!valMemc.empty()) {
		deserializeComment(valMemc, comment);
	} else // valMemc was not in memcached
	{
		DELEGATE_ASSIGN(comment, getComment, path);
		valMemc = serializeComment(comment);
		safeSetMemcachedFromKeyValue(key, valMemc);
	}
  return comment;
}

void MemcacheCatalog::setComment(const std::string& path, const std::string& comment) throw (DmException)
{
  DELEGATE(setComment, path, comment);
  ExtendedStat meta = this->extendedStat(path);
  const std::string key = keyFromAny(key_prefix[PRE_COMMENT],
                                     meta.stat.st_ino);
	delMemcachedFromKey(key);
}

void MemcacheCatalog::setGuid(const std::string& path, const std::string& guid) throw (DmException)
{
  DELEGATE(setGuid, path, guid);
	delMemcachedFromPath(path, KEEP_DIRLIST);
}


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
  safeAddToListFromMemcachedKey(listKey, key);
}

void MemcacheCatalog::deleteReplica(const std::string& guid, int64_t id,
                                   const std::string& sfn) throw (DmException)
{
  DELEGATE(deleteReplica, guid, id, sfn);
  std::string key = keyFromURI(key_prefix[PRE_REPL], sfn);
  std::string listKey = keyFromAny(key_prefix[PRE_REPL_LIST], id);
  delMemcachedFromKey(key);
  safeRemoveListItemFromMemcachedKey(listKey, key);
}

std::vector<FileReplica> MemcacheCatalog::getReplicas(const std::string& path) throw(DmException)
{
  // Function copied from NsMySql.cpp
  ExtendedStat  meta;

  // Need to grab the file first
  meta = this->extendedStat(path, true);

  // The file exists, plus we have permissions to go there. Check we can read
  if (checkPermissions(&this->secCtx_,
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

  vecValMemc = safeGetListFromMemcachedKey(listKey);

	if (vecValMemc.size() > 0) {
    std::vector<std::string>::iterator itRepl;
    for (itRepl = vecValMemc.begin();
         itRepl != vecValMemc.end();
         itRepl++) { 
      repl = deserializeFileReplica(*itRepl);
      replicas.push_back(repl);
    }
//    replicas = deserialize(vecValMemc);
  } else {
    // otherwise, get replicas from mysql
    DELEGATE_ASSIGN(replicas, getReplicas, path);

    // save replicas in memcached
    safeSetMemcachedFromReplicas(replicas, inode);
  } 
  
  return replicas;
}

Uri MemcacheCatalog::get(const std::string& path) throw(DmException)
{
  std::vector<std::string> vecValMemc;
  FileReplica repl;
  ExtendedStat  meta;
  ino_t inode;

  // Need to grab the file first
  meta = this->extendedStat(path, true);
  inode = meta.stat.st_ino;

  // The file exists, plus we have permissions to go there. Check we can read
  if (checkPermissions(&this->secCtx_,
                       meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN,
                   "Not enough permissions to read " + path);


  // get replica list from memcached
  const std::string listKey = keyFromAny(key_prefix[PRE_REPL_LIST],
                                         inode);

  vecValMemc = safeGetListFromMemcachedKey(listKey);

	if (vecValMemc.size() > 0) {
    // Pick a random one
    int i = rand() % vecValMemc.size();

    repl = deserializeFileReplica(vecValMemc[i]);
  } else {
    std::vector<FileReplica> replicas;

    // otherwise, get replicas from mysql
    DELEGATE_ASSIGN(replicas, getReplicas, path);

    // save replicas in memcached
    safeSetMemcachedFromReplicas(replicas, inode);

    // Pick a random one
    int i = rand() % replicas.size();

    repl = replicas[i];
  } 

  // Copy
  return dmlite::splitUri(repl.url);
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
																							ino_t parent,
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
	
  if (path[0] == '/' || cwdPath.empty()) {
	  key_path = path;
	} else {
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

const std::string MemcacheCatalog::getValFromMemcachedKey(const std::string key)  throw (MemcacheException)
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
			statMemc != MEMCACHED_NOTFOUND) {
  	throw MemcacheException(statMemc, this->conn_);
	}

	if (lenValue > 0) {
   	valMemcStr.assign(valMemc, lenValue);
	}
	return valMemcStr;
}

const std::string MemcacheCatalog::safeGetValFromMemcachedKey(const std::string key) 
{
  try {
    return getValFromMemcachedKey(key);
  } catch (MemcacheException) { /* pass */ }
}

const std::string MemcacheCatalog::getValFromMemcachedVersionedKey(const std::string key) throw (MemcacheException)
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
	if (statMemc != MEMCACHED_SUCCESS) {
    if (statMemc != MEMCACHED_NOTFOUND) {
  	  throw MemcacheException(statMemc, this->conn_);
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
		  	statMemc != MEMCACHED_NOTFOUND) {
  	  throw MemcacheException(statMemc, this->conn_);
	  }

	  if (lenValue > 0) {
		  valMemcStr.assign(valMemc, lenValue);
	  }
  }
	return valMemcStr;
}


const std::string MemcacheCatalog::safeGetValFromMemcachedVersionedKey(const std::string key) 
{
  try {
    return getValFromMemcachedVersionedKey(key);
  } catch (MemcacheException) { /* pass */ }
}

const std::string MemcacheCatalog::getDListValFromMemcachedKey(const std::string key) throw (MemcacheException)
{
	memcached_return statMemc;
	size_t lenValue;
	uint32_t flags;
	char* valMemc;
  int noSegments;
  std::string segmentKey;
	std::string valMemcStr;
	std::string valMemcSegmentStr;

	valMemc = memcached_get(this->conn_,
													 key.data(),
													 key.length(),
													 &lenValue,
													 &flags,
													 &statMemc);

	if (statMemc != MEMCACHED_SUCCESS &&
			statMemc != MEMCACHED_NOTFOUND) {
  	throw MemcacheException(statMemc, this->conn_);
	}

  if (statMemc == MEMCACHED_SUCCESS) {
    std::vector<std::string> keyList;
    std::vector<std::string> segmentList;
    std::vector<std::string>::iterator itSegmentList;
    noSegments = this->atoi(valMemc, lenValue);
  
    for (int i = 0; i < noSegments; i++) {
      segmentKey = key + ":" + this->toString(i);
      keyList.push_back(segmentKey);
    }
    segmentList = getValListFromMultipleMemcachedKeys(keyList);
    for (itSegmentList = segmentList.begin();
         itSegmentList != segmentList.end();
         itSegmentList++) {
      valMemcStr.append(*itSegmentList);
    }    
  }
	return valMemcStr;
}

const std::string MemcacheCatalog::safeGetDListValFromMemcachedKey(const std::string key) throw (MemcacheException)
{
  try {
    return getDListValFromMemcachedKey(key);
  } catch (MemcacheException) { /* pass */ }
}

int MemcacheCatalog::atoi(char *text, size_t length)
{
  std::string num(text, length);
  return std::atoi(num.c_str());
}

std::string MemcacheCatalog::toString(int i)
{
  std::stringstream ss;
  ss << i;
  return ss.str();
}

ino_t MemcacheCatalog::getInodeFromStatKey(const std::string key)
{
  ino_t inode = (ino_t) std::atoi(key.substr(5).c_str());
  return inode;
}

std::vector<std::string> MemcacheCatalog::getValListFromMemcachedKeyList(
                            const std::vector<std::string>& keyList)
{
  memcached_return statMemc;
  std::vector<std::string> valList;

  prepareMemcachedMultiGetFromKeyList(keyList);

  valList = doMemcachedMultiGet(keyList.size(), true); //use placeholders

  return valList;
}

std::vector<ExtendedStat>
              MemcacheCatalog::getExtendedStatListFromMemcachedKeyList(
                            const std::vector<std::string>& keyList) throw (DmException)
{
  std::vector<std::string> valList;
  std::vector<ExtendedStat> xstatList;
  ExtendedStat xstat;
  std::string valMemc;
  ino_t inode;
  valList = getValListFromMemcachedKeyList(keyList);
  //printf("get:sizeof keyList = %d.\n", keyList.size());
  //printf("get:sizeof valList = %d.\n", valList.size());

  std::vector<std::string>::const_iterator itKeys;
  std::vector<std::string>::const_iterator itVals;
  for (itKeys = keyList.begin(), itVals = valList.begin();
       itKeys != keyList.end(), itVals != valList.end();
       itKeys++, itVals++) { 
    if (!(*itVals).empty()) {
      //printf("getting the xstat from memcached\n");
      deserialize(*itVals, xstat);
    } else {
      //printf("getting the xstat from the db\n");
      inode = getInodeFromStatKey(*itKeys);
      //printf("key = %s, inode = %d.\n", (*itKeys).c_str(), inode);
      DELEGATE_ASSIGN(xstat, extendedStat, inode);
      srand(time(NULL));
      int r = rand() % 10;
      if (r < PROB_CACHE * 10) {
        valMemc = serialize(xstat);
        setMemcachedFromKeyValue(*itKeys, valMemc);
      }
    }
    xstatList.push_back(xstat);
  }

  return xstatList;
}

void MemcacheCatalog::prepareMemcachedMultiGetFromKeyList(
                            const std::vector<std::string>& keyList) throw (MemcacheException)
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
  unsigned int i;
  for (i = 0; i < keyList.size(); i++) {
    key_length[i] = keyList[i].size();
    keys_size += keyList[i].size();
    pntKey = (char*) malloc(keyList[i].size());
    std::memset(pntKey, 0x00, keyList[i].size());
    std::memcpy(pntKey, keyList[i].data(), keyList[i].size());
    keys.push_back(pntKey);
  }
  //printf("prepare:sizeof keys = %d.\n", keys.size());

  // mget results 
  statMemc = memcached_mget(this->conn_,
                            &keys[0],
                            key_length,
                            keyList.size());

	if (statMemc != MEMCACHED_SUCCESS) {
  	throw MemcacheException(statMemc, this->conn_);
	}

  for (i = 0; i < keyList.size(); i++) {
    free(keys[i]);
  }
}

std::vector<std::string>
          MemcacheCatalog::doMemcachedMultiGet(int keyListSize, const bool usePlaceholder) throw (MemcacheException)
{
  char return_key[MEMCACHED_MAX_KEY];
  size_t return_key_length;
  size_t lenValue;
	uint32_t flags;
  memcached_return statMemc;
	char* valMemc;
  std::vector<std::string> vecValMemc;
	std::string valMemcStr;

  while ((valMemc = memcached_fetch(this->conn_,
                                   return_key,
                                   &return_key_length,
                                   &lenValue,
                                   &flags,
                                   &statMemc))) {

    //printf("getting one val.\n");
	  if (statMemc == MEMCACHED_SUCCESS) {
      //printf("got stat in list from memcached.\n");
		  valMemcStr.assign(valMemc, lenValue);
      vecValMemc.push_back(valMemcStr);
      free(valMemc);
    } else if (statMemc == MEMCACHED_NOTFOUND && usePlaceholder) {
      //printf("used placeholder.\n");
      vecValMemc.push_back(std::string());
    } else {
  	  throw MemcacheException(statMemc, this->conn_);
    }
  }
  //printf("%s\n", memcached_strerror(this->conn_, statMemc));
  //printf("sizeof vecValMemc = %d.\n", vecValMemc.size());

  // invalidate result if not all keys are found
  if (vecValMemc.size() < keyListSize)
    if (usePlaceholder) {
      while (vecValMemc.size() < keyListSize) {
        //printf("used placeholder (late).\n");
        vecValMemc.push_back(std::string());
      }
    } else {
      vecValMemc.resize(0);  
    }

  //return vector
  return vecValMemc;
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
  unsigned int i;
  for (i = 0; i < keyList.size(); i++) {
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

	if (statMemc != MEMCACHED_SUCCESS) {
  	throw MemcacheException(statMemc, this->conn_);
	}

  for (i = 0; i < keyList.size(); i++) {
    free(keys[i]);
  }
 
  // actually fetch results in std::vector

  char return_key[MEMCACHED_MAX_KEY];
  size_t return_key_length;
  size_t lenValue;
	uint32_t flags;
	std::string valMemcStr;
  std::set<std::string> foundKeys;

  while ((valMemc = memcached_fetch(this->conn_,
                                   return_key,
                                   &return_key_length,
                                   &lenValue,
                                   &flags,
                                   &statMemc))) {
	  if (statMemc != MEMCACHED_SUCCESS) {
  	  throw MemcacheException(statMemc, this->conn_);
	  } else {
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

ExtendedStat* MemcacheCatalog::fetchExtendedStatFromMemcached(MemcacheDir *dirp)
                            throw (DmException)
{
  if (dirp->keysPntr >= dirp->keysOrigSize)
    return 0x00;

  ExtendedStat meta;
  ExtendedStat *return_meta;
  std::vector<ExtendedStat> vecXstat;
  std::vector<std::string> keyList;
  std::vector<std::string>::iterator itKeySliceBegin;
  std::vector<std::string>::iterator itKeySliceEnd;
  
  //printf("sizeof xstats = %d.\n", dirp->xstats.size());
  if (dirp->xstats.empty()) {
    //printf("xstats list is empty\n");
    // fetch from cache/db
/*
    itKeySliceBegin = dirp->keys.begin() + dirp->keysPntr;
    itKeySliceEnd = itKeySliceBegin + FETCH_COMBINED;
    if (itKeySliceEnd > dirp->keys.end()) {
      itKeySliceEnd = dirp->keys.end();
    }
    keyList.assign(itKeySliceBegin, itKeySliceEnd);
*/
    for (int i = 0; i < dirp->fetchCombined; i++) {
      if (dirp->keys.empty())
        break;
      keyList.push_back(dirp->keys.front());
      dirp->keys.pop_front();
    }
    if (dirp->fetchCombined < FETCH_COMBINED_MAX) {
      dirp->fetchCombined *= FETCH_COMBINED_MUL;
      if (dirp->fetchCombined > FETCH_COMBINED_MAX)
        dirp->fetchCombined = FETCH_COMBINED_MAX;
    }
    //printf("sizeof keys = %d.\n", dirp->keys.size());
    //printf("sizeof keyList = %d.\n", keyList.size());

    vecXstat = getExtendedStatListFromMemcachedKeyList(keyList);
    //printf("sizeof vecXstat = %d.\n", vecXstat.size());
    dirp->xstats.assign(vecXstat.begin(), vecXstat.end());
  }
  // serve from local list
  meta = dirp->xstats.front();
  dirp->xstats.pop_front();
  dirp->keysPntr++;


  // copy the extendedStat into dir->current
  std::memcpy(&(dirp->current), &meta, sizeof(ExtendedStat));
  return_meta = &(dirp->current);

	return return_meta;
}

ExtendedStat* MemcacheCatalog::fetchExtendedStatFromDelegate(MemcacheDir *dirp, const bool saveToMemc)
                            throw (DmException)
{
  std::string valMemcStr;
  ExtendedStat *metap;
  std::string key, listKey;
  ExtendedStat meta;
  std::string valMemc;
	memcached_return statMemc;

  DELEGATE_ASSIGN(metap, readDirx, (Directory *) dirp->dirp);


  if (saveToMemc) {
    listKey = keyFromAny(key_prefix[PRE_DIR], dirp->dirId);
    if (dirp->keys.size() > FETCH_COMBINED || metap == 0x00) {

      statMemc = memcached_behavior_set(this->conn_, MEMCACHED_BEHAVIOR_NOREPLY, 1);
      
      dirp->curKeysSegment = addToDListFromMemcachedKeyListNoReply(listKey,
         std::vector<std::string>(dirp->keys.begin(), dirp->keys.end()),
                                          true, false,
                                          dirp->curKeysSegment);

      // Set the no reply behavior, don't care if it fails
      statMemc = memcached_behavior_set(this->conn_, MEMCACHED_BEHAVIOR_NOREPLY, 1);


      while (!dirp->keys.empty()) {
        valMemc = serialize(dirp->xstats.front());
        setMemcachedFromKeyValue(dirp->keys.front(), valMemc);
        dirp->keys.pop_front();
        dirp->xstats.pop_front();
      }
      // unset no reply. check for failure, otherwise deletes hang
      statMemc = memcached_behavior_set(this->conn_, MEMCACHED_BEHAVIOR_NOREPLY, 0);
    	if (statMemc != MEMCACHED_SUCCESS)
		    throw MemcacheException(statMemc, this->conn_);
  
    }
    if (metap != 0x00) {
      key = keyFromAny(key_prefix[PRE_STAT], metap->stat.st_ino);
      dirp->keys.push_back(key);
      dirp->xstats.push_back(*metap);
    }
  }

  // copy the extendedStat into dir->current
//  std::memcpy(&(dirp->current), metap, sizeof(ExtendedStat));

  return metap;
}

void MemcacheCatalog::setMemcachedFromKeyValue(const std::string key,
							  						  const std::string value) throw (MemcacheException)
{
	memcached_return statMemc;
	statMemc = memcached_set(this->conn_,
													 key.data(),
													 key.length(),
													 value.data(), value.length(),
													 this->memcachedExpirationLimit_,
  												 (uint32_t)0);

	if (statMemc != MEMCACHED_SUCCESS) {
  	throw MemcacheException(statMemc, this->conn_);
	}

	return;
}

void MemcacheCatalog::safeSetMemcachedFromKeyValue(const std::string key,
							  						  const std::string value)
{
  try {
    return setMemcachedFromKeyValue(key, value);
  } catch (MemcacheException) { /* pass */ }
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

  if (statMemc == MEMCACHED_NOTFOUND) {
    std::string strVersion("1");
    version = 1;
	  statMemc = memcached_set(this->conn_,
		  											 key.data(),
			  										 key.length(),
				  									 strVersion.data(), strVersion.length(),
					  								 this->memcachedExpirationLimit_,
    												 (uint32_t)0);
  }
  
	if (statMemc != MEMCACHED_SUCCESS) {
  	throw MemcacheException(statMemc, this->conn_);
	}

  versionedKey = versionedKeyFromAny(version, key);

	statMemc = memcached_set(this->conn_,
													 versionedKey.data(),
													 versionedKey.length(),
													 strValue.data(), strValue.length(),
													 this->memcachedExpirationLimit_,
  												 (uint32_t)0);

	if (statMemc != MEMCACHED_SUCCESS) {
  	throw MemcacheException(statMemc, this->conn_);
	}

	return;
}

void MemcacheCatalog::safeSetMemcachedFromVersionedKeyValue(const std::string key,
							  						  const std::string value)
{
  try {
    return setMemcachedFromVersionedKeyValue(key, value);
  } catch (MemcacheException) { /* pass */ }
}

int MemcacheCatalog::setMemcachedDListFromKeyValue(const std::string key,
							  						  const std::string value)
{
	memcached_return statMemc;

  std::string strNoSegments("1");
  statMemc = memcached_set(this->conn_,
													 key.data(),
													 key.length(),
				  								 strNoSegments.data(),
                           strNoSegments.length(),
					  							 this->memcachedExpirationLimit_,
    											 (uint32_t)0);

	if (statMemc != MEMCACHED_SUCCESS &&
      statMemc != MEMCACHED_NOTSTORED) {
  	throw MemcacheException(statMemc, this->conn_);
	}
  if (statMemc == MEMCACHED_SUCCESS) {
    const std::string segmentKey = key + ":" + "0";

	  statMemc = memcached_set(this->conn_,
		  											 segmentKey.data(),
			  										 segmentKey.length(),
				  									 value.data(), value.length(),
					  								 this->memcachedExpirationLimit_,
  					  							 (uint32_t)0);

  	if (statMemc != MEMCACHED_SUCCESS) {
  	  throw MemcacheException(statMemc, this->conn_);
    }
	}

	return statMemc;
}

int MemcacheCatalog::addMemcachedDListFromKeyValue(const std::string key,
							  						  const std::string value)
{
	memcached_return statMemc;

  std::string strNoSegments("1");
  statMemc = memcached_add(this->conn_,
													 key.data(),
													 key.length(),
				  								 strNoSegments.data(),
                           strNoSegments.length(),
					  							 this->memcachedExpirationLimit_,
    											 (uint32_t)0);

	if (statMemc != MEMCACHED_SUCCESS) {
  	throw MemcacheException(statMemc, this->conn_);
	}
  if (statMemc == MEMCACHED_SUCCESS) {
    const std::string segmentKey = key + ":" + "0";

	  statMemc = memcached_set(this->conn_,
		  											 segmentKey.data(),
			  										 segmentKey.length(),
				  									 value.data(), value.length(),
					  								 this->memcachedExpirationLimit_,
  					  							 (uint32_t)0);

  	if (statMemc != MEMCACHED_SUCCESS) {
  	  throw MemcacheException(statMemc, this->conn_);
    }
	}

	return statMemc;
}


int MemcacheCatalog::safeSetMemcachedDListFromKeyValue(const std::string key,
							  						  const std::string value)
{
  try {
    return setMemcachedDListFromKeyValue(key, value);
  } catch (MemcacheException) { /* pass */ }
}

void MemcacheCatalog::setMemcachedFromReplicas(std::vector<FileReplica>& replicas, ino_t inode)
{
  std::string serialList;
  std::string serialReplica;
  std::vector<std::string> keys;

  for (int i = 0; i < replicas.size(); i++) {
    serialReplica =  serializeFileReplica(replicas[i]);
    keys.push_back(keyFromURI(key_prefix[PRE_REPL], replicas[i].url));

    setMemcachedFromKeyValue(keys.back(), serialReplica);
  }
  serialList = serializeList(keys);
  setMemcachedFromKeyValue(keyFromAny(key_prefix[PRE_REPL_LIST], inode),
                           serialList);
}

void MemcacheCatalog::safeSetMemcachedFromReplicas(std::vector<FileReplica>& replicas, ino_t inode)
{
  try {
    return setMemcachedFromReplicas(replicas, inode);
  } catch (MemcacheException) { /* pass */ }
}

void MemcacheCatalog::delMemcachedFromKey(const std::string key)
{
	memcached_return statMemc;
	statMemc = memcached_delete(this->conn_,
													 		key.data(),
														  key.length(),
													 		(time_t)0);

	if (statMemc != MEMCACHED_SUCCESS &&
			statMemc != MEMCACHED_NOTFOUND) {
  	throw MemcacheException(statMemc, this->conn_);
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
			statMemc != MEMCACHED_NOTFOUND) {
  	throw MemcacheException(statMemc, this->conn_);
	}
}

void MemcacheCatalog::delMemcachedFromDListKey(const std::string key)
{
	memcached_return statMemc;
	size_t lenValue;
	uint32_t flags;
	char* valMemc;
  int noSegments; 
  std::string segmentKey;
	std::string valMemcStr;

	valMemc = memcached_get(this->conn_,
													 key.data(),
													 key.length(),
													 &lenValue,
													 &flags,
													 &statMemc);

	if (statMemc != MEMCACHED_SUCCESS &&
			statMemc != MEMCACHED_NOTFOUND) {
  	throw MemcacheException(statMemc, this->conn_);
	}

  if (statMemc == MEMCACHED_SUCCESS) {
    noSegments = this->atoi(valMemc, lenValue);
  
    for (int i = 0; i < noSegments; i++) {
      std::string segmentKey = key + ":" + this->toString(i);
      delMemcachedFromKey(segmentKey);
    }
  }
  delMemcachedFromKey(key);
}


void MemcacheCatalog::delMemcachedFromPath(const std::string& path, bool removeDirEntry) throw (MemcacheException)
{
  ExtendedStat meta;

  try
  {  
    meta = this->extendedStat(path);
  } catch (DmException e) {
    int code = e.code();
    if (code != DM_NO_SUCH_FILE)
      throw;
  }

	// delete entry cached with inode
  const std::string key1(keyFromAny(key_prefix[PRE_STAT],
                                    meta.stat.st_ino));
	delMemcachedFromKey(key1);

	// delete entry cached with parent_inode + filename
	const std::string key2(keyFromAny(key_prefix[PRE_STAT],
                                    meta.parent,
                                    meta.name));
	delMemcachedFromKey(key2);

  // delete the directory list of the parent as well
  if (removeDirEntry) {
    const std::string keyDir(keyFromAny(key_prefix[PRE_DIR],
                                        meta.parent));
    delMemcachedFromDListKey(keyDir);
  }
}

void MemcacheCatalog::delMemcachedFromInode(const ino_t inode, bool removeDirEntry) throw (MemcacheException)
{
  ExtendedStat meta;

  try
  {  
    meta = this->extendedStat(inode);
  } catch (DmException e) {
    int code = e.code();
    if (code != DM_NO_SUCH_FILE)
      throw;
  }
	// delete entry cached with inode
  const std::string key1(keyFromAny(key_prefix[PRE_STAT],
                                    meta.stat.st_ino));
	delMemcachedFromKey(key1);

	// delete entry cached with parent_inode + filename
	const std::string key2(keyFromAny(key_prefix[PRE_STAT],
                                    meta.parent,
                                    meta.name));
	delMemcachedFromKey(key2);

  // delete the directory list of the parent as well
  if (removeDirEntry) {
    const std::string keyDir(keyFromAny(key_prefix[PRE_DIR],
                                        meta.parent));
    delMemcachedFromDListKey(keyDir);
  }
}

std::vector<std::string> MemcacheCatalog::getListFromMemcachedKey(const std::string& listKey)
        throw (MemcacheException)
{
  std::string valMemc;
  std::vector<std::string> keyList; 
  std::vector<std::string> vecValMemc; 

  valMemc = getValFromMemcachedKey(listKey);

  if (!valMemc.empty()) {
    keyList = deserializeBlackList(valMemc);

    // get values
    if (keyList.size() > 0)
      vecValMemc = getValListFromMultipleMemcachedKeys(keyList);
  }

  return vecValMemc;
}

std::vector<std::string> MemcacheCatalog::safeGetListFromMemcachedKey(const std::string& listKey)
        throw (MemcacheException)
{
  try {
    return getListFromMemcachedKey(listKey);
  } catch (MemcacheException) { /* pass */ }
}

void MemcacheCatalog::addToListFromMemcachedKey(const std::string& listKey, const std::string& key, const bool isWhite, const bool isComplete)
{
	memcached_return statMemc;
  uint64_t version;
  std::string serialKey;

  std::vector<std::string> keyList;
  if (!key.empty())
    keyList.push_back(key);
  serialKey = serializeList(keyList, isWhite, isComplete);

  statMemc = 
      memcached_append(this->conn_,
                       listKey.data(),
                       listKey.length(),
                       serialKey.data(),
                       serialKey.length(),
                       this->memcachedExpirationLimit_, 
                       (uint32_t)0);

	if (statMemc != MEMCACHED_SUCCESS &&
      // NOTSTORED applies when the key is not present, but also
      // when no space is left to append.
      statMemc != MEMCACHED_NOTSTORED &&
      statMemc != MEMCACHED_PROTOCOL_ERROR) {
  	throw MemcacheException(statMemc, this->conn_);
	}
}

void MemcacheCatalog::safeAddToListFromMemcachedKey(const std::string& listKey, const std::string& key, const bool isWhite, const bool isComplete)
{
  try {
    return addToListFromMemcachedKey(listKey, key, isWhite, isComplete);
  } catch (MemcacheException) { /* pass */ }
}

int MemcacheCatalog::addToDListFromMemcachedKey(const std::string& listKey,
                                                const std::string& key, 
                                                const bool isWhite,
                                                const bool isComplete,
                                                int curSegment) throw (DmException)
{
	memcached_return statMemc;
  uint64_t noSegmentsIncremented;
  std::string segmentKey;
  std::string serialKey;

  std::vector<std::string> keyList;
  if (!key.empty())
    keyList.push_back(key);
  serialKey = serializeList(keyList, isWhite, isComplete);

  segmentKey = listKey + ":" + this->toString(curSegment);

  statMemc = 
      memcached_append(this->conn_,
                       segmentKey.data(),
                       segmentKey.length(),
                       serialKey.data(),
                       serialKey.length(),
                       this->memcachedExpirationLimit_, 
                       (uint32_t)0);

	if (statMemc != MEMCACHED_SUCCESS &&
			statMemc != MEMCACHED_NOTSTORED &&
      statMemc != MEMCACHED_PROTOCOL_ERROR) {
  	throw MemcacheException(statMemc, this->conn_);
	}
  if (statMemc == MEMCACHED_NOTSTORED) {
    curSegment++;
    segmentKey = listKey + ":" + this->toString(curSegment);

    statMemc = 
        memcached_set(this->conn_,
                      segmentKey.data(),
                      segmentKey.length(),
                      serialKey.data(),
                      serialKey.length(),
                      this->memcachedExpirationLimit_, 
                      (uint32_t)0);

    if (statMemc != MEMCACHED_SUCCESS)
      throw MemcacheException(statMemc, this->conn_);

    statMemc = 
        memcached_increment(this->conn_,
                            listKey.data(),
                            listKey.length(),
                            1, // offset
                            &noSegmentsIncremented);

    if (statMemc != MEMCACHED_SUCCESS)
      throw MemcacheException(statMemc, this->conn_);
    if ((noSegmentsIncremented - 1) != curSegment)
      throw DmException(DM_UNKNOWN_ERROR,
          std::string("Incrementing the number of segments on memcached failed."));
  }
  return curSegment;
}

int MemcacheCatalog::safeAddToDListFromMemcachedKey(const std::string& listKey,
                                                const std::string& key, 
                                                const bool isWhite,
                                                const bool isComplete,
                                                int curSegment) throw (DmException)
{
  try {
    return addToDListFromMemcachedKey(listKey, key, isWhite,
                                      isComplete, curSegment);
  } catch (MemcacheException) { /* pass */ }
}

void MemcacheCatalog::addToDListFromMemcachedKey(const std::string& listKey, const std::string& key, const bool isWhite, const bool isComplete) throw (DmException)
{
	memcached_return statMemc;
	size_t lenValue;
	uint32_t flags;
	char* valMemc;
  int noSegments; 
  uint64_t noSegmentsIncremented;
  std::string segmentKey;
  std::string serialKey;

  std::vector<std::string> keyList;
  if (!key.empty())
    keyList.push_back(key);
  serialKey = serializeList(keyList, isWhite, isComplete);

	valMemc = memcached_get(this->conn_,
													 listKey.data(),
													 listKey.length(),
													 &lenValue,
													 &flags,
													 &statMemc);

  if (statMemc == MEMCACHED_SUCCESS) {
    noSegments = this->atoi(valMemc, lenValue);
    segmentKey = listKey + ":" + this->toString(noSegments-1);

    statMemc = 
        memcached_append(this->conn_,
                         segmentKey.data(),
                         segmentKey.length(),
                         serialKey.data(),
                         serialKey.length(),
                         this->memcachedExpirationLimit_, 
                         (uint32_t)0);

  	if (statMemc != MEMCACHED_SUCCESS &&
	  		statMemc != MEMCACHED_NOTSTORED &&
        statMemc != MEMCACHED_PROTOCOL_ERROR) {
    	throw MemcacheException(statMemc, this->conn_);
  	}
    if (statMemc == MEMCACHED_NOTSTORED) {
      noSegments++;
      segmentKey = listKey + ":" + this->toString(noSegments-1);
  
      statMemc = 
          memcached_set(this->conn_,
                        segmentKey.data(),
                        segmentKey.length(),
                        serialKey.data(),
                        serialKey.length(),
                        this->memcachedExpirationLimit_, 
                        (uint32_t)0);

      if (statMemc != MEMCACHED_SUCCESS)
        throw MemcacheException(statMemc, this->conn_);

      statMemc = 
          memcached_increment(this->conn_,
                              listKey.data(),
                              listKey.length(),
                              1, // offset
                              &noSegmentsIncremented);

      if (statMemc != MEMCACHED_SUCCESS)
        throw MemcacheException(statMemc, this->conn_);
      if (noSegmentsIncremented != noSegments)
        throw DmException(DM_UNKNOWN_ERROR, std::string("Incrementing the number of segments on memcached failed."));
    }
  }
}

int MemcacheCatalog::safeAddToDListFromMemcachedKeyListNoReply(const std::string& listKey,
                               const std::vector<std::string>& keyList, 
                                                const bool isWhite,
                                                const bool isComplete,
                                                int curSegment) throw (DmException)
{
  try {
    return addToDListFromMemcachedKeyListNoReply(listKey, keyList, isWhite,
                                      isComplete, curSegment);
  } catch (MemcacheException) { /* pass */ }
}


int MemcacheCatalog::addToDListFromMemcachedKeyListNoReply(
                               const std::string& listKey,
                               const std::vector<std::string>& keyList, 
                               const bool isWhite,
                               const bool isComplete,
                               int curSegment) throw (DmException)
{
	memcached_return statMemc;
  uint64_t noSegmentsIncremented;
  std::string segmentKey;
  std::string serialKey;
  std::vector<std::string> singleKey;

  segmentKey = listKey + ":" + this->toString(curSegment);

  statMemc = memcached_behavior_set(this->conn_, MEMCACHED_BEHAVIOR_NOREPLY, 1);

  std::vector<std::string> mykeyList(keyList);
  std::vector<std::string>::iterator it;
  for (it = mykeyList.begin();
       it != (mykeyList.end()-1);
       it++) {
    singleKey.assign(1,*it);
    serialKey = serializeList(singleKey, isWhite, isComplete);
    statMemc = 
      memcached_append(this->conn_,
                       segmentKey.data(),
                       segmentKey.length(),
                       serialKey.data(),
                       serialKey.length(),
                       this->memcachedExpirationLimit_, 
                       (uint32_t)0);
  }
  statMemc = memcached_behavior_set(this->conn_, MEMCACHED_BEHAVIOR_NOREPLY, 0);

  singleKey.assign(1,mykeyList.back());
  serialKey = serializeList(singleKey, isWhite, isComplete);
  statMemc = 
      memcached_append(this->conn_,
                       segmentKey.data(),
                       segmentKey.length(),
                       serialKey.data(),
                       serialKey.length(),
                       this->memcachedExpirationLimit_, 
                       (uint32_t)0);

	if (statMemc != MEMCACHED_SUCCESS &&
			statMemc != MEMCACHED_NOTSTORED &&
      statMemc != MEMCACHED_PROTOCOL_ERROR) {
  	throw MemcacheException(statMemc, this->conn_);
	}
  if (statMemc == MEMCACHED_NOTSTORED ||
      statMemc == MEMCACHED_PROTOCOL_ERROR) {
    curSegment++;
    segmentKey = listKey + ":" + this->toString(curSegment);

      singleKey.assign(mykeyList.begin(), mykeyList.end());
      serialKey = serializeList(singleKey, isWhite, isComplete);
      statMemc = 
        memcached_set(this->conn_,
                      segmentKey.data(),
                      segmentKey.length(),
                      serialKey.data(),
                      serialKey.length(),
                      this->memcachedExpirationLimit_, 
                      (uint32_t)0);

    if (statMemc != MEMCACHED_SUCCESS)
      throw MemcacheException(statMemc, this->conn_);

    statMemc = 
        memcached_increment(this->conn_,
                            listKey.data(),
                            listKey.length(),
                            1, // offset
                            &noSegmentsIncremented);

    if (statMemc != MEMCACHED_SUCCESS)
      throw MemcacheException(statMemc, this->conn_);
    if ((noSegmentsIncremented - 1) != curSegment)
      throw DmException(DM_UNKNOWN_ERROR,
          std::string("Incrementing the number of segments on memcached failed."));
  }
  return curSegment;
}


void MemcacheCatalog::removeListItemFromMemcachedKey(const std::string& listKey, std::string& key)
{
  addToListFromMemcachedKey(listKey, key, false);
}

void MemcacheCatalog::safeRemoveListItemFromMemcachedKey(const std::string& listKey, std::string& key)
{
  safeAddToListFromMemcachedKey(listKey, key, false);
}
