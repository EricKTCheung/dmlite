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

// encodes the isCached variable into the direcotry inode 
// the value is encoded in the last two bits of the integer
#define ENCODE_DIRCACHED(dir, isCached) \
dir->dirId = (dir->dirId << 2) + isCached;

// takes the isCached value from the encoded one and
// resets the directory inode to its true value
#define DECODE_DIRCACHED(dir, isCached) \
isCached = (dirp->dirId & 3); \
dirp->dirId = (dirp->dirId >> 2);


MemcacheCatalog::MemcacheCatalog(PoolContainer<memcached_st*>* connPool, 
																 Catalog* decorates,
																 unsigned int symLinkLimit,
																 time_t memcachedExpirationLimit,
                                 bool memcachedStrict) throw (DmException):
   DummyCatalog(decorates),
	 symLinkLimit_(symLinkLimit),
	 memcachedExpirationLimit_(memcachedExpirationLimit),
   memcachedStrict_(memcachedStrict)
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

std::string MemcacheCatalog::serializeList(std::vector<std::string>& keyList, const bool isWhite, const bool isComplete)
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
       itKeyList++)
  {
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

	valMemc = getValFromMemcachedKey(key);
	if (!valMemc.empty())
	{
		deserialize(valMemc, meta);
	} else // valMemc was not in memcached
	{
		DELEGATE_ASSIGN(meta, extendedStat, fileId);
		valMemc = serialize(meta);
		setMemcachedFromKeyValue(key, valMemc);
	}

  return meta;
}

ExtendedStat MemcacheCatalog::extendedStat(ino_t parent, const std::string& name) throw (DmException)
{
  ExtendedStat meta;

  memset(&meta, 0x00, sizeof(ExtendedStat));

	std::string valMemc;

	const std::string key = keyFromAny(key_prefix[PRE_STAT], parent, name);

	valMemc = getValFromMemcachedKey(key);
	if (!valMemc.empty())
	{
		deserialize(valMemc, meta);
	} else // valMemc was not in memcached
	{
		DELEGATE_ASSIGN(meta, extendedStat, parent, name);
		valMemc = serialize(meta);
		setMemcachedFromKeyValue(key, valMemc);
	}

  return meta;
}

SymLink MemcacheCatalog::readLink(ino_t linkId) throw(DmException)
{
  SymLink meta; 

  memset(&meta, 0x00, sizeof(SymLink));

	std::string valMemc;

	const std::string key = keyFromAny(key_prefix[PRE_LINK], linkId); 

	valMemc = getValFromMemcachedKey(key);
	if (!valMemc.empty())
	{
		deserializeLink(valMemc, meta);
	} else // valMemc was not in memcached
	{
		DELEGATE_ASSIGN(meta, readLink, linkId);
		valMemc = serializeLink(meta);
		setMemcachedFromKeyValue(key, valMemc);
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
/*
  There are two variations of the openDir, readDirx methods.
  The first one works as is, the second one would be favourable, but
  does not work. It wants to use memcached_fetch like mysql_fetch,
  but the memcached_fetch returns all values at once, so you cannot
  get items in separate function calls.
  If that behaviour seems possible at some time, the code still there to
  be used. It is commented out with //+
  */
  Directory    *dir, *remote_dir; 
  MemcacheDir  *local_dir;
  ExtendedStat meta;
  std::string  valMemc;
  std::vector<std::string> keyList;
  uint32_t     isCached;
  time_t       mtime; 

  if (this->memcachedStrict_)
  {
    // Get the dir stat from a delegate to compare the utimes 
    DELEGATE_ASSIGN(meta, extendedStat, path);
  } else
  {
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
  
  const std::string listKey = keyFromAny(key_prefix[PRE_DIR], 
                                         local_dir->dirId);

  valMemc = getDListValFromMemcachedKey(listKey);

  if (!valMemc.empty())
  {
    isCached = deserializeDirList(valMemc, keyList, mtime);
    if (mtime < meta.stat.st_mtime)
    {
      delMemcachedFromKey(listKey);
      isCached = DIR_NOTCACHED;
    }
    if (isCached == DIR_CACHED)
    { 
//+      multiGetFromMemcachedByKeys(keyList);
      local_dir->keys = keyList;
      local_dir->keysPntr = 0;
      dir = local_dir;
    } 
  } else
  {
    isCached = DIR_NOTCACHED;
  }
  if (isCached == DIR_NOTCACHED ||
      isCached == DIR_NOTCOMPLETE)
  {
    delete local_dir;
    DELEGATE_ASSIGN(remote_dir, openDir, path);
    dir = remote_dir;
  }
  if (isCached == DIR_NOTCACHED)
  {
    // create an empty list with 'white' elements, which is not complete
    std::vector<std::string> empty;
    valMemc = serializeDirList(empty, tim.modtime, true, false);
    try 
    {
      setMemcachedDListFromKeyValue(listKey, valMemc);
    } catch (...)
    {
      delete local_dir;
      throw;
    }
  }

  // encode the isCached value
  local_dir = (MemcacheDir *) dir;
  ENCODE_DIRCACHED(local_dir, isCached);
  dir = local_dir;

  return dir;
}

void MemcacheCatalog::closeDir(Directory* dir) throw(DmException)
{
  MemcacheDir *dirp;
  uint32_t isCached;
  dirp = (MemcacheDir *) dir;
  DECODE_DIRCACHED(dirp, isCached);

  if (isCached == DIR_NOTCACHED ||
      isCached == DIR_NOTCOMPLETE)
  {
    DELEGATE(closeDir, dir);
  } else
  {
//+    if (dirp->keysPntr < dirp->keys.size())
//+      memcached_quit(this->conn_);

    delete dirp;
  }
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
  uint32_t isCached;

  if (dir == 0x00)
    throw DmException(DM_NULL_POINTER, "Tried to read a null dir");

  dirp = (MemcacheDir*)dir;
  DECODE_DIRCACHED(dirp, isCached);

  switch (isCached)
  {
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
    if (isCached == DIR_CACHED)
    {
      // copy the stat info into the dirent and touch 
      // only if not already done by the delegate plugin
      memset(&dirp->ds, 0x00, sizeof(struct dirent));
      dirp->ds.d_ino  = dirp->current.stat.st_ino;
      strncpy(dirp->ds.d_name,
              dirp->current.name,
              sizeof(dirp->ds.d_name));

      if (this->memcachedStrict_)
      {
        // Touch
        struct utimbuf tim;
        tim.actime  = time(NULL);
        tim.modtime = dirp->current.stat.st_mtime;
        this->utime(dirp->dirId, &tim);
      }
    }

    ENCODE_DIRCACHED(dirp, isCached);

    return &dirp->current;
  }
  else {
    // mark the file list in Memcached as complete
    std::string valMemc;
    const std::string listKey = keyFromAny(key_prefix[PRE_DIR], 
                                           dirp->dirId);

    // append an empty list with 'white' elements, which is complete
    // to mark the lsit as complete
    addToDListFromMemcachedKey(listKey, std::string(), true, true);

    ENCODE_DIRCACHED(dirp, isCached);
    
    return 0x00;
  }
}

void MemcacheCatalog::changeMode(const std::string& path, mode_t mode) throw (DmException)
{
	DELEGATE(changeMode, path, mode);
	delMemcachedFromPath(path, KEEP_DIRLIST);
}

void MemcacheCatalog::changeOwner(const std::string& path, uid_t newUid, gid_t newGid)
  throw (DmException)
{
	DELEGATE(changeOwner, path, newUid, newGid);
	delMemcachedFromPath(path, KEEP_DIRLIST);
}

void MemcacheCatalog::linkChangeOwner(const std::string& path, uid_t newUid, gid_t newGid)
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
  delMemcachedFromPath(path, KEEP_DIRLIST);
}

void MemcacheCatalog::utime(ino_t inode, const struct utimbuf* buf) throw (DmException)
{
  DELEGATE(utime, inode, buf);
  const std::string key = keyFromAny(key_prefix[PRE_STAT], inode);
  delMemcachedFromKey(key);
}

std::string MemcacheCatalog::getComment(const std::string& path) throw(DmException)
{
  // Get the file and check we can read
  ExtendedStat meta = this->extendedStat(path);
  
  if (checkPermissions(&this->secCtx_, meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to read " + path);

  // Query
  std::string comment;
	std::string valMemc;

	const std::string key = keyFromAny(key_prefix[PRE_COMMENT], meta.stat.st_ino); 

	valMemc = getValFromMemcachedKey(key);
	if (!valMemc.empty())
	{
		deserializeComment(valMemc, comment);
	} else // valMemc was not in memcached
	{
		DELEGATE_ASSIGN(comment, getComment, path);
		valMemc = serializeComment(comment);
		setMemcachedFromKeyValue(key, valMemc);
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

  vecValMemc = getListFromMemcachedKey(listKey);

	if (vecValMemc.size() > 0)
	{
    // Pick a random one
    int i = rand() % vecValMemc.size();

    repl = deserializeFileReplica(vecValMemc[i]);
  } else
  {
    std::vector<FileReplica> replicas;

    // otherwise, get replicas from mysql
    DELEGATE_ASSIGN(replicas, getReplicas, path);

    // save replicas in memcached
    setMemcachedFromReplicas(replicas, inode);

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
  	throw MemcacheException(statMemc, this->conn_);
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
		  	statMemc != MEMCACHED_NOTFOUND)
	  {
  	  throw MemcacheException(statMemc, this->conn_);
	  }

	  if (lenValue > 0)
	  {
		  valMemcStr.assign(valMemc, lenValue);
	  }
  }
	return valMemcStr;
}

const std::string MemcacheCatalog::getDListValFromMemcachedKey(const std::string key)
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
			statMemc != MEMCACHED_NOTFOUND)
	{
  	throw MemcacheException(statMemc, this->conn_);
	}

  if (statMemc == MEMCACHED_SUCCESS)
  {
    noSegments = this->atoi(valMemc, lenValue);
  
    for (int i = 0; i < noSegments; i++)
    {
      segmentKey = key + ":" + this->toString(i);
      printf("getting the segment no %d.\n", i);
      valMemcSegmentStr = getValFromMemcachedKey(segmentKey);
      printf("gotten the segment no %d.\n", i);
      if (valMemcSegmentStr.length() == 0)
      {
        valMemcStr.clear();
      }
      valMemcStr.append(valMemcSegmentStr);
    }
  }
	return valMemcStr;
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
  for (i = 0; i < keyList.size(); i++)
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
  	throw MemcacheException(statMemc, this->conn_);
	}

  for (i = 0; i < keyList.size(); i++)
  {
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
                                   &statMemc)))
  {
	  if (statMemc != MEMCACHED_SUCCESS)
	  {
  	  throw MemcacheException(statMemc, this->conn_);
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

ExtendedStat* MemcacheCatalog::fetchExtendedStatFromMemcached(MemcacheDir *dirp)
                            throw (DmException)
{
  if (dirp->keysPntr >= dirp->keys.size())
    return 0x00;

  ExtendedStat meta;
  ExtendedStat *return_meta;
  memcached_return statMemc;
	char *valMemc;

  char return_key[MEMCACHED_MAX_KEY+1];
  size_t return_key_length = 0;
  char *return_value;
  size_t lenValue;
	uint32_t flags;
	std::string valMemcStr;

  memset(&meta, 0x00, sizeof(ExtendedStat));

  valMemc = memcached_get(this->conn_,
                          dirp->keys[dirp->keysPntr].data(),
                          dirp->keys[dirp->keysPntr].length(),
                          &lenValue,
                          &flags,
                          &statMemc);
  dirp->keysPntr++;

//+  valMemc = memcached_fetch(this->conn_,
//+                            return_key,
//+                            &return_key_length,
//+                            &lenValue,
//+                            &flags,
//+                            &statMemc);
  
//+  // make sure the key is null-terminated
//+  return_key[return_key_length+1] = '\0';
	if (statMemc != MEMCACHED_SUCCESS &&
			statMemc != MEMCACHED_NOTFOUND)
	{
  		throw MemcacheException(statMemc, this->conn_);
//		throw DmException(DM_UNKNOWN_ERROR,
//											std::string(memcached_strerror(this->conn_,
//																										 statMemc)));
	}
  // not found, get it from the DB
  else if (statMemc == MEMCACHED_NOTFOUND)
  {
    DELEGATE_ASSIGN(meta, extendedStat, return_key);
		valMemcStr = serialize(meta);
    // add xstat to memcached 
		setMemcachedFromKeyValue(return_key, valMemcStr);
    // create directory list entry for the xstat
    const std::string listKey = keyFromAny(key_prefix[PRE_DIR], 
                                           dirp->dirId);
    addToListFromMemcachedKey(listKey, return_key);
  } else // MEMCACHED_SUCCESS
  {
		valMemcStr.assign(valMemc, lenValue);
    if (!valMemcStr.empty())
    {
  		deserialize(valMemcStr, meta);
      // copy the extendedStat into dir->current
      std::memcpy(&(dirp->current), &meta, sizeof(ExtendedStat));
      return_meta = &(dirp->current);
    } else
    {
      return_meta = 0x00;
    }
    free(valMemc);  
  }

	return return_meta;
}

ExtendedStat* MemcacheCatalog::fetchExtendedStatFromDelegate(MemcacheDir *dirp, const bool saveToMemc)
                            throw (DmException)
{
  std::string valMemcStr;
  ExtendedStat *meta;
  DELEGATE_ASSIGN(meta, readDirx, (Directory *) dirp);

  if (meta != 0x00 && saveToMemc)
  {
    valMemcStr = serialize(*meta);
    // add xstat to memcached 
    const std::string key = keyFromAny(key_prefix[PRE_STAT], 
                                       meta->stat.st_ino);
	  setMemcachedFromKeyValue(key, valMemcStr);
    // create directory list entry for the xstat
    const std::string listKey = keyFromAny(key_prefix[PRE_DIR], 
                                           dirp->dirId);
    addToDListFromMemcachedKey(listKey, key, true, false);
  }
  return meta;
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
  	throw MemcacheException(statMemc, this->conn_);
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
  	throw MemcacheException(statMemc, this->conn_);
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
  	throw MemcacheException(statMemc, this->conn_);
	}

	return;
}

int MemcacheCatalog::setMemcachedDListFromKeyValue(const std::string key,
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

	if (statMemc != MEMCACHED_SUCCESS &&
      statMemc != MEMCACHED_NOTSTORED)
	{
  	throw MemcacheException(statMemc, this->conn_);
	}
  if (statMemc == MEMCACHED_SUCCESS)
  {
    const std::string segmentKey = key + ":" + "0";

	  statMemc = memcached_set(this->conn_,
		  											 segmentKey.data(),
			  										 segmentKey.length(),
				  									 value.data(), value.length(),
					  								 this->memcachedExpirationLimit_,
  					  							 (uint32_t)0);

  	if (statMemc != MEMCACHED_SUCCESS)
	  {
  	  throw MemcacheException(statMemc, this->conn_);
    }
	}

	return statMemc;
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
			statMemc != MEMCACHED_NOTFOUND)
	{
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
			statMemc != MEMCACHED_NOTFOUND)
	{
  	throw MemcacheException(statMemc, this->conn_);
	}

  if (statMemc == MEMCACHED_SUCCESS)
  {
    noSegments = this->atoi(valMemc, lenValue);
  
    for (int i = 0; i < noSegments; i++)
    {
      std::string segmentKey = key + ":" + this->toString(i);
      delMemcachedFromKey(segmentKey);
    }
  }
  delMemcachedFromKey(key);
}


void MemcacheCatalog::delMemcachedFromPath(const std::string& path, bool removeDirEntry)
{
  ExtendedStat meta;

  try
  {  
    meta = this->extendedStat(path);
  } catch (DmException e)
  {
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
  if (removeDirEntry)
  {
    const std::string keyDir(keyFromAny(key_prefix[PRE_DIR],
                                        meta.parent));
    delMemcachedFromDListKey(keyDir);
  }
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
      statMemc != MEMCACHED_PROTOCOL_ERROR)
	{
  	throw MemcacheException(statMemc, this->conn_);
	}
}

void MemcacheCatalog::addToDListFromMemcachedKey(const std::string& listKey, const std::string& key, const bool isWhite, const bool isComplete)
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

  if (statMemc == MEMCACHED_SUCCESS)
  {
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
        statMemc != MEMCACHED_PROTOCOL_ERROR)
  	{
    	throw MemcacheException(statMemc, this->conn_);
  	}
    if (statMemc == MEMCACHED_NOTSTORED)
    {
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

void MemcacheCatalog::removeListItemFromMemcachedKey(const std::string& listKey, std::string& key)
{
  addToListFromMemcachedKey(listKey, key, false);
}
