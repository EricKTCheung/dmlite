/// @file    plugins/memcache/MemcacheCatalog.cpp
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>

#include <algorithm>
#include <libgen.h>
#include <cstring>
#include <set>

#include <dmlite/cpp/utils/checksums.h>

#include "MemcacheCatalog.h"
#include "MemcacheFunctions.h"

using namespace dmlite;

/// Used to keep the Key Prefixes
enum {
  PRE_STAT = 0,
  PRE_REPL,
  PRE_REPL_LIST,
  PRE_COMMENT,
  PRE_DIR,
  PRE_DIR_LIST
};

/// Used internally to define Key Prefixes.
/// Must match with PRE_* constants!
static const char* key_prefix[] = {
  "STAT",
  "REPL",
  "RPLI",
  "CMNT",
  "DIR",
  "DRLI"
};


/// Little of help here to avoid redundancy
#define DELEGATE(method, ...) \
{ \
  if (this->decorated_ == 0x00)\
throw DmException(DMLITE_SYSERR(ENOSYS), "There is no plugin in the stack that implements "#method);\
this->decorated_->method(__VA_ARGS__);\
} \

/// Little of help here to avoid redundancy
#define DELEGATE_RETURN(method, ...) \
{ \
  if (this->decorated_ == 0x00)\
throw DmException(DMLITE_SYSERR(ENOSYS), "There is no plugin in the stack that implements "#method);\
return this->decorated_->method(__VA_ARGS__);\
} \

/// Little of help here to avoid redundancy
#define DELEGATE_ASSIGN(var, method, ...) \
{ \
  if (this->decorated_ == 0x00)\
throw DmException(DMLITE_SYSERR(ENOSYS), "There is no plugin in the stack that implements "#method);\
var = this->decorated_->method(__VA_ARGS__);\
} \


inline ExtendedStat& fillChecksumInXattr(ExtendedStat& xstat)
{
  if (!xstat.csumtype.empty()) {
    std::string csumXattr("checksum.");
    csumXattr += checksums::fullChecksumName(xstat.csumtype);
    if (!xstat.hasField(csumXattr))
      xstat[csumXattr] = xstat.csumvalue;
  }
  return xstat;
}


MemcacheCatalog::MemcacheCatalog(PoolContainer<memcached_st*>& connPool,
    Catalog* decorates,
    MemcacheFunctionCounter* funcCounter,
    bool doFuncCount,
    unsigned int symLinkLimit,
    time_t memcachedExpirationLimit,
    bool memcachedPOSIX)
throw (DmException):
  si_(0x00),
  conn_(connPool),
  connNoReply_(0x0),
  funcCounter_(funcCounter),
  doFuncCount_(doFuncCount),
  symLinkLimit_(symLinkLimit),
  memcachedExpirationLimit_(memcachedExpirationLimit),
  memcachedPOSIX_(memcachedPOSIX)
{
  //this->conn_           = conn;

  //this->connNoReply_    = connPool_->acquire();

  // Don't worry about the return. If it's not successful,
  // it will just be slightly slower. No reason to fail.
  //memcached_behavior_set(this->connNoReply_, MEMCACHED_BEHAVIOR_NOREPLY, 1);

  this->decorated_   = decorates;
  this->decoratedId_ = new char [decorates->getImplId().size() + 1];
  strcpy(this->decoratedId_, decorates->getImplId().c_str());
}


MemcacheCatalog::~MemcacheCatalog() throw (DmException)
{
  //this->connPool_->release(this->conn_);
  //this->connPool_->release(this->connNoReply_);

  if (this->decorated_ != 0x00)
    delete this->decorated_;

  if (this->decoratedId_ != 0x00)
    delete this->decoratedId_;
}


std::string MemcacheCatalog::getImplId() const throw ()
{
  std::string implId = "MemcacheCatalog";
  implId += " over ";
  implId += this->decoratedId_;

  return implId;
}


void MemcacheCatalog::setStackInstance(StackInstance* si) throw (DmException)
{
  BaseInterface::setStackInstance(this->decorated_, si);
  this->si_ = si;
}


void MemcacheCatalog::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  BaseInterface::setSecurityContext(this->decorated_, ctx);
  this->secCtx_ = ctx;
}


void MemcacheCatalog::changeDir(const std::string& path) throw (DmException)
{
  incrementFunctionCounter(CHANGEDIR);

  if (path.empty()) {
    this->cwd_.clear();
  }

  std::string normPath;
  normPath = this->extendedStatPOSIX(path, true).getString("normPath");

  if (normPath[0] == '/')
    this->cwd_ = normPath;
  else
    this->cwd_ = Url::normalizePath(this->cwd_ + "/" + normPath);
}


std::string MemcacheCatalog::getWorkingDir(void) throw (DmException)
{
  incrementFunctionCounter(GETWORKINGDIR);
  return this->cwd_;
}


ExtendedStat MemcacheCatalog::extendedStat(const std::string& path, bool followSym) throw (DmException)
{
  if (this->memcachedPOSIX_)
    return this->extendedStatPOSIX(path, followSym);
  else
    return this->extendedStatNoPOSIX(path, followSym);
}


ExtendedStat MemcacheCatalog::extendedStatPOSIX(const std::string& path, bool followSym) throw (DmException)
{
  ExtendedStat meta;

  std::string currentPathElem;

  std::vector<std::string> components;
  std::vector<std::string> cwdComponents;

  std::string cwd = this->cwd_;

  unsigned int symLinkLevel = 0;
  unsigned int cwdMarker = 0;

  components = Url::splitPath(path);
  if (path[0] == '/' || cwd.empty()) {
    meta = this->extendedStatNoPOSIX("/", followSym);
  } else {
    cwdComponents = Url::splitPath(cwd);
    meta = this->extendedStatNoPOSIX(cwd, followSym);
  }

  for (; cwdMarker < components.size(); ) {
    // Check that the parent is a directory first
    if (!S_ISDIR(meta.stat.st_mode) && !S_ISLNK(meta.stat.st_mode))
      throw DmException(ENOTDIR,
                        meta.name + " is not a directory");
    // New element traversed! Need to check if it is possible to keep going.
    if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IEXEC) != 0)
      throw DmException(EACCES,
                        "Not enough permissions to list " + meta.name);

    currentPathElem = components[cwdMarker];

    // Stay here
    if (currentPathElem == ".") {
      // Nothing
    }
    // Up one level
    else if (currentPathElem == "..") {
      // always stay at root level
      if (!cwdComponents.empty()) cwdComponents.pop_back();
    }
    // Regular entry
    else {
      cwdComponents.push_back(currentPathElem);
      cwd = Url::joinPath(cwdComponents);
      // Stat, this throws an Exception if the path doesn't exist
      meta = this->extendedStatNoPOSIX(cwd, followSym);

      // Symbolic link!, follow that instead
      if (S_ISLNK(meta.stat.st_mode) && followSym) {
        std::string link = this->readLink(cwd);

        ++symLinkLevel;
        if (symLinkLevel > this->symLinkLimit_) {
          throw DmException(DMLITE_SYSERR(ELOOP),
                           "Symbolic links limit exceeded: > %d",
                           this->symLinkLimit_);
        }

        // We have the symbolic link now. Split it and push
        // into the component
        std::vector<std::string> symPath = Url::splitPath(link);

        for (unsigned j = cwdMarker + 1; j < components.size(); ++j)
          symPath.push_back(components[j]);

        components.swap(symPath);
        cwdMarker = 0;

        // If absolute, need to reset parent
        if (link[0] == '/') {
          cwdComponents.clear();
          meta = this->extendedStatNoPOSIX("/", followSym);
        }
        // Keep the meta of the symlink, will be replaced soon

        continue; // Jump directly to the beginning of the loop
      }
    }
    ++cwdMarker; // Next in array
  }

  // add the absolute path (cleared from relative movements and links)
  cwd = Url::joinPath(cwdComponents);
  if (cwd.empty())
    cwd = "/";
  meta["normPath"] = cwd;

  return fillChecksumInXattr(meta);
}


ExtendedStat MemcacheCatalog::extendedStatNoPOSIX(const std::string& path, bool followSym) throw (DmException)
{
  incrementFunctionCounter(EXTENDEDSTAT);

  ExtendedStat meta;

  std::string valMemc;

  std::string absPath = getAbsolutePath(path);
  const std::string key = keyFromString(key_prefix[PRE_STAT], absPath);

  valMemc = safeGetValFromMemcachedKey(key);
  if (!valMemc.empty()) {
    deserializeExtendedStat(valMemc, meta);
  } else // valMemc was not in memcached
  {
    incrementFunctionCounter(EXTENDEDSTAT_DELEGATE);
    DELEGATE_ASSIGN(meta, extendedStat, absPath, followSym);
    serializeExtendedStat(meta, valMemc);
    safeSetMemcachedFromKeyValue(key, valMemc);
  }

  return fillChecksumInXattr(meta);
}


ExtendedStat MemcacheCatalog::extendedStatByRFN(const std::string& rfn) throw (DmException)
{
  incrementFunctionCounter(EXTENDEDSTATBYRFN);

  ExtendedStat meta;

  std::string valMemc;

  const std::string key = keyFromString(key_prefix[PRE_STAT], rfn);

  valMemc = safeGetValFromMemcachedKey(key);
  if (!valMemc.empty()) {
    deserializeExtendedStat(valMemc, meta);
  } else // valMemc was not in memcached
  {
    incrementFunctionCounter(EXTENDEDSTATBYRFN_DELEGATE);
    DELEGATE_ASSIGN(meta, extendedStatByRFN, rfn);
    serializeExtendedStat(meta, valMemc);
    safeSetMemcachedFromKeyValue(key, valMemc);
  }

  return fillChecksumInXattr(meta);
}


bool MemcacheCatalog::access(const std::string& path, int mode) throw (DmException)
{
  // Copy from BuiltinCatalog to use the memcache extendedStat method
  try {
    ExtendedStat xstat = this->extendedStat(path);

    mode_t perm = 0;
    if (mode & R_OK) perm  = S_IREAD;
    if (mode & W_OK) perm |= S_IWRITE;
    if (mode & X_OK) perm |= S_IEXEC;

    return checkPermissions(this->secCtx_, xstat.acl, xstat.stat, perm) == 0;
  }
  catch (DmException& e) {
    if (e.code() != EACCES) throw;
    return false;
  }
}


bool MemcacheCatalog::accessReplica(const std::string& replica, int mode) throw (DmException)
{
  incrementFunctionCounter(ACCESSREPLICA_DELEGATE);
  DELEGATE_RETURN(accessReplica, replica, mode);
}


void MemcacheCatalog::addReplica(const Replica& replica) throw (DmException)
{
  incrementFunctionCounter(ADDREPLICA_DELEGATE);
  DELEGATE(addReplica, replica);
}


void MemcacheCatalog::deleteReplica(const Replica& replica) throw (DmException)
{
  incrementFunctionCounter(DELETEREPLICA_DELEGATE);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_REPL], replica.rfn));
  DELEGATE(deleteReplica, replica);
}


std::vector<Replica> MemcacheCatalog::getReplicas(const std::string& path) throw(DmException)
{
  incrementFunctionCounter(GETREPLICAS);

  std::vector<Replica> replicas;
  std::string valMemc;
  SerialReplicaList pb_replicas;
  Replica repl;

  // get replica list from memcached
  std::string absPath = getAbsolutePath(path);
  const std::string key = keyFromString(key_prefix[PRE_REPL_LIST], absPath);

  valMemc = safeGetValFromMemcachedKey(key);

  if (!valMemc.empty()) {
    pb_replicas.ParseFromString(valMemc);
    deserializeReplicaList(valMemc, replicas);
  }
  if (replicas.size() == 0) {
    // otherwise, get replicas from mysql
    // this also applies if all cached replicas have been
    // deleted, but just the list remains
    incrementFunctionCounter(GETREPLICAS_DELEGATE);
    DELEGATE_ASSIGN(replicas, getReplicas, absPath);

    valMemc = serializeReplicaList(replicas);
    safeSetMemcachedFromKeyValue(key, valMemc);
  }

  return replicas;
}


void MemcacheCatalog::symlink(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
  std::string absOldPath = getAbsolutePath(oldPath);
  std::string absNewPath = getAbsolutePath(newPath);
  incrementFunctionCounter(SYMLINK_DELEGATE);
  DELEGATE(symlink, absOldPath, absNewPath);
}


std::string MemcacheCatalog::readLink(const std::string& path) throw(DmException)
{
  std::string absPath = getAbsolutePath(path);
  incrementFunctionCounter(READLINK_DELEGATE);
  DELEGATE_RETURN(readLink, absPath);
}


void MemcacheCatalog::unlink(const std::string& path) throw (DmException)
{
  incrementFunctionCounter(UNLINK_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_REPL_LIST], absPath));
  DELEGATE(unlink, absPath);
}


void MemcacheCatalog::create(const std::string& path, mode_t mode) throw (DmException)
{
  incrementFunctionCounter(CREATE_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  std::string basepath = getBasePath(absPath);
  DELEGATE(create, absPath, mode);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], basepath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR_LIST], basepath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], basepath));
}


mode_t MemcacheCatalog::umask(mode_t mask) throw ()
{
  incrementFunctionCounter(UMASK_DELEGATE);
  DELEGATE_RETURN(umask, mask);
}


void MemcacheCatalog::setMode(const std::string& path, mode_t mode)
   throw (DmException)
{
  incrementFunctionCounter(SETMODE_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  DELEGATE(setMode, absPath, mode);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR_LIST], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], absPath));
}


void MemcacheCatalog::setOwner(const std::string& path,
                               uid_t newUid, gid_t newGid,
                               bool followSymLink)
  throw (DmException)
{
  incrementFunctionCounter(SETOWNER_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  DELEGATE(setOwner, absPath, newUid, newGid);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR_LIST], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], absPath));
}


void MemcacheCatalog::setSize(const std::string& path,
                                  size_t newSize)
  throw (DmException)
{
  incrementFunctionCounter(SETSIZE_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  DELEGATE(setSize, absPath, newSize);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
}


void MemcacheCatalog::setChecksum(const std::string& path,
                                  const std::string& csumtype,
                                  const std::string& csumvalue)
  throw (DmException)
{
  incrementFunctionCounter(SETCHECKSUM_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  DELEGATE(setChecksum, absPath, csumtype, csumvalue);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
}


void MemcacheCatalog::setAcl(const std::string& path, const Acl& acl) throw (DmException)
{
  incrementFunctionCounter(SETACL_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  DELEGATE(setAcl, absPath, acl);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], absPath));
}


void MemcacheCatalog::utime(const std::string& path, const struct utimbuf* buf) throw (DmException)
{
  incrementFunctionCounter(UTIME_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  DELEGATE(utime, absPath, buf);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], absPath));
}


std::string MemcacheCatalog::getComment(const std::string& path) throw (DmException)
{
  incrementFunctionCounter(GETCOMMENT);

  // Get the file and check we can read
  ExtendedStat meta = this->extendedStat(path);

  if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(EACCES, "Not enough permissions to read " + path);

  // Query
  std::string comment;
  std::string valMemc;

  std::string absPath = getAbsolutePath(path);
  const std::string key = keyFromString(key_prefix[PRE_COMMENT], absPath);

  valMemc = safeGetValFromMemcachedKey(key);
  if (!valMemc.empty()) {
    deserializeComment(valMemc, comment);
  } else // valMemc was not in memcached
  {
    incrementFunctionCounter(GETCOMMENT_DELEGATE);
    DELEGATE_ASSIGN(comment, getComment, absPath);
    valMemc = serializeComment(comment);
    safeSetMemcachedFromKeyValue(key, valMemc);
  }
  return comment;
}


void MemcacheCatalog::setComment(const std::string& path, const std::string& comment) throw (DmException)
{
  incrementFunctionCounter(SETCOMMENT_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  DELEGATE(setComment, absPath, comment);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_COMMENT], absPath));
}


void MemcacheCatalog::setGuid(const std::string& path, const std::string& guid) throw (DmException)
{
  incrementFunctionCounter(SETGUID_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  DELEGATE(setGuid, absPath, guid);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], absPath));
}


void MemcacheCatalog::updateExtendedAttributes(const std::string& path,
                              const Extensible& attr) throw (DmException)
{
  incrementFunctionCounter(UPDATEEXTENDEDATTRIBUTES_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  DELEGATE(updateExtendedAttributes, absPath, attr);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], absPath));
}


Directory* MemcacheCatalog::openDir(const std::string& path) throw(DmException)
{
  incrementFunctionCounter(OPENDIR);

  MemcacheDir *dirp;
  ExtendedStat meta;

  meta = this->extendedStat(path);

  if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(EACCES,
        "Not enough permissions to read " + path);

  dirp = new MemcacheDir();

  dirp->dir = meta;
  dirp->basepath = getAbsolutePath(meta.getString("normPath"));

  // get replica list from memcached
  const std::string key = keyFromString(key_prefix[PRE_DIR_LIST], dirp->basepath);

  const std::string valMemc = safeGetValFromMemcachedKey(key);

  if (!valMemc.empty()) {
    //dirp->isCached = true;
    // ParseFromString will set the state for us
    dirp->pb_keys.ParseFromString(valMemc);
    dirp->pb_keys_idx = 0;
  }
  // also check the state of the value in memcached
  if (dirp->pb_keys.state() != VALID) {
    //dirp->isCached = false;
    // avoid a race condition creating directories.
    // only one memcached_add can succeed
    const std::string dirkey = keyFromString(key_prefix[PRE_DIR], dirp->basepath);
    try {
      addMemcachedFromKeyValue(dirkey, "CANBEANYTHING");
      dirp->pb_keys.set_state(MISSING);
    } catch (MemcacheException) {
      dirp->pb_keys.set_state(INVALID);
    }
    incrementFunctionCounter(OPENDIR_DELEGATE);
    try {
      DELEGATE_ASSIGN(dirp->decorated_dirp, openDir, dirp->basepath);
    } catch (...) {
      delete dirp;
      throw;
    }
  }

  return dirp;
}


void MemcacheCatalog::closeDir(Directory* dir) throw(DmException)
{
  incrementFunctionCounter(CLOSEDIR);

  MemcacheDir *dirp = dynamic_cast<MemcacheDir *>(dir);
  //if (!(dirp->isCached)) {
  if (dirp->pb_keys.state() != VALID) {
    incrementFunctionCounter(CLOSEDIR_DELEGATE);
    DELEGATE(closeDir, dirp->decorated_dirp);
  }
  delete dirp;
}


struct dirent* MemcacheCatalog::readDir(Directory* dir) throw(DmException)
{
  if (this->readDirx(dir) == 0)
    return 0x00;
  else
    return &((dynamic_cast<MemcacheDir *>(dir))->ds);
}


ExtendedStat* MemcacheCatalog::readDirx(Directory* dir) throw(DmException)
{
  incrementFunctionCounter(READDIRX);

  MemcacheDir *dirp = dynamic_cast<MemcacheDir *>(dir);
  ExtendedStat *pMeta = &(dirp->dir);

  switch (dirp->pb_keys.state()) {
    case MISSING:
      incrementFunctionCounter(READDIRX_DELEGATE);
      pMeta = delegateReadDirxAndAddEntryToCache(dirp);
      break;
    case INVALID:
      incrementFunctionCounter(READDIRX_DELEGATE);
      DELEGATE_ASSIGN(pMeta, readDirx, dirp->decorated_dirp);
      break;
    case VALID:
      pMeta = getDirEntryFromCache(dirp);
      break;
  }

  // we need the dirent struct as well ...
  if (pMeta != 0x00) {
    dirp->ds.d_ino = dirp->dir.stat.st_ino;
    strncpy(dirp->ds.d_name,
            (*pMeta).name.c_str(),
            sizeof(dirp->ds.d_name));
  }
  else if (dirp->pb_keys.state() == MISSING) {
    // finally push all to memcached
    //std::string valMemc = serializeDirList(dirp->keys);
    dirp->pb_keys.set_state(VALID);
    std::string valMemc = dirp->pb_keys.SerializeAsString();
    std::string listkey = keyFromString(key_prefix[PRE_DIR_LIST], dirp->basepath);
    safeSetMemcachedFromKeyValue(listkey, valMemc);
  }

  if (pMeta == 0x00)
    return 0x00;
  else {
    fillChecksumInXattr(*pMeta);
    return pMeta;
  }
}


ExtendedStat* MemcacheCatalog::delegateReadDirxAndAddEntryToCache(MemcacheDir *dirp) throw (DmException)
{
  ExtendedStat* pMeta = &(dirp->dir);

  DELEGATE_ASSIGN(pMeta, readDirx, dirp->decorated_dirp);
  if (pMeta != 0x00) {
    // safety net not to store large elements in memcached
    // computes the serialized size of the message, it can still
    // grow large in memory. use .SpaceUsed() to test that.
    if (dirp->pb_keys.ByteSize() > (1 << 20)) {
      syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s:: %s: %d", this->decoratedId_,
          "dir size to large to cache", dirp->pb_keys.ByteSize());
      dirp->pb_keys.set_state(INVALID);
    }

    //dirp->keys.push_back(pMeta->name);
    SerialKey *pntKey = dirp->pb_keys.add_key();
    pntKey->set_key(pMeta->name);
  }

  return pMeta;
}


ExtendedStat* MemcacheCatalog::getDirEntryFromCache(MemcacheDir *dirp) throw (DmException)
{
  ExtendedStat *pMeta = &(dirp->dir);

  if (dirp->pb_keys.key_size() > dirp->pb_keys_idx) {
    std::string valMemc;
    //std::string absPath = dirp->basepath + dirp->keys.front();
    std::string absPath =
      concatPath(dirp->basepath,
                 dirp->pb_keys.key(dirp->pb_keys_idx).key());
    //dirp->keys.pop_front();
    dirp->pb_keys_idx++;

    std::string key = keyFromString(key_prefix[PRE_STAT], absPath);

    valMemc = safeGetValFromMemcachedKey(key);

    if (!valMemc.empty()) {
      deserializeExtendedStat(valMemc, *pMeta);
    } else {
      DELEGATE_ASSIGN(*pMeta, extendedStat, absPath, true);
      serializeExtendedStat(*pMeta, valMemc);
      safeSetMemcachedFromKeyValue(key, valMemc);
    }
  } else {
    return 0x00;
  }

  return pMeta;
}


void MemcacheCatalog::makeDir(const std::string& path, mode_t mode) throw (DmException)
{
  incrementFunctionCounter(MAKEDIR_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  std::string basepath = getBasePath(absPath);
  DELEGATE(makeDir, absPath, mode);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], basepath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR_LIST], basepath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], basepath));
}


void MemcacheCatalog::rename(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
  incrementFunctionCounter(RENAME_DELEGATE);
  std::string absOldPath = getAbsolutePath(oldPath);
  std::string absNewPath = getAbsolutePath(newPath);
  DELEGATE(rename, absOldPath, absNewPath);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absOldPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], absOldPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR_LIST], absOldPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_REPL_LIST], absOldPath));
}


void MemcacheCatalog::removeDir(const std::string& path) throw (DmException)
{
  incrementFunctionCounter(REMOVEDIR_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  DELEGATE(removeDir, absPath);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR_LIST], absPath));
}


Replica MemcacheCatalog::getReplicaByRFN(const std::string& rfn) throw (DmException)
{
  incrementFunctionCounter(GETREPLICABYRFN);

  Replica replica;

  std::string valMemc;

  const std::string key = keyFromString(key_prefix[PRE_REPL], rfn);

  valMemc = safeGetValFromMemcachedKey(key);
  if (!valMemc.empty()) {
    deserializeReplica(valMemc, replica);
  } else // valMemc was not in memcached
  {
    incrementFunctionCounter(GETREPLICABYRFN_DELEGATE);
    DELEGATE_ASSIGN(replica, getReplicaByRFN, rfn);
    valMemc = serializeReplica(replica);
    safeSetMemcachedFromKeyValue(key, valMemc);
  }

  return replica;
}


void MemcacheCatalog::updateReplica(const Replica& replica) throw (DmException)
{
  incrementFunctionCounter(UPDATEREPLICA_DELEGATE);
  DELEGATE(updateReplica, replica);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_REPL], replica.rfn));
}


/* internal functions */


const std::string MemcacheCatalog::keyFromString(const char* preKey,
    const std::string& key)
{
  std::stringstream streamKey;

  streamKey << preKey << ":" << key;

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


const std::string MemcacheCatalog::getValFromMemcachedKey(const std::string& key)  throw (MemcacheException)
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
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s:: %s: %s", this->decoratedId_,
        "getting a value from memcache failed",
        memcached_strerror(this->conn_, statMemc));
    throw MemcacheException(statMemc, this->conn_);
  }

  if (lenValue > 0) {
    valMemcStr.assign(valMemc, lenValue);
    free(valMemc);
  }
  return valMemcStr;
}


const std::string MemcacheCatalog::safeGetValFromMemcachedKey(const std::string& key) throw ()
{
  try {
    return getValFromMemcachedKey(key);
  } catch (MemcacheException) { return std::string(); /* pass */ }
}


void MemcacheCatalog::setMemcachedFromKeyValue(const std::string& key,
    const std::string& value, const bool noreply) throw (MemcacheException)
{
  //memcached_st *conn;
  //if (noreply)
  //  conn = this->connNoReply_;
  //else
  //  conn = this->conn_;

  memcached_return statMemc;
  statMemc = memcached_set(this->conn_, //conn,
      key.data(),
      key.length(),
      value.data(), value.length(),
      this->memcachedExpirationLimit_,
      (uint32_t)0);

  if (statMemc != MEMCACHED_SUCCESS) {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s:: %s: %s", this->decoratedId_,
        "setting a value to memcache failed",
        memcached_strerror(this->conn_, statMemc));
    throw MemcacheException(statMemc, this->conn_); //conn);
  }

  return;
}


void MemcacheCatalog::safeSetMemcachedFromKeyValue(const std::string& key,
    const std::string& value) throw ()
{
  try {
    // set noreply true, since we don't look at the return anyway
    return setMemcachedFromKeyValue(key, value, true);
  } catch (MemcacheException) { /* pass */ }
}


void MemcacheCatalog::addMemcachedFromKeyValue(const std::string& key,
    const std::string& value) throw (MemcacheException)
{
  memcached_return statMemc;
  statMemc = memcached_add(this->conn_,
      key.data(),
      key.length(),
      value.data(), value.length(),
      this->memcachedExpirationLimit_,
      (uint32_t)0);

  if (statMemc != MEMCACHED_SUCCESS) {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s:: %s: %s", this->decoratedId_,
        "adding a value to memcache failed",
        memcached_strerror(this->conn_, statMemc));
    throw MemcacheException(statMemc, this->conn_);
  }

  return;
}


void MemcacheCatalog::safeAddMemcachedFromKeyValue(const std::string& key,
    const std::string& value) throw ()
{
  try {
    return addMemcachedFromKeyValue(key, value);
  } catch (MemcacheException) { /* pass */ }
}


void MemcacheCatalog::delMemcachedFromKey(const std::string& key, const bool noreply) throw (MemcacheException)
{
  //memcached_st *conn;
  //if (noreply)
  //  conn = this->connNoReply_;
  //else
  //  conn = this->conn_;

  memcached_return statMemc;
  statMemc = memcached_delete(this->conn_, //conn,
      key.data(),
      key.length(),
      (time_t)0);

  if (statMemc != MEMCACHED_SUCCESS &&
      statMemc != MEMCACHED_NOTFOUND) {
    throw MemcacheException(statMemc, this->conn_); //conn);
  }
}


void MemcacheCatalog::safeDelMemcachedFromKey(const std::string& key) throw ()
{
  try {
    // set noreply true, since we don't look at the return anyway
    return delMemcachedFromKey(key, true);
  } catch (MemcacheException) { /* pass */ }
}


/* serialization functions */

void MemcacheCatalog::serializeExtendedStat(const ExtendedStat& var, std::string& serialString)
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


void MemcacheCatalog::deserializeExtendedStat(const std::string& serial_str, ExtendedStat& var)
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


std::string MemcacheCatalog::serializeReplica(const Replica& replica)
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


void MemcacheCatalog::deserializeReplica(const std::string& serial_str, Replica& replica)
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


std::string MemcacheCatalog::serializeReplicaList(const std::vector<Replica>& vecRepl)
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


void MemcacheCatalog::deserializeReplicaList(const std::string& serial_str, std::vector<Replica>& vecRepl)
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


std::string MemcacheCatalog::serializeComment(const std::string& var)
{
  //GOOGLE_PROTOBUF_VERIFY_VERSION;

  std::string serialString;
  SerialComment seComment;

  seComment.set_comment(var);

  return seComment.SerializeAsString();
}


void MemcacheCatalog::deserializeComment(std::string& serial_str,
                                         std::string& var)
{
  //GOOGLE_PROTOBUF_VERIFY_VERSION;

  SerialComment seComment;

  seComment.ParseFromString(serial_str);
//  seStat.PrintDebugString();

  var = seComment.comment();
}


inline void MemcacheCatalog::incrementFunctionCounter(const int funcName)
{
  if (this->funcCounter_ != 0x00) this->funcCounter_->incr(funcName, &this->randomSeed_);
}


//std::string MemcacheCatalog::normalizePath(const std::string& path)
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


std::string MemcacheCatalog::getAbsolutePath(const std::string& path)
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


std::string MemcacheCatalog::getBasePath(const std::string& path)
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


std::string MemcacheCatalog::concatPath(const std::string& basepath, const std::string& relpath)
  throw ()
{
  if (basepath[basepath.length()-1] == '/') {
    return basepath + relpath;
  } else {
    return basepath + "/" + relpath;
  }
}
