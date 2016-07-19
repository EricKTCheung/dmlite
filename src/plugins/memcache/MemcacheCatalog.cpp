/// @file    plugins/memcache/MemcacheCatalog.cpp
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
/// @author  Andrea Manzi <amanzi@cern.ch>

#include <algorithm>
#include <libgen.h>
#include <cstring>
#include <set>

#include <dmlite/cpp/utils/checksums.h>

#include "MemcacheCatalog.h"
#include "MemcacheFunctions.h"

using namespace dmlite;



MemcacheCatalog::MemcacheCatalog(PoolContainer<memcached_st*>& connPool,
    Catalog* decorates,
    MemcacheFunctionCounter* funcCounter,
    bool doFuncCount,
    unsigned int symLinkLimit,
    time_t memcachedExpirationLimit,
    bool memcachedPOSIX)
throw (DmException):
  MemcacheCommon(connPool, funcCounter, doFuncCount, memcachedExpirationLimit),
  si_(0x00),
  symLinkLimit_(symLinkLimit),
  memcachedPOSIX_(memcachedPOSIX)
{
  //this->conn_           = conn;

  //this->connNoReply_    = connPool_->acquire();

  // Don't worry about the return. If it's not successful,
  // it will just be slightly slower. No reason to fail.
  //memcached_behavior_set(this->connNoReply_, MEMCACHED_BEHAVIOR_NOREPLY, 1);

  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "MemcacheCatalog started.");

  this->decorated_   = decorates;
  this->decoratedId_ = strdup( decorates->getImplId().c_str() );
}


MemcacheCatalog::~MemcacheCatalog()
{
  //this->connPool_->release(this->conn_);
  //this->connPool_->release(this->connNoReply_);

  if (this->decorated_ != 0x00)
    delete this->decorated_;

  if (this->decoratedId_ != 0x00)
    free(this->decoratedId_);
}


std::string MemcacheCatalog::getImplId() const throw ()
{
  std::string implId = "MemcacheCatalog";
  implId += " over ";
  implId += std::string(this->decoratedId_);

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
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);

  incrementFunctionCounter(CHANGEDIR);

  if (path.empty()) {
    this->cwd_.clear();
  }

  ExtendedStat meta;
  DmStatus st = this->extendedStatPOSIX(meta, path, true);
  if(!st.ok()) throw st.exception();
  std::string normPath = meta.getString("normPath");

  if (normPath[0] == '/')
    this->cwd_ = normPath;
  else
    this->cwd_ = Url::normalizePath(this->cwd_ + "/" + normPath, false);

  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}


std::string MemcacheCatalog::getWorkingDir(void) throw (DmException)
{
  incrementFunctionCounter(GETWORKINGDIR);
  return this->cwd_;
}

DmStatus MemcacheCatalog::extendedStat(ExtendedStat &xstat, const std::string &path, bool followSym) throw (DmException) {
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path << ". No exit log msg.");
  if (this->memcachedPOSIX_) {
    DmStatus st = this->extendedStatSimplePOSIX(xstat, path, followSym);
    if(!st.ok()) {
      Log(Logger::Lvl1, Logger::unregistered, "extendedStat",   " Could not stat '" << path << "'");
    }
    return st;
  }
  else {
    DmStatus st = this->extendedStatNoPOSIX(xstat, path, followSym);
    if(!st.ok()) {
      Log(Logger::Lvl1, Logger::unregistered, "extendedStat",   " Could not stat '" << path << "'");
    }
    return st;
  }

  return DmStatus();
}

ExtendedStat MemcacheCatalog::extendedStat(const std::string& path, bool followSym) throw (DmException)
{
  ExtendedStat ret;
  DmStatus st = this->extendedStat(ret, path, followSym);
  if(!st.ok()) throw st.exception();
  return ret;
}

DmStatus MemcacheCatalog::extendedStatSimplePOSIX(ExtendedStat &xstat, const std::string& path, bool followSym) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
  xstat = ExtendedStat();

  // includes cwd, if applicable
  std::string absPath = getAbsolutePath(path);

  // fall back to slower implementation, if relative movement
  if (absPath.find("/./") != std::string::npos || absPath.find("/../") != std::string::npos) {
    return extendedStatPOSIX(xstat, path, followSym);
  }

  size_t endCurrentPath = 1;

  DmStatus st = this->extendedStatNoCheck(xstat, "/", followSym);
  if(!st.ok()) return st;
  endCurrentPath = absPath.find('/', 1);

  while (endCurrentPath != std::string::npos) {
    // Check that the parent is a directory first
    if (!S_ISDIR(xstat.stat.st_mode) && !S_ISLNK(xstat.stat.st_mode))
      return DmStatus(ENOTDIR, xstat.name + " is not a directory");
    // New element traversed! Need to check if it is possible to keep going.
    if (checkPermissions(this->secCtx_, xstat.acl, xstat.stat, S_IEXEC) != 0)
      return DmStatus(EACCES, "Not enough permissions to list " + xstat.name);

    std::string currentPath = absPath.substr(0, endCurrentPath);
    // Stat, this throws an Exception if the path doesn't exist
    DmStatus st = this->extendedStatNoCheck(xstat, currentPath, followSym);
    if(!st.ok()) return st;
    // fall back to slower implementation, if link
    if (S_ISLNK(xstat.stat.st_mode)) {
      return extendedStatPOSIX(xstat, path, followSym);
    }
    // include the next level
    endCurrentPath = absPath.find('/', endCurrentPath+1);
  }
  st = this->extendedStatNoCheck(xstat, absPath, followSym);
  if(!st.ok()) return st;
  xstat["normPath"] = absPath;

  checksums::fillChecksumInXattr(xstat);
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
  return DmStatus();
}



DmStatus MemcacheCatalog::extendedStatPOSIX(ExtendedStat &meta, const std::string& path, bool followSym) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
  meta = ExtendedStat();

  std::string currentPathElem;

  std::vector<std::string> components;
  std::vector<std::string> cwdComponents;

  std::string cwd = this->cwd_;

  unsigned int symLinkLevel = 0;
  unsigned int cwdMarker = 0;

  components = Url::splitPath(path);
  if (path[0] == '/' || cwd.empty()) {
    DmStatus st = this->extendedStatNoCheck(meta, "/", followSym);
    if(!st.ok()) return st;
  } else {
    cwdComponents = Url::splitPath(cwd);
    DmStatus st = this->extendedStatNoCheck(meta, cwd, followSym);
    if(!st.ok()) return st;
  }

  for (; cwdMarker < components.size(); ) {
    // Check that the parent is a directory first
    if (!S_ISDIR(meta.stat.st_mode) && !S_ISLNK(meta.stat.st_mode))
      return DmStatus(ENOTDIR, meta.name + " is not a directory");
    // New element traversed! Need to check if it is possible to keep going.
    if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IEXEC) != 0)
      return DmStatus(EACCES, "Not enough permissions to list " + meta.name);

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
      if (cwd.empty()) cwd = "/";
      // Stat, this throws an Exception if the path doesn't exist
      DmStatus st = this->extendedStatNoCheck(meta, cwd, followSym);
      if(!st.ok()) return st;

      // Symbolic link!, follow that instead
      if (S_ISLNK(meta.stat.st_mode) && followSym) {
        std::string link = this->readLink(cwd);

        ++symLinkLevel;
        if (symLinkLevel > this->symLinkLimit_) {
          return DmStatus(DMLITE_SYSERR(ELOOP),
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
          DmStatus st = this->extendedStatNoCheck(meta, "/", followSym);
          if(!st.ok()) return st;
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

  checksums::fillChecksumInXattr(meta);
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
  return DmStatus();
}


DmStatus MemcacheCatalog::extendedStatNoPOSIX(ExtendedStat &xstat, const std::string& path, bool followSym) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
  incrementFunctionCounter(EXTENDEDSTAT);

  xstat = ExtendedStat();

  std::string valMemc;

  std::string absPath = getAbsolutePath(path);
  const std::string key = keyFromString(key_prefix[PRE_STAT], absPath);

  valMemc = safeGetValFromMemcachedKey(key);
  if (!valMemc.empty()) {
    deserializeExtendedStat(valMemc, xstat);
  } else // valMemc was not in memcached
  {
    incrementFunctionCounter(EXTENDEDSTAT_DELEGATE);
    DmStatus st;
    DELEGATE_ASSIGN(st, extendedStat, xstat, absPath, followSym);
    if(!st.ok()) return st;

    //only if the size is > 0 we cache the stat, this is needed to fix some problem with third party copies.( we  cache also empty folders)
    if (xstat.stat.st_size == 0 && !S_ISDIR(xstat.stat.st_mode)) {}
    else {
          serializeExtendedStat(xstat, valMemc);
          safeSetMemcachedFromKeyValue(key, valMemc);
         }
  }
  xstat["normPath"] = absPath;

  checksums::fillChecksumInXattr(xstat);
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
  return DmStatus();
}


DmStatus MemcacheCatalog::extendedStatNoCheck(ExtendedStat &xstat, const std::string& absPath, bool followSym) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << absPath);
  incrementFunctionCounter(EXTENDEDSTAT);

  xstat = ExtendedStat();

  std::string valMemc;

  const std::string key = keyFromString(key_prefix[PRE_STAT], absPath);

  valMemc = safeGetValFromMemcachedKey(key);
  if (!valMemc.empty()) {
    deserializeExtendedStat(valMemc, xstat);
  } else // valMemc was not in memcached
  {
    incrementFunctionCounter(EXTENDEDSTAT_DELEGATE);
    DmStatus st;
    DELEGATE_ASSIGN(st, extendedStat, xstat, absPath, followSym);
    if(!st.ok()) return st;

    //only if the size is > 0 we cache the stat, this is needed to fix some problem with third party copies.( we  cache also empty folders)
    if (xstat.stat.st_size == 0 && !S_ISDIR(xstat.stat.st_mode)) {}
    else {
           serializeExtendedStat(xstat, valMemc);

           // Let's write directories into the fast local cache
           if ((localCacheMaxSize > 0) && S_ISDIR(xstat.stat.st_mode)) {
             setLocalFromKeyValue(key, valMemc);
           }

           safeSetMemcachedFromKeyValue(key, valMemc);
          }
   }

  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
  return DmStatus();
}

ExtendedStat MemcacheCatalog::extendedStatByRFN(const std::string& rfn) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, rfn = " << rfn);
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
   //only if the size is > 0 we cache the stat, this is needed to fix some problem with third party copies.( we  cache also empty folders)
   if (meta.stat.st_size == 0 && !S_ISDIR(meta.stat.st_mode)) {}
   else {
  	serializeExtendedStat(meta, valMemc);
   	safeSetMemcachedFromKeyValue(key, valMemc);
    }
  }

  checksums::fillChecksumInXattr(meta);
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");

  return meta;
}


bool MemcacheCatalog::access(const std::string& path, int mode) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
  // Copy from BuiltinCatalog to use the memcache extendedStat method
  try {
    ExtendedStat xstat = this->extendedStat(path);

    mode_t perm = 0;
    if (mode & R_OK) perm  = S_IREAD;
    if (mode & W_OK) perm |= S_IWRITE;
    if (mode & X_OK) perm |= S_IEXEC;

    Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
    return checkPermissions(this->secCtx_, xstat.acl, xstat.stat, perm) == 0;
  }
  catch (DmException& e) {
    if (e.code() != EACCES) throw;
    return false;
  }
}


bool MemcacheCatalog::accessReplica(const std::string& replica, int mode) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, replica = " << replica);
  incrementFunctionCounter(ACCESSREPLICA_DELEGATE);
  DELEGATE_RETURN(accessReplica, replica, mode);
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}


void MemcacheCatalog::addReplica(const Replica& replica) throw (DmException)
{
  std::string valMemc;
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering.");
  incrementFunctionCounter(ADDREPLICA_DELEGATE);
  DELEGATE(addReplica, replica);
  //I need to get the replica ID;
  Replica newreplica;
  DELEGATE_ASSIGN(newreplica, getReplicaByRFN, replica.rfn);

  if (replica.status == dmlite::Replica::kAvailable) {
    valMemc = serializeReplica(newreplica);
    safeSetMemcachedFromKeyValue(keyFromString(key_prefix[PRE_REPL],newreplica.rfn),valMemc);
  }

  //invalidating replica list
  std::string filepath = getFullPathByRFN(newreplica.rfn);
  filepath = getAbsolutePath(filepath);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_REPL_LIST], filepath));

  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}


void MemcacheCatalog::deleteReplica(const Replica& replica) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering.");
  incrementFunctionCounter(DELETEREPLICA_DELEGATE);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_REPL], replica.rfn));
  //invalidating replica list
  std::string filepath = getFullPathByRFN(replica.rfn);
  filepath = getAbsolutePath(filepath);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_REPL_LIST], filepath));
  DELEGATE(deleteReplica, replica);
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}


std::vector<Replica> MemcacheCatalog::getReplicas(const std::string& path) throw(DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
  incrementFunctionCounter(GETREPLICAS);

  std::vector<Replica> replicas;
  std::string valMemc;
  Replica repl;

  // get replica list from memcached
  std::string absPath = getAbsolutePath(path);
  const std::string key = keyFromString(key_prefix[PRE_REPL_LIST], absPath);

  valMemc = safeGetValFromMemcachedKey(key);

  if (!valMemc.empty()) {
    deserializeReplicaList(valMemc, replicas);
  }
  if (replicas.size() == 0) {
    // otherwise, get replicas from mysql
    incrementFunctionCounter(GETREPLICAS_DELEGATE);
    DELEGATE_ASSIGN(replicas, getReplicas, absPath);

    valMemc = serializeReplicaList(replicas);
    if (valMemc.length() > 0) safeSetMemcachedFromKeyValue(key, valMemc);
  }

  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
  return replicas;
}


void MemcacheCatalog::symlink(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, oldpath = " << oldPath
      << " newpath = " << newPath);
  std::string absOldPath = getAbsolutePath(oldPath);
  std::string absNewPath = getAbsolutePath(newPath);
  incrementFunctionCounter(SYMLINK_DELEGATE);
  DELEGATE(symlink, absOldPath, absNewPath);
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}


std::string MemcacheCatalog::readLink(const std::string& path) throw(DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
  std::string absPath = getAbsolutePath(path);
  incrementFunctionCounter(READLINK_DELEGATE);
  DELEGATE_RETURN(readLink, absPath);
}


void MemcacheCatalog::unlink(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
  incrementFunctionCounter(UNLINK_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  std::string basepath = getBasePath(absPath);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_REPL_LIST], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], basepath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR_LIST], basepath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], basepath));
  DELEGATE(unlink, absPath);
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}


void MemcacheCatalog::create(const std::string& path, mode_t mode) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
  incrementFunctionCounter(CREATE_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  std::string basepath = getBasePath(absPath);
  DELEGATE(create, absPath, mode);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], basepath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR_LIST], basepath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], basepath));
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}


mode_t MemcacheCatalog::umask(mode_t mask) throw ()
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering.");
  incrementFunctionCounter(UMASK_DELEGATE);
  DELEGATE_RETURN(umask, mask);
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}


void MemcacheCatalog::setMode(const std::string& path, mode_t mode)
   throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
  incrementFunctionCounter(SETMODE_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  DELEGATE(setMode, absPath, mode);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR_LIST], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], absPath));
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}


void MemcacheCatalog::setOwner(const std::string& path,
                               uid_t newUid, gid_t newGid,
                               bool followSymLink)
  throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
  incrementFunctionCounter(SETOWNER_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  DELEGATE(setOwner, absPath, newUid, newGid);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR_LIST], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], absPath));
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}


void MemcacheCatalog::setSize(const std::string& path,
                                  size_t newSize)
  throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
  incrementFunctionCounter(SETSIZE_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  DELEGATE(setSize, absPath, newSize);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}


void MemcacheCatalog::setChecksum(const std::string& path,
                                  const std::string& csumtype,
                                  const std::string& csumvalue)
  throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
  incrementFunctionCounter(SETCHECKSUM_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  DELEGATE(setChecksum, absPath, csumtype, csumvalue);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}

void MemcacheCatalog::getChecksum(const std::string& path,
                       const std::string& csumtype,
                       std::string& csumvalue,
                       const std::string& pfn,
                       const bool forcerecalc,
                       const int waitsecs) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
  incrementFunctionCounter(GETCHECKSUM_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  DELEGATE(getChecksum, absPath, csumtype, csumvalue, pfn, forcerecalc, waitsecs);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}


void MemcacheCatalog::setAcl(const std::string& path, const Acl& acl) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
  incrementFunctionCounter(SETACL_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  DELEGATE(setAcl, absPath, acl);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], absPath));
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}


void MemcacheCatalog::utime(const std::string& path, const struct utimbuf* buf) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
  incrementFunctionCounter(UTIME_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  DELEGATE(utime, absPath, buf);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], absPath));
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}


std::string MemcacheCatalog::getComment(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
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
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
  return comment;
}


void MemcacheCatalog::setComment(const std::string& path, const std::string& comment) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
  incrementFunctionCounter(SETCOMMENT_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  DELEGATE(setComment, absPath, comment);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_COMMENT], absPath));
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}


void MemcacheCatalog::setGuid(const std::string& path, const std::string& guid) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
  incrementFunctionCounter(SETGUID_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  DELEGATE(setGuid, absPath, guid);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], absPath));
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}


void MemcacheCatalog::updateExtendedAttributes(const std::string& path,
                              const Extensible& attr) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
  incrementFunctionCounter(UPDATEEXTENDEDATTRIBUTES_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  DELEGATE(updateExtendedAttributes, absPath, attr);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], absPath));
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}


Directory* MemcacheCatalog::openDir(const std::string& path) throw(DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
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
  dirp->has_called_opendir = false;

  // get replica list from memcached
  const std::string key = keyFromString(key_prefix[PRE_DIR_LIST], dirp->basepath);

  const std::string valMemc = safeGetValFromMemcachedKey(key);

  if (!valMemc.empty()) {
    // ParseFromString will set the state for us
    dirp->pb_keys.ParseFromString(valMemc);
    dirp->pb_keys_idx = 0;
  }
  // also check the state of the value in memcached
  if (dirp->pb_keys.state() != VALID) {
    // avoid a race condition creating directories.
    // only one memcached_add can succeed
    const std::string dirkey = keyFromString(key_prefix[PRE_DIR], dirp->basepath);
    try {
      addMemcachedFromKeyValue(dirkey, "CANBEANYTHING");
      dirp->pb_keys.set_state(MISSING);
    } catch (MemcacheException) {
      Log(Logger::Lvl3, memcachelogmask, memcachelogname,
          "conflict when caching dir object: " <<
          "caching is already in process");
      dirp->pb_keys.set_state(INVALID);
    }
    incrementFunctionCounter(OPENDIR_DELEGATE);
    try {
      DELEGATE_ASSIGN(dirp->decorated_dirp, openDir, dirp->basepath);
      dirp->has_called_opendir = true;
    } catch (...) {
      Err(memcachelogname, "delegation of openDir failed");
      delete dirp;
      throw;
    }
  }

  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
  return dirp;
}


void MemcacheCatalog::closeDir(Directory* dir) throw(DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering.");
  incrementFunctionCounter(CLOSEDIR);

  MemcacheDir *dirp = dynamic_cast<MemcacheDir *>(dir);
  if (dirp->has_called_opendir) {
    incrementFunctionCounter(CLOSEDIR_DELEGATE);
    DELEGATE(closeDir, dirp->decorated_dirp);
  }
  delete dirp;
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
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

  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, dir base path = " << dirp->basepath
      << " dir name = " << dirp->dir.name);

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
    dirp->pb_keys.set_state(VALID);
    std::string valMemc = dirp->pb_keys.SerializeAsString();
    std::string listkey = keyFromString(key_prefix[PRE_DIR_LIST], dirp->basepath);
    safeSetMemcachedFromKeyValue(listkey, valMemc);
  }

  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");

  if (pMeta == 0x00)
    return 0x00;
  else {
    checksums::fillChecksumInXattr(*pMeta);
    return pMeta;
  }
}


ExtendedStat* MemcacheCatalog::delegateReadDirxAndAddEntryToCache(MemcacheDir *dirp) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, dir base path = " << dirp->basepath
      << " dir name = " << dirp->dir.name);
  ExtendedStat* pMeta = &(dirp->dir);

  DELEGATE_ASSIGN(pMeta, readDirx, dirp->decorated_dirp);
  if (pMeta != 0x00) {
    // safety net not to store large elements in memcached
    // computes the serialized size of the message, it can still
    // grow large in memory. use .SpaceUsed() to test that.
    if (dirp->pb_keys.ByteSize() > (1 << 20)) {
      Log(Logger::Lvl4, memcachelogmask, memcachelogname,
          "dir size to large to cache: " << dirp->pb_keys.ByteSize());
      dirp->pb_keys.set_state(INVALID);
    }

    SerialKey *pntKey = dirp->pb_keys.add_key();
    pntKey->set_key(pMeta->name);
  }

  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
  return pMeta;
}


ExtendedStat* MemcacheCatalog::getDirEntryFromCache(MemcacheDir *dirp) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, dir base path = " << dirp->basepath
      << " dir name = " << dirp->dir.name);
  ExtendedStat *pMeta = &(dirp->dir);

  if (dirp->pb_keys.key_size() > dirp->pb_keys_idx) {
    std::string valMemc;
    std::string absPath =
      concatPath(dirp->basepath,
                 dirp->pb_keys.key(dirp->pb_keys_idx).key());
    dirp->pb_keys_idx++;

    std::string key = keyFromString(key_prefix[PRE_STAT], absPath);

    valMemc = safeGetValFromMemcachedKey(key);

    if (!valMemc.empty()) {
      deserializeExtendedStat(valMemc, *pMeta);
    } else {
      try {
        DELEGATE_ASSIGN(*pMeta, extendedStat, absPath, true);
      } catch (DmException& e) {
        if (e.code() != ENOENT)
          throw;
        else
          return getDirEntryFromCache(dirp);
      }
      serializeExtendedStat(*pMeta, valMemc);
      safeSetMemcachedFromKeyValue(key, valMemc);
    }
  } else {
    return 0x00;
  }

  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
  return pMeta;
}


void MemcacheCatalog::makeDir(const std::string& path, mode_t mode) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
  incrementFunctionCounter(MAKEDIR_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  std::string basepath = getBasePath(absPath);
  DELEGATE(makeDir, absPath, mode);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], basepath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR_LIST], basepath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], basepath));
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}


void MemcacheCatalog::rename(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, oldpath = " << oldPath
      << " newpath = " << newPath);
  incrementFunctionCounter(RENAME_DELEGATE);
  std::string absOldPath = getAbsolutePath(oldPath);
  std::string oldbasepath = getBasePath(absOldPath);
  std::string absNewPath = getAbsolutePath(newPath);
  std::string newbasepath = getBasePath(absNewPath);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absOldPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], absOldPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR_LIST], absOldPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_REPL_LIST], absOldPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], oldbasepath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], oldbasepath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR_LIST], oldbasepath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], newbasepath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], newbasepath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR_LIST], newbasepath));
  DELEGATE(rename, absOldPath, absNewPath);
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}


void MemcacheCatalog::removeDir(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, path = " << path);
  incrementFunctionCounter(REMOVEDIR_DELEGATE);
  std::string absPath = getAbsolutePath(path);
  std::string basepath = getBasePath(absPath);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR_LIST], absPath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_STAT], basepath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR], basepath));
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_DIR_LIST], basepath));
  DELEGATE(removeDir, absPath);
  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}


Replica MemcacheCatalog::getReplicaByRFN(const std::string& rfn) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering, rfn = " << rfn);
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

    if (replica.status == dmlite::Replica::kAvailable) {
      valMemc = serializeReplica(replica);
      safeSetMemcachedFromKeyValue(key, valMemc);
    }

  }

  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
  return replica;
}


void MemcacheCatalog::updateReplica(const Replica& replica) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering.");
  incrementFunctionCounter(UPDATEREPLICA_DELEGATE);
  DELEGATE(updateReplica, replica);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_REPL], replica.rfn));
  //invalidating replica list
  std::string filepath = getFullPathByRFN(replica.rfn);
  filepath = getAbsolutePath(filepath);
  safeDelMemcachedFromKey(keyFromString(key_prefix[PRE_REPL_LIST], filepath));

  Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
}

std::string MemcacheCatalog::getFullPathByRFN(const std::string& rfn) throw (DmException)
{
  Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Entering.");
  ExtendedStat stat;
  std::vector<std::string> paths;
  std::string filename;
  stat = this->extendedStatByRFN(rfn);
  filename = stat.name;

  while (stat.parent != 0) {
      stat = this->si_->getINode()->extendedStat(stat.parent);
      paths.push_back(stat.name);
  }

 std::string finalPath;

 while( paths.size() > 0){
    Log(Logger::Lvl4, memcachelogmask, memcachelogname, paths.back());
    finalPath.append(paths.back());
    paths.pop_back();
    finalPath.append("/");
 }
 finalPath.append(filename);
 Log(Logger::Lvl4, memcachelogmask, memcachelogname, "Full Path:" << finalPath);
 Log(Logger::Lvl3, memcachelogmask, memcachelogname, "Exiting.");
 return finalPath.substr(1,finalPath.length());
}


/* internal functions */



inline void MemcacheCatalog::incrementFunctionCounter(const int funcName)
{
  if (this->funcCounter_ != 0x00) this->funcCounter_->incr(funcName, &this->randomSeed_);
}
