/// @file     core/builtin/Catalog.cpp
/// @brief    Implementation of a Catalog using other plugins, as INode.
/// @detailed Intended to ease the development of database backends.
/// @author   Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <list>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/utils/dm_security.h>
#include <dmlite/cpp/utils/dm_urls.h>

#include "Catalog.h"

using namespace dmlite;


BuiltInCatalogFactory::BuiltInCatalogFactory():
  updateATime_(true), symLinkLimit_(3)
{
  // Nothing
}



BuiltInCatalogFactory::~BuiltInCatalogFactory()
{
  
}
  


void BuiltInCatalogFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  if (key == "SymLinkLimit")
    this->symLinkLimit_ = atoi(value.c_str());
  else if (key == "UpdateAccessTime") {
    std::string lower;
    std::transform(value.begin(), value.end(), lower.begin(), tolower);
    this->updateATime_ = (value == "yes");
  }
  else
    throw DmException(DM_UNKNOWN_OPTION, std::string("Unknown option ") + key);
}



Catalog* BuiltInCatalogFactory::createCatalog(PluginManager*) throw (DmException)
{
  return new BuiltInCatalog(this->updateATime_, this->symLinkLimit_);
}



BuiltInCatalog::BuiltInCatalog(bool updateATime, unsigned symLinkLimit) throw (DmException):
    si_(0x00), secCtx_(), cwd_(0), umask_(022), updateATime_(updateATime), symLinkLimit_(symLinkLimit)
{
  // Nothing
}



BuiltInCatalog::~BuiltInCatalog()
{
  // Nothing
}



std::string BuiltInCatalog::getImplId(void) throw()
{
  return std::string("BuiltInCatalog");
}



void BuiltInCatalog::setStackInstance(StackInstance* si) throw (DmException)
{
  this->si_ = si;
}



void BuiltInCatalog::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  this->secCtx_ = ctx;
}



void BuiltInCatalog::changeDir(const std::string& path) throw (DmException)
{
  ExtendedStat cwd = this->extendedStat(path);
  this->cwdPath_   = path;
  this->cwd_       = cwd.stat.st_ino;
}



std::string BuiltInCatalog::getWorkingDir (void) throw (DmException)
{
  return this->cwdPath_;
}



ino_t BuiltInCatalog::getWorkingDirI(void) throw (DmException)
{
  return this->cwd_;
}



ExtendedStat BuiltInCatalog::extendedStat(const std::string& path, bool followSym) throw (DmException)
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
    meta   = this->si_->getINode()->extendedStat(parent);
  }
  

  while (!components.empty()) {
    // Check that the parent is a directory first
    if (!S_ISDIR(meta.stat.st_mode) && !S_ISLNK(meta.stat.st_mode))
      throw DmException(DM_NOT_DIRECTORY, "%s is not a directory", meta.name);
    // New element traversed! Need to check if it is possible to keep going.
    if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IEXEC) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to list %s", meta.name);

    // Pop next component
    c = components.front();
    components.pop_front();

    // Stay here
    if (c == ".") {
      continue;
    }
    // Up one level
    else if (c == "..") {
      meta   = this->si_->getINode()->extendedStat(parent);
      parent = meta.parent;
    }
    // Regular entry
    else {
      meta = this->si_->getINode()->extendedStat(parent, c);

      // Symbolic link!, follow that instead
      if (S_ISLNK(meta.stat.st_mode) && followSym) {
        SymLink link = this->si_->getINode()->readLink(meta.stat.st_ino);

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



void BuiltInCatalog::addReplica(const std::string& guid, int64_t id, const std::string& server,
                                const std::string& sfn, char status, char fileType,
                                const std::string& poolName, const std::string& fileSystem) throw (DmException)
{
  ExtendedStat meta;
  
  if (guid.empty())
    meta = this->si_->getINode()->extendedStat(id);
  else
    meta = this->si_->getINode()->extendedStat(guid);
  
  this->traverseBackwards(meta);
  
  this->si_->getINode()->addReplica(meta.stat.st_ino, server, sfn, status, fileType, poolName, fileSystem);
}



void BuiltInCatalog::deleteReplica(const std::string& guid, int64_t id,
                                   const std::string& sfn) throw (DmException)
{
  ExtendedStat meta;
  
  if (guid.empty())
    meta = this->si_->getINode()->extendedStat(id);
  else
    meta = this->si_->getINode()->extendedStat(guid);
  
  this->traverseBackwards(meta);
  this->si_->getINode()->deleteReplica(meta.stat.st_ino, sfn);
}



std::vector<FileReplica> BuiltInCatalog::getReplicas(const std::string& path) throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path);
  
  // The file exists, plus we have permissions to go there. Check we can read
  if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN,
                   "Not enough permissions to read " + path);

  this->updateAccessTime(meta);
  return this->si_->getINode()->getReplicas(meta.stat.st_ino);
}



Location BuiltInCatalog::get(const std::string& path) throw (DmException)
{
  unsigned i;
  std::vector<FileReplica> replicas = this->getReplicas(path);
  std::vector<Location>    available;
  
  if (replicas.size() == 0)
    throw DmException(DM_NO_REPLICAS, "No replicas");

  // Ask the pool manager to see if available, and translate
  if (this->si_->isTherePoolManager()) {
    for (i = 0; i < replicas.size(); ++i) {
      Pool pool = this->si_->getPoolManager()->getPool(replicas[i].pool);
      PoolHandler* handler = this->si_->getPoolDriver(pool.pool_type)->createPoolHandler(pool.pool_name);
      
      Location location = handler->getLocation(replicas[i]);

      if (location.available) {
        available.push_back(location);
      }
      
      delete handler;
    }
  }
  // If no pool manager, make sure we can guess the whole location (LFC)
  else {
    for (i = 0; i < replicas.size(); ++i) {
      Url url = dmlite::splitUrl(replicas[i].rfn);
      if (url.host[0] != '\0')
        available.push_back(url);
    }
  }
  
  // Pick a random one from the available
  if (available.size() > 0) {
    i = rand() % available.size();
    return available[i];
  }
  else {
    throw DmException(DM_NO_REPLICAS, "No available replicas");
  }
}



Location BuiltInCatalog::put(const std::string& path) throw (DmException)
{
  // Get the available pool list
  if (this->si_->isTherePoolManager() == 0x00)
    throw DmException(DM_NO_POOL_MANAGER, "Can not put if no PoolManager is loaded");
    
  std::vector<Pool> pools = this->si_->getPoolManager()->getAvailablePools();
  
  // Pick a random one
  unsigned i = rand()  % pools.size();
  
  // Get the handler
  PoolHandler* handler = this->si_->getPoolDriver(pools[i].pool_type)->createPoolHandler(pools[i].pool_name);
  
  // Create the entry
  this->create(path, 0777);
  
  // Delegate to it
  Location loc = handler->putLocation(path);
  delete handler;
  
  // Done!
  return loc;
}



void BuiltInCatalog::putDone(const std::string& host, const std::string& rfn,
                             const std::map<std::string, std::string>& extras) throw (DmException)
{
  // Get the available pool list
  if (this->si_->isTherePoolManager() == 0x00)
    throw DmException(DM_NO_POOL_MANAGER, "Can not put if no PoolManager is loaded");
  
  FileReplica replica;
  
  // Try to fetch using host
  try {
    replica = this->si_->getINode()->getReplica(host + ":" + rfn);
  }
  catch (DmException& e) {
    if (e.code() != DM_NO_SUCH_REPLICA)
      throw;
    // Try without
    replica = this->si_->getINode()->getReplica(rfn);
  }
  
  // Get the driver and delegate
  Pool pool            = this->si_->getPoolManager()->getPool(replica.pool);
  PoolHandler* handler = this->si_->getPoolDriver(pool.pool_type)->createPoolHandler(pool.pool_name);
 
  handler->putDone(replica, extras);
  
  delete handler;
}



void BuiltInCatalog::symlink(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
  std::string parentPath, symName;
  
  // Get the parent of the destination and file
  ExtendedStat parent = this->getParent(newPath, &parentPath, &symName);

  // Check we have write access for the parent
  if (checkPermissions(this->secCtx_, parent.acl, parent.stat, S_IWRITE | S_IEXEC) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions on " + parentPath);

  // Effective gid
  gid_t  egid;
  mode_t mode = 0777;
  
  if (parent.stat.st_mode & S_ISGID) {
    egid = parent.stat.st_gid;
    mode |= S_ISGID;
  }
  else {
    egid = this->secCtx_->getGroup(0).gid;
  }
  
  this->si_->getINode()->begin();
  
  try {
    // Create file
    ExtendedStat linkMeta = this->si_->getINode()->create(parent.stat.st_ino, symName,
                                                 this->secCtx_->getUser().uid,
                                                 egid, mode | S_IFLNK,
                                                 0, 0, '-', "", "", "");
    // Create symlink
    this->si_->getINode()->symlink(linkMeta.stat.st_ino, oldPath);
  }
  catch (...) {
    this->si_->getINode()->rollback();
    throw;
  }
  
  this->si_->getINode()->commit();
}



void BuiltInCatalog::unlink(const std::string& path) throw (DmException)
{
  std::string  parentPath, name;

  // Get the parent
  ExtendedStat parent = this->getParent(path, &parentPath, &name);

  // Check we have exec access for the parent
  if (checkPermissions(this->secCtx_, parent.acl, parent.stat, S_IEXEC) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to list " + parentPath);

  // The file itself
  ExtendedStat file = this->si_->getINode()->extendedStat(parent.stat.st_ino, name);

  // Directories can not be removed with this method!
  if (S_ISDIR(file.stat.st_mode))
    throw DmException(DM_IS_DIRECTORY, path + " is a directory, can not unlink");

  // Check we can remove it
  if ((parent.stat.st_mode & S_ISVTX) == S_ISVTX) {
    // Sticky bit set
    if (this->secCtx_->getUser().uid != file.stat.st_uid &&
        this->secCtx_->getUser().uid != parent.stat.st_uid &&
        checkPermissions(this->secCtx_, file.acl, file.stat, S_IWRITE) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to unlink " +
                                      path + "( sticky bit set)");
  }
  else {
    // No sticky bit
    if (checkPermissions(this->secCtx_, parent.acl, parent.stat, S_IWRITE) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to unlink " + path);
  }

  // Check there are no replicas
  if (!S_ISLNK(file.stat.st_mode)) {
    std::vector<FileReplica> replicas = this->si_->getINode()->getReplicas(file.stat.st_ino);
    
    // Pure catalogs must not remove files with replicas    
    if (!this->si_->isTherePoolManager() && replicas.size() != 0)
      throw DmException(DM_EXISTS, path + " has replicas, can not remove");
    
    // Try to remove replicas first
    for (unsigned i = 0; i < replicas.size(); ++i) {
      Pool         pool   = this->si_->getPoolManager()->getPool(replicas[i].pool);
      PoolHandler* handler = this->si_->getPoolDriver(pool.pool_type)->createPoolHandler(pool.pool_name);
      
      handler->remove(replicas[i]);
      
      delete handler;
    }
  }
  
  // All preconditions are good, so remove
  try {
    this->si_->getINode()->unlink(file.stat.st_ino);
  }
  catch (DmException& e) {
    if (e.code() != DM_NO_SUCH_FILE)
      throw;
    // If not found, that's good, as the pool driver probably
    // did it (i.e. legacy DPM)
  }
}



void BuiltInCatalog::create(const std::string& path, mode_t mode) throw (DmException)
{
  int          code;
  std::string  parentPath, name;
  ExtendedStat parent = this->getParent(path, &parentPath, &name);
  ExtendedStat file;
  
  // Need to be able to write to the parent
  if (checkPermissions(this->secCtx_, parent.acl, parent.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Need write access on the parent");

  // Check that the file does not exist, or it has no replicas
  try {
    file = this->si_->getINode()->extendedStat(parent.stat.st_ino, name);
    if (this->si_->getINode()->getReplicas(file.stat.st_ino).size() > 0)
      throw DmException(DM_EXISTS, path + " exists and has replicas. Can not truncate.");
  }
  catch (DmException& e) {
    code = e.code();
    if (code != DM_NO_SUCH_FILE)
      throw;
  }
  
  // Effective gid
  gid_t egid;
  if (parent.stat.st_mode & S_ISGID) {
    egid = parent.stat.st_gid;
    mode |= S_ISGID;
  }
  else {
    egid = this->secCtx_->getGroup(0).gid;
  }

  // Create new
  if (code == DM_NO_SUCH_FILE) {
    mode_t newMode = ((mode & ~S_IFMT) & ~this->umask_) | S_IFREG;

    // Generate inherited ACL's if there are defaults
    std::string aclStr;
    if (strchr (parent.acl, ACL_DEFAULT | ACL_USER_OBJ  | '@') != NULL) {
      aclStr = serializeAcl(inheritAcl(deserializeAcl(parent.acl),
                                       this->secCtx_->getUser().uid,
                                       this->secCtx_->getGroup(0).gid,
                                       &newMode, mode));
    }

    this->si_->getINode()->create(parent.stat.st_ino, name,
                         this->secCtx_->getUser().uid ,egid, newMode,
                         0, 0, '-', "", "", aclStr);
  }
  // Truncate
  else if (code == DM_NO_REPLICAS) {
    if (this->secCtx_->getUser().uid != file.stat.st_uid &&
        checkPermissions(this->secCtx_, file.acl, file.stat, S_IWRITE) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to truncate " + path);
    this->si_->getINode()->changeSize(file.stat.st_ino, 0);
  }
}



void BuiltInCatalog::makeDir(const std::string& path, mode_t mode) throw (DmException)
{
  std::string parentPath, name;

  // Get the parent of the new folder
  ExtendedStat parent = this->getParent(path, &parentPath, &name);

  // Check we have write access for the parent
  if (checkPermissions(this->secCtx_, parent.acl, parent.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Need write access for " + parentPath);

  // Mode
  mode_t newMode = ((mode & ~S_IFMT) & ~this->umask_) | S_IFDIR;
  
  // Effective gid
  gid_t egid;
  if (parent.stat.st_mode & S_ISGID) {
    egid = parent.stat.st_gid;
    newMode |= S_ISGID;
  }
  else {
    egid = this->secCtx_->getGroup(0).gid;
  }

  // Generate inherited ACL's if there are defaults
  std::string aclStr;
  if (strchr (parent.acl, ACL_DEFAULT | ACL_USER_OBJ  | '@') != NULL) {
    aclStr = serializeAcl(inheritAcl(deserializeAcl(parent.acl),
                                     this->secCtx_->getUser().uid,
                                     egid,
                                     &newMode, mode));
  }

  // Create the file entry
  this->si_->getINode()->create(parent.stat.st_ino, name,
                       this->secCtx_->getUser().uid, egid, newMode,
                       0, parent.type, '-', "", "", aclStr);
}



void BuiltInCatalog::removeDir(const std::string& path) throw (DmException)
{
  std::string parentPath, name;

  // Fail inmediately with '/'
  if (path == "/")
    throw DmException(DM_INVALID_VALUE, "Can not remove '/'");

  // Get the parent of the new folder
  ExtendedStat parent = this->getParent(path, &parentPath, &name);

  // Get the file, and check it is a directory and it is empty
  ExtendedStat entry = this->si_->getINode()->extendedStat(parent.stat.st_ino, name);

  if (!S_ISDIR(entry.stat.st_mode))
    throw DmException(DM_NOT_DIRECTORY, path + " is not a directory. Can not remove.");

  if (this->cwd_ == entry.stat.st_ino)
    throw DmException(DM_IS_CWD, "Can not remove the current working dir");

  if (entry.stat.st_nlink > 0)
    throw DmException(DM_EXISTS, path + " is not empty. Can not remove.");

  // Check we can remove it
  if ((parent.stat.st_mode & S_ISVTX) == S_ISVTX) {
    // Sticky bit set
    if (this->secCtx_->getUser().uid != entry.stat.st_uid &&
        this->secCtx_->getUser().uid != parent.stat.st_uid &&
        checkPermissions(this->secCtx_, entry.acl, entry.stat, S_IWRITE) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to remove " +
                                      path + "( sticky bit set)");
  }
  else {
    // No sticky bit
    if (checkPermissions(this->secCtx_, parent.acl, parent.stat, S_IWRITE) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to remove " + path);
  }

  // All preconditions are good!
  this->si_->getINode()->begin();
  
  try {
    // Remove associated comments
    this->si_->getINode()->deleteComment(entry.stat.st_ino);
    // Remove directory itself
    this->si_->getINode()->unlink(entry.stat.st_ino);
  }
  catch (...) {
    this->si_->getINode()->rollback();
    throw;
  }
  
  this->si_->getINode()->commit();
}



void BuiltInCatalog::rename(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
std::string oldParentPath, newParentPath;
  std::string oldName,       newName;

  // Do not even bother with '/'
  if (oldPath == "/" || newPath == "/")
    throw DmException(DM_INVALID_VALUE, "Not the source, neither the destination, can be '/'");

  // Get source and destination parent
  ExtendedStat oldParent = this->getParent(oldPath, &oldParentPath, &oldName);
  ExtendedStat newParent = this->getParent(newPath, &oldParentPath, &newName);

  // Source
  ExtendedStat old = this->si_->getINode()->extendedStat(oldParent.stat.st_ino, oldName);

  // Is the cwd?
  if (old.stat.st_ino == this->cwd_) {
    throw DmException(DM_IS_CWD, "Can not rename the current working directory");
  }

  // Need write permissions in both origin and destination
  if (checkPermissions(this->secCtx_, oldParent.acl, oldParent.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions on origin " + oldParentPath);

  if (checkPermissions(this->secCtx_, newParent.acl, newParent.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions on destination " + newParentPath);

  // If source is a directory, need write permissions there too
  if (S_ISDIR(old.stat.st_mode)) {
    if (checkPermissions(this->secCtx_, old.acl, old.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions on " + oldPath);

    // AND destination can not be a child
    ExtendedStat aux = newParent;

    while (aux.parent > 0) {
      if (aux.stat.st_ino == old.stat.st_ino)
        throw DmException(DM_INVALID_VALUE, "Destination is descendant of source");
      aux = this->si_->getINode()->extendedStat(aux.parent);
    }
  }

  // Check sticky
  if (oldParent.stat.st_mode & S_ISVTX &&
      this->secCtx_->getUser().uid != oldParent.stat.st_uid &&
      this->secCtx_->getUser().uid != old.stat.st_uid &&
      checkPermissions(this->secCtx_, old.acl, old.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Sticky bit set on the parent, and not enough permissions");

  // If the destination exists...
  try {
    ExtendedStat newF = this->si_->getINode()->extendedStat(newParent.stat.st_ino, newName);

    // If it is the same, leave the function
    if (newF.stat.st_ino == old.stat.st_ino)
      return;

    // It does! It has to be the same type
    if ((newF.stat.st_mode & S_IFMT) != (old.stat.st_mode & S_IFMT)) {
      if (S_ISDIR(old.stat.st_mode))
        throw DmException(DM_NOT_DIRECTORY, "Source is a directory and destination is not");
      else
        throw DmException(DM_IS_DIRECTORY, "Source is not directory and destination is");
    }

    // And it has to be empty. Just call remove or unlink
    // and they will fail if it is not
    if (S_ISDIR(newF.stat.st_mode))
      this->removeDir(newPath);
    else
      this->unlink(newPath);
  }
  catch (DmException& e) {
    if (e.code() != DM_NO_SUCH_FILE)
      throw;
  }

  // We are good, so we can move now
  this->si_->getINode()->begin();
  
  try {
    // Change the name if needed
    if (newName != oldName)
      this->si_->getINode()->rename(old.stat.st_ino, newName);

    // Change the parent if needed
    if (newParent.stat.st_ino != oldParent.stat.st_ino)
      this->si_->getINode()->move(old.stat.st_ino, newParent.stat.st_ino);
    else {
      // Parent is the same, but change its mtime
      struct utimbuf utim;
      utim.actime  = time(NULL);
      utim.modtime = utim.actime;
      this->si_->getINode()->utime(oldParent.stat.st_ino, &utim);
    }
  }
  catch (...) {
    this->si_->getINode()->rollback();
    throw;
  }

  // Done!
  this->si_->getINode()->commit();
}



mode_t BuiltInCatalog::umask(mode_t mask) throw ()
{
  mode_t prev = this->umask_;
  this->umask_ = mask;
  return prev;
}



void BuiltInCatalog::changeMode(const std::string& path, mode_t mode) throw (DmException)
{
ExtendedStat meta = this->extendedStat(path);

  // User has to be the owner, or root
  if (this->secCtx_->getUser().uid != meta.stat.st_uid &&
      this->secCtx_->getUser().uid != 0)
    throw DmException(DM_FORBIDDEN, "Only the owner can change the mode of " + path);

  // Clean up unwanted bits
  mode &= ~S_IFMT;
  if (!S_ISDIR(meta.stat.st_mode) && this->secCtx_->getUser().uid != 0)
    mode &= ~S_ISVTX;
  if (this->secCtx_->getUser().uid != 0 &&
      !this->secCtx_->hasGroup(meta.stat.st_gid))
    mode &= ~S_ISGID;

  // Update, keeping type bits from db.
  mode |= (meta.stat.st_mode & S_IFMT);

  // Update the ACL
  std::string aclStr;
  if (meta.acl[0] != '\0') {
    std::vector<Acl> acls = dmlite::deserializeAcl(meta.acl);
    for (size_t i = 0; i < acls.size(); ++i) {
      switch (acls[i].type) {
        case ACL_USER_OBJ:
          acls[i].perm = mode >> 6 & 07;
          break;
        case ACL_GROUP_OBJ:
        case ACL_MASK:
          acls[i].perm = mode >> 3 & 07;
          break;
        case ACL_OTHER:
          acls[i].perm = mode & 07;
          break;
      }
    }
    aclStr = dmlite::serializeAcl(acls);
  }

  // Update entry
  this->si_->getINode()->changeMode(meta.stat.st_ino, meta.stat.st_uid, meta.stat.st_gid, mode, aclStr);
}



void BuiltInCatalog::changeOwner(const std::string& path, uid_t newUid, gid_t newGid, bool followSymLink) throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path, followSymLink);
  
  // If -1, no changes
  if (newUid == (uid_t)-1)
    newUid = meta.stat.st_uid;
  if (newGid == (gid_t)-1)
    newGid = meta.stat.st_gid;

  // Make sense to do anything?
  if (newUid == meta.stat.st_uid && newGid == meta.stat.st_gid)
    return;

  // If root, skip all checks
  if (this->secCtx_->getUser().uid != 0) {
    // Only root can change the owner
    if (meta.stat.st_uid != newUid)
      throw DmException(DM_BAD_OPERATION, "Only root can change the owner");
    // If the group is changing...
    if (meta.stat.st_gid != newGid) {
      // The user has to be the owner
      if (meta.stat.st_uid != this->secCtx_->getUser().uid)
        throw DmException(DM_BAD_OPERATION, "Only root or the owner can change the group");
      // AND it has to belong to that group
      if (!this->secCtx_->hasGroup(newGid))
        throw DmException(DM_BAD_OPERATION, "The user does not belong to the group %d", newGid);
      // If it does, the group exists :)
    }
  }

  // Update the ACL's if there is any
  std::string aclStr;
  if (meta.acl[0] != '\0') {
    std::vector<Acl> acls = dmlite::deserializeAcl(meta.acl);
    for (size_t i = 0; i < acls.size(); ++i) {
      if (acls[i].type == ACL_USER_OBJ)
        acls[i].id = newUid;
      else if (acls[i].type == ACL_GROUP_OBJ)
        acls[i].id = newGid;
    }
    aclStr = dmlite::serializeAcl(acls);
  }

  // Change!
  this->si_->getINode()->changeMode(meta.stat.st_ino, newUid, newGid, meta.stat.st_mode, aclStr);
}



void BuiltInCatalog::changeSize(const std::string& path, size_t newSize) throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path, false);
  if (this->secCtx_->getUser().uid != meta.stat.st_uid &&
      checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Can not change the size of " + path);
  
  this->si_->getINode()->changeSize(meta.stat.st_ino, newSize);
}



void BuiltInCatalog::changeChecksum(const std::string& path,
                                    const std::string& csumtype,
                                    const std::string& csumvalue) throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path, false);
  if (this->secCtx_->getUser().uid != meta.stat.st_uid &&
      checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Can not change the checksum of " + path);
  
  if (csumtype != "MD" && csumtype != "AD" && csumtype != "CS")
    throw DmException(DM_INVALID_VALUE,
                      "%s is an invalid checksum type", csumtype.c_str());
  
  this->si_->getINode()->changeChecksum(meta.stat.st_ino, csumtype, csumvalue);
}



void BuiltInCatalog::setAcl(const std::string& path, const std::vector<Acl>& acls) throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path);

  // Check we can change it
  if (this->secCtx_->getUser().uid != meta.stat.st_uid &&
      this->secCtx_->getUser().uid != 0)
    throw DmException(DM_FORBIDDEN, "Only the owner can change the ACL of " + path);

  std::vector<Acl> aclsCopy(acls);

  // Make sure the owner and group matches!
  for (size_t i = 0; i < aclsCopy.size(); ++i) {
    if (aclsCopy[i].type == ACL_USER_OBJ)
      aclsCopy[i].id = meta.stat.st_uid;
    else if (aclsCopy[i].type == ACL_GROUP_OBJ)
      aclsCopy[i].id = meta.stat.st_gid;
    else if (aclsCopy[i].type & ACL_DEFAULT && !S_ISDIR(meta.stat.st_mode))
      throw DmException(DM_INVALID_ACL, "Defaults can be only applied to directories");
  }

  // Validate the ACL
  dmlite::validateAcl(aclsCopy);

  // Update the file mode
  for (size_t i = 0; i < aclsCopy.size(); ++i) {
    switch (aclsCopy[i].type) {
      case ACL_USER_OBJ:
        meta.stat.st_mode = (meta.stat.st_mode & 0177077) | (aclsCopy[i].perm << 6);
        break;
      case ACL_GROUP_OBJ:
        meta.stat.st_mode = (meta.stat.st_mode & 0177707) | (aclsCopy[i].perm << 3);
        break;
      case ACL_MASK:
        meta.stat.st_mode = (meta.stat.st_mode & ~070) | (meta.stat.st_mode & aclsCopy[i].perm << 3);
        break;
      case ACL_OTHER:
        meta.stat.st_mode = (meta.stat.st_mode & 0177770) | (aclsCopy[i].perm);
        break;
    }
  }

  // If only 3 entries, no extended ACL
  std::string aclStr;
  if (aclsCopy.size() > 3)
    aclStr = dmlite::serializeAcl(aclsCopy);

  // Update the file
  this->si_->getINode()->changeMode(meta.stat.st_ino, meta.stat.st_uid, meta.stat.st_gid, meta.stat.st_mode, aclStr);
}



void BuiltInCatalog::utime(const std::string& path, const struct utimbuf* buf) throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path);

  // The user is the owner OR buf is NULL and has write permissions
  if (this->secCtx_->getUser().uid != meta.stat.st_uid &&
      checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to modify the time of " + path);

  // Touch
  this->si_->getINode()->utime(meta.stat.st_ino, buf);
}



std::string BuiltInCatalog::getComment(const std::string& path) throw (DmException)
{
  // Get the file and check we can read
  ExtendedStat meta = this->extendedStat(path);
  
  if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IREAD) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to read " + path);

  return this->si_->getINode()->getComment(meta.stat.st_ino);
}



void BuiltInCatalog::setComment(const std::string& path, const std::string& comment) throw (DmException)
{
  // Get the file and check we can write
  ExtendedStat meta = this->extendedStat(path);

  if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to write " + path);

  this->si_->getINode()->setComment(meta.stat.st_ino, comment);
}



void BuiltInCatalog::setGuid(const std::string& path, const std::string &guid) throw (DmException)
{
  ExtendedStat meta = this->extendedStat(path);

  if (checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Not enough permissions to write " + path);

  this->si_->getINode()->setGuid(meta.stat.st_ino, guid);
}



Directory* BuiltInCatalog::openDir (const std::string& path) throw (DmException)
{
  BuiltInDir* dirp;
  
  dirp = new BuiltInDir;
  
  try {
    dirp->dir = this->extendedStat(path);

    if (checkPermissions(this->secCtx_, dirp->dir.acl, dirp->dir.stat, S_IREAD) != 0)
      throw DmException(DM_FORBIDDEN, "Not enough permissions to read " + path);

    dirp->idir = this->si_->getINode()->openDir(dirp->dir.stat.st_ino);
    
    return dirp;
  }
  catch (...) {
    delete dirp;
    throw;
  }  
}



void BuiltInCatalog::closeDir(Directory* dir) throw (DmException)
{
  BuiltInDir* dirp = (BuiltInDir*)dir;
  this->si_->getINode()->closeDir(dirp->idir);
  delete dirp;
}



struct dirent* BuiltInCatalog::readDir (Directory* dir) throw (DmException)
{
  BuiltInDir* dirp = (BuiltInDir*)dir;
  struct dirent* d = this->si_->getINode()->readDir(dirp->idir);
  this->updateAccessTime(dirp->dir);
  return d;
}



ExtendedStat* BuiltInCatalog::readDirx(Directory* dir) throw (DmException)
{
  BuiltInDir* dirp = (BuiltInDir*)dir;
  ExtendedStat* s = this->si_->getINode()->readDirx(dirp->idir);
  this->updateAccessTime(dirp->dir);
  return s;
}


FileReplica BuiltInCatalog::getReplica(const std::string& rfn) throw (DmException)
{
  FileReplica rdata = this->si_->getINode()->getReplica(rfn);
  ExtendedStat meta = this->si_->getINode()->extendedStat(rdata.fileid);
  
  this->traverseBackwards(meta);
  
  return rdata;
}



void BuiltInCatalog::updateReplica(const FileReplica& replica) throw (DmException)
{
  // Can not trust the fileid of replica!
  FileReplica rdata = this->si_->getINode()->getReplica(replica.replicaid);
  ExtendedStat meta = this->si_->getINode()->extendedStat(rdata.fileid);
  
  this->traverseBackwards(meta);
  
  if (dmlite::checkPermissions(this->secCtx_, meta.acl, meta.stat, S_IWRITE) != 0)
    throw DmException(DM_FORBIDDEN, "Can not modify the replica");
  
  this->si_->getINode()->updateReplica(replica);
}



void BuiltInCatalog::updateAccessTime(const ExtendedStat& meta) throw (DmException)
{
  if (this->updateATime_) {
    struct utimbuf tim;
    tim.actime  = time(NULL);
    tim.modtime = meta.stat.st_mtime;
    this->si_->getINode()->utime(meta.stat.st_ino, &tim);
  }
}



ExtendedStat BuiltInCatalog::getParent(const std::string& path,
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
    return this->si_->getINode()->extendedStat(this->cwd_);
  else
    return this->extendedStat("/");
}



void BuiltInCatalog::traverseBackwards(const ExtendedStat& meta) throw (DmException)
{
  ExtendedStat current = meta;

  // We want to check if we can arrive here...
  while (current.parent != 0) {
    current = this->si_->getINode()->extendedStat(current.parent);
    if (checkPermissions(this->secCtx_, current.acl, current.stat, S_IEXEC))
      throw DmException(DM_FORBIDDEN, "Can not access #%ld",
                        current.stat.st_ino);
  }
}
