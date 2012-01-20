/// @file    plugins/profiler/Profiler.h
/// @brief   Profiler plugin.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef PROFILER_H
#define	PROFILER_H

#include <cstdarg>
#include <iostream>

#include <dmlite/dmlite++.h>

namespace dmlite {

// Profiler plugin.
class ProfilerCatalog: public Catalog
{
public:

  /// Constructor.
  /// @param decorates The underlying decorated catalog.
  ProfilerCatalog(Catalog* decorates) throw (DmException);

  /// Destructor.
  ~ProfilerCatalog();

  // Overloading
  std::string getImplId(void);

  void set(const std::string&, ...)     throw (DmException);
  void set(const std::string&, va_list) throw (DmException);

  void        changeDir    (const std::string&) throw (DmException);
  std::string getWorkingDir(void)               throw (DmException);

  struct stat  stat        (const std::string&) throw (DmException);
  struct stat  linkStat    (const std::string&) throw (DmException);
  struct xstat extendedStat(const std::string&) throw (DmException);

  void addReplica(const std::string&, int64_t, const std::string&,
                          const std::string&, const char, const char,
                          const std::string&, const std::string&) throw (DmException);

  void deleteReplica(const std::string&, int64_t,
                             const std::string&) throw (DmException);

  std::vector<FileReplica> getReplicas(const std::string&) throw (DmException);
  FileReplica              get        (const std::string&) throw (DmException);

  void symlink(const std::string&, const std::string&) throw (DmException);
  void unlink (const std::string&)                     throw (DmException);

  std::string put(const std::string&, Uri*)                           throw (DmException);
  std::string put(const std::string&, Uri*, const std::string&)       throw (DmException);
  void        putStatus(const std::string&, const std::string&, Uri*) throw (DmException);
  void        putDone  (const std::string&, const std::string&)       throw (DmException);

  void changeMode     (const std::string&, mode_t)       throw (DmException);
  void changeOwner    (const std::string&, uid_t, gid_t) throw (DmException);
  void linkChangeOwner(const std::string&, uid_t, gid_t) throw (DmException);

  std::string getComment(const std::string&)                     throw (DmException);
  void        setComment(const std::string&, const std::string&) throw (DmException);

  GroupInfo getGroup(gid_t)              throw (DmException);
  GroupInfo getGroup(const std::string&) throw (DmException);

  void getIdMap(const std::string&, const std::vector<std::string>&,
                        uid_t*, std::vector<gid_t>*) throw (DmException);

  UserInfo getUser(uid_t)              throw (DmException);
  UserInfo getUser(const std::string&) throw (DmException);

  Directory* openDir (const std::string&) throw (DmException);
  void       closeDir(Directory*)         throw (DmException);

  struct dirent*     readDir (Directory*) throw (DmException);
  struct direntstat* readDirx(Directory*) throw (DmException);

  void makeDir(const std::string&, mode_t) throw (DmException);

  void rename     (const std::string&, const std::string&) throw (DmException);
  void removeDir  (const std::string&)                     throw (DmException);

  void setUserId  (uid_t, gid_t, const std::string&)                    throw (DmException);
  void setVomsData(const std::string&, const std::vector<std::string>&) throw (DmException);

  std::vector<Pool>       getPools(void)                         throw (DmException);
  std::vector<FileSystem> getPoolFilesystems(const std::string&) throw (DmException);

protected:
  /// The decorated Catalog.
  Catalog* decorated_;
  char*    decoratedId_;
private:
};

/// Concrete factory for Profiler plugin.
class ProfilerFactory: public CatalogFactory {
public:
  /// Constructor
  ProfilerFactory(CatalogFactory* catalogFactory) throw (DmException);
  /// Destructor
  ~ProfilerFactory();

  void set(const std::string& key, const std::string& value) throw (DmException);
  Catalog* create()                            throw (DmException);
protected:
  /// The decorated factory.
  CatalogFactory* nestedFactory_;
private:
};

};

#endif	// PROFILER_H
