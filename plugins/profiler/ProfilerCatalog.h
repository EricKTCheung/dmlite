/// @file    plugins/profiler/ProfilerCatalog.h
/// @brief   Profiler plugin.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef PROFILERCATALOG_H
#define	PROFILERCATALOG_H

#include <dmlite/dm_catalog.h>

namespace dmlite {

// Profiler catalog.
class ProfilerCatalog: public Catalog
{
public:

  /// Constructor.
  /// @param decorates The underlying decorated catalog.
  ProfilerCatalog(Catalog* decorates) throw (DmException);

  /// Destructor.
  ~ProfilerCatalog();

  // Overloading
  std::string getImplId(void) throw ();

  void set(const std::string&, ...)     throw (DmException);
  void set(const std::string&, va_list) throw (DmException);

  void        changeDir    (const std::string&) throw (DmException);
  std::string getWorkingDir(void)               throw (DmException);

  struct stat  stat        (const std::string&) throw (DmException);
  struct stat  stat        (ino_t)              throw (DmException);
  struct stat  linkStat    (const std::string&) throw (DmException);
  struct xstat extendedStat(const std::string&) throw (DmException);
  struct xstat extendedStat(ino_t)              throw (DmException);

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

protected:
  /// The decorated Catalog.
  Catalog* decorated_;
  char*    decoratedId_;
private:
};

};

#endif	// PROFILERCATALOG_H

