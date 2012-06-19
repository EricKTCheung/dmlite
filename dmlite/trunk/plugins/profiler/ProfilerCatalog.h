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

  void setStackInstance(StackInstance* si) throw (DmException);
  
  void setSecurityContext(const SecurityContext*) throw (DmException);

  void        changeDir     (const std::string&) throw (DmException);
  std::string getWorkingDir (void)               throw (DmException);
  ino_t       getWorkingDirI(void)               throw (DmException);

  struct stat  stat        (const std::string&) throw (DmException);
  ExtendedStat extendedStat(const std::string&, bool) throw (DmException);

  void addReplica(const std::string&, int64_t, const std::string&,
                  const std::string&, char, char,
                  const std::string&, const std::string&) throw (DmException);

  void deleteReplica(const std::string&, int64_t,
                             const std::string&) throw (DmException);

  std::vector<FileReplica> getReplicas(const std::string&) throw (DmException);
  Location get(const std::string&) throw (DmException);
  
  Location put(const std::string& path) throw (DmException);
  Location put(const std::string& path,
               const std::string& guid) throw (DmException);
  void     putDone(const std::string& host, const std::string& rfn,
                   const std::map<std::string, std::string>& params) throw (DmException);

  void symlink(const std::string&, const std::string&) throw (DmException);
  void unlink (const std::string&)                     throw (DmException);

  void create(const std::string&, mode_t) throw (DmException);

  mode_t umask          (mode_t)                           throw ();
  void   changeMode     (const std::string&, mode_t)       throw (DmException);
  void   changeOwner    (const std::string&, uid_t, gid_t, bool) throw (DmException);
  
  void changeSize(const std::string&, size_t) throw (DmException);

  void setAcl(const std::string&, const std::vector<Acl>&) throw (DmException);

  void utime(const std::string&, const struct utimbuf*) throw (DmException);

  std::string getComment(const std::string&)                     throw (DmException);
  void        setComment(const std::string&, const std::string&) throw (DmException);

  void setGuid(const std::string&, const std::string&) throw (DmException);

  Directory* openDir (const std::string&) throw (DmException);
  void       closeDir(Directory*)         throw (DmException);

  struct dirent* readDir (Directory*) throw (DmException);
  ExtendedStat*  readDirx(Directory*) throw (DmException);

  void makeDir(const std::string&, mode_t) throw (DmException);

  void rename     (const std::string&, const std::string&) throw (DmException);
  void removeDir  (const std::string&)                     throw (DmException);

  void replicaSetLifeTime  (const std::string&, time_t) throw (DmException);
  void replicaSetAccessTime(const std::string&)         throw (DmException);
  void replicaSetType      (const std::string&, char)   throw (DmException);
  void replicaSetStatus    (const std::string&, char)   throw (DmException);

protected:
  /// The decorated Catalog.
  Catalog* decorated_;
  char*    decoratedId_;
private:
};

};

#endif	// PROFILERCATALOG_H

