/// @file    include/dmlite/dummy/DummyCatalog.h
/// @brief   A dummy plugin that just delegates calls to a decorated one.
/// @details It makes sense as a base for other decorator plug-ins.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_DUMMY_CATALOG_H
#define	DMLITE_DUMMY_CATALOG_H

#include <dmlite/dmlite++.h>

namespace dmlite {

// Dummmy catalog implementation
class DummyCatalog: public Catalog
{
public:

  /// Constructor
  /// @param decorates The underlying decorated catalog.
  DummyCatalog(Catalog* decorates) throw (DmException);

  /// Destructor
  virtual ~DummyCatalog();

  // Overloading
  virtual void set(const std::string&, va_list) throw (DmException);

  virtual SecurityContext* createSecurityContext(const SecurityCredentials&) throw (DmException);
  virtual void setSecurityContext(const SecurityContext*) throw (DmException);

  virtual void        changeDir     (const std::string&) throw (DmException);
  virtual std::string getWorkingDir (void)               throw (DmException);
  virtual ino_t       getWorkingDirI(void)               throw (DmException);

  virtual ExtendedStat extendedStat(const std::string&, bool) throw (DmException);
  virtual ExtendedStat extendedStat(ino_t)              throw (DmException);
  virtual ExtendedStat extendedStat(ino_t, const std::string&) throw (DmException);

  SymLink readLink(ino_t) throw (DmException);

  virtual void addReplica(const std::string&, int64_t, const std::string&,
                          const std::string&, char, char,
                          const std::string&, const std::string&) throw (DmException);

  virtual void deleteReplica(const std::string&, int64_t,
                             const std::string&) throw (DmException);

  virtual std::vector<FileReplica> getReplicas(const std::string&) throw (DmException);
  virtual std::vector<Uri> getReplicasLocation(const std::string&) throw (DmException);
  
  virtual Uri get        (const std::string&) throw (DmException);

  virtual void symlink(const std::string&, const std::string&) throw (DmException);
  virtual void unlink (const std::string&)                     throw (DmException);

  virtual void create(const std::string&, mode_t) throw (DmException);

  virtual std::string put(const std::string&, Uri*)                     throw (DmException);
  virtual std::string put(const std::string&, Uri*, const std::string&) throw (DmException);
  virtual void        putDone(const std::string&, const Uri&, const std::string&) throw (DmException);

  virtual mode_t umask          (mode_t)                           throw ();
  virtual void   changeMode     (const std::string&, mode_t)       throw (DmException);
  virtual void   changeOwner    (const std::string&, uid_t, gid_t) throw (DmException);
  virtual void   linkChangeOwner(const std::string&, uid_t, gid_t) throw (DmException);
  
  virtual void changeSize(const std::string&, size_t) throw (DmException);

  virtual void setAcl(const std::string&, const std::vector<Acl>&) throw (DmException);

  virtual void utime(const std::string&, const struct utimbuf*) throw (DmException);
  virtual void utime(ino_t, const struct utimbuf*) throw (DmException);

  virtual std::string getComment(const std::string&)                     throw (DmException);
  virtual void        setComment(const std::string&, const std::string&) throw (DmException);

  virtual void setGuid(const std::string& path, const std::string &guid) throw (DmException);

  virtual UserInfo  getUser(uid_t)              throw (DmException);
  virtual UserInfo  getUser(const std::string&) throw (DmException);
  virtual GroupInfo getGroup(gid_t)              throw (DmException);
  virtual GroupInfo getGroup(const std::string&) throw (DmException);

  virtual Directory* openDir (const std::string&) throw (DmException);
  virtual void       closeDir(Directory*)         throw (DmException);

  virtual struct dirent* readDir (Directory*) throw (DmException);
  virtual ExtendedStat*  readDirx(Directory*) throw (DmException);

  virtual void makeDir(const std::string&, mode_t) throw (DmException);

  virtual void rename     (const std::string&, const std::string&) throw (DmException);
  virtual void removeDir  (const std::string&)                     throw (DmException);

  virtual void replicaSetLifeTime  (const std::string&, time_t) throw (DmException);
  virtual void replicaSetAccessTime(const std::string&)         throw (DmException);
  virtual void replicaSetType      (const std::string&, char)   throw (DmException);
  virtual void replicaSetStatus    (const std::string&, char)   throw (DmException);
 
protected:
  Catalog* decorated_;
private:
};


};

#endif	// DMLITE_DUMMY_CATALOG_H
