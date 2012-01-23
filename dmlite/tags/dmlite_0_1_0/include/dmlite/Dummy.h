/// @file    include/dm/Dummy.h
/// @brief   A dummy plugin that just delegates calls to a decorated one.
/// @details It makes sense as a base for other decorator plug-ins.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DUMMY_H
#define	DUMMY_H

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
  virtual std::string getImplId(void);

  virtual void set(const std::string&, ...)     throw (DmException);
  virtual void set(const std::string&, va_list) throw (DmException);

  virtual void        changeDir    (const std::string&) throw (DmException);
  virtual std::string getWorkingDir(void)               throw (DmException);

  virtual struct stat  stat        (const std::string&) throw (DmException);
  virtual struct stat  stat        (ino_t)              throw (DmException);
  virtual struct stat  linkStat    (const std::string&) throw (DmException);
  virtual struct xstat extendedStat(const std::string&) throw (DmException);
  virtual struct xstat extendedStat(ino_t)              throw (DmException);

  virtual void addReplica(const std::string&, int64_t, const std::string&,
                          const std::string&, const char, const char,
                          const std::string&, const std::string&) throw (DmException);

  virtual void deleteReplica(const std::string&, int64_t,
                             const std::string&) throw (DmException);

  virtual std::vector<FileReplica> getReplicas(const std::string&) throw (DmException);
  virtual FileReplica              get        (const std::string&) throw (DmException);

  virtual void symlink(const std::string&, const std::string&) throw (DmException);
  virtual void unlink (const std::string&)                     throw (DmException);

  virtual std::string put(const std::string&, Uri*)                           throw (DmException);
  virtual std::string put(const std::string&, Uri*, const std::string&)       throw (DmException);
  virtual void        putStatus(const std::string&, const std::string&, Uri*) throw (DmException);
  virtual void        putDone  (const std::string&, const std::string&)       throw (DmException);

  virtual void changeMode     (const std::string&, mode_t)       throw (DmException);
  virtual void changeOwner    (const std::string&, uid_t, gid_t) throw (DmException);
  virtual void linkChangeOwner(const std::string&, uid_t, gid_t) throw (DmException);

  virtual std::string getComment(const std::string&)                     throw (DmException);
  virtual void        setComment(const std::string&, const std::string&) throw (DmException);

  virtual UserInfo  getUser(uid_t)              throw (DmException);
  virtual UserInfo  getUser(const std::string&) throw (DmException);
  virtual GroupInfo getGroup(gid_t)              throw (DmException);
  virtual GroupInfo getGroup(const std::string&) throw (DmException);

  virtual void getIdMap(const std::string&, const std::vector<std::string>&,
                        uid_t*, std::vector<gid_t>*) throw (DmException);

  virtual Directory* openDir (const std::string&) throw (DmException);
  virtual void       closeDir(Directory*)         throw (DmException);

  virtual struct dirent*     readDir (Directory*) throw (DmException);
  virtual struct direntstat* readDirx(Directory*) throw (DmException);

  virtual void makeDir(const std::string&, mode_t) throw (DmException);

  virtual void rename     (const std::string&, const std::string&) throw (DmException);
  virtual void removeDir  (const std::string&)                     throw (DmException);

  virtual void setUserId  (uid_t, gid_t, const std::string&)                 throw (DmException);
  virtual void setVomsData(const std::string&, const std::vector<std::string>&) throw (DmException);

  virtual std::vector<Pool>       getPools(void)                         throw (DmException);
  virtual std::vector<FileSystem> getPoolFilesystems(const std::string&) throw (DmException);

protected:
  Catalog* decorated_;
private:
};

/// Concrete factory for Dummy plugin
class DummyFactory: public CatalogFactory {
public:
  /// Constructor
  DummyFactory(CatalogFactory* catalogFactory) throw (DmException);
  /// Destructor
  ~DummyFactory() throw (DmException);

  void     set(const std::string& key, const std::string& value) throw (DmException);
  Catalog* create() throw (DmException);
protected:
  CatalogFactory* nested_factory_;
private:
};

};

#endif	//DUMMY_H

