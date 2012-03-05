/// @file   plugins/adapter/NsAdapter.h
/// @brief  Adapter Plugin: Cns wrapper implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef NS_ADAPTER_H
#define	NS_ADAPTER_H

#include <dmlite/dm_catalog.h>
#include <dpns_api.h>

namespace dmlite {

/// @brief The private structure used by NsDummy to handle
///        openDir/readDir/closeDir
struct PrivateDir {
  dpns_DIR *dpnsDir;            ///< Used for calls to the dpns API.
  struct direntstat direntstat; ///< Where the data is actually stored.
};

/// Catalog implemented as a wrapper around Cns API
class NsAdapterCatalog: public Catalog
{
public:
  /// Constructor
  /// @param nsHost     The host where the NS is running.
  /// @param retryLimit Limit of retrials.
  /// @note             As CNS API is not thread-safe, neither is this.
  NsAdapterCatalog(const std::string& nsHost, unsigned retryLimit) throw (DmException);

  /// Destructor
  ~NsAdapterCatalog();


  // Overloading
  std::string getImplId(void) throw ();

  void set(const std::string&, ...)     throw (DmException);
  void set(const std::string&, va_list) throw (DmException);

  void        changeDir     (const std::string&) throw (DmException);
  std::string getWorkingDir (void)               throw (DmException);
  ino_t       getWorkingDirI(void)               throw (DmException);

  ExtendedStat extendedStat(const std::string&, bool) throw (DmException);
  ExtendedStat extendedStat(ino_t)              throw (DmException);
  ExtendedStat extendedStat(ino_t, const std::string&) throw (DmException);

  SymLink readLink(ino_t) throw (DmException);

  void addReplica(const std::string&, int64_t, const std::string&,
                  const std::string&, char, char,
                  const std::string&, const std::string&) throw (DmException);

  void deleteReplica(const std::string&, int64_t,
                     const std::string&) throw (DmException);

  std::vector<FileReplica> getReplicas(const std::string&) throw (DmException);
  FileReplica              get        (const std::string&) throw (DmException);

  void symlink(const std::string&, const std::string&) throw (DmException);
  void unlink (const std::string&)                     throw (DmException);

  void create(const std::string&, mode_t) throw (DmException);

  std::string put(const std::string&, Uri*)                           throw (DmException);
  std::string put(const std::string&, Uri*, const std::string&)       throw (DmException);
  void        putStatus(const std::string&, const std::string&, Uri*) throw (DmException);
  void        putDone  (const std::string&, const std::string&)       throw (DmException);

  mode_t umask          (mode_t)                           throw ();
  void   changeMode     (const std::string&, mode_t)       throw (DmException);
  void   changeOwner    (const std::string&, uid_t, gid_t) throw (DmException);
  void   linkChangeOwner(const std::string&, uid_t, gid_t) throw (DmException);

  void utime(const std::string&, const struct utimbuf*) throw (DmException);

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
  unsigned    retryLimit_;
  std::string cwdPath_;

  // Need to keep this in memory, as dpns/dpm API do not make
  // copy of them (sigh)
  // setAuthorizationId does, though (don't ask me why)
  char **fqans_;
  int    nFqans_;
  char  *vo_;

  uid_t uid;
  gid_t gid;
  std::string udn;

private:
  std::string nsHost_;
};

};

#endif	// NS_ADAPTER_H
