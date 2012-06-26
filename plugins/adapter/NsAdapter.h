/// @file   plugins/adapter/NsAdapter.h
/// @brief  Adapter Plugin: Cns wrapper implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef NS_ADAPTER_H
#define	NS_ADAPTER_H

#include <dmlite/cpp/dm_catalog.h>
#include <dpns_api.h>

namespace dmlite {

/// @brief The private structure used by NsDummy to handle
///        openDir/readDir/closeDir
struct PrivateDir {
  dpns_DIR    *dpnsDir; ///< Used for calls to the dpns API.
  struct xstat stat;    ///< Where the data is actually stored.
};

/// Catalog implemented as a wrapper around Cns API
class NsAdapterCatalog: public Catalog, public UserGroupDb
{
public:
  /// Constructor
  /// @param retryLimit Limit of retrials.
  NsAdapterCatalog(unsigned retryLimit) throw (DmException);

  /// Destructor
  ~NsAdapterCatalog();


  // Overloading
  std::string getImplId(void) throw ();

  void setStackInstance(StackInstance* si) throw (DmException);
  
  SecurityContext* createSecurityContext(const SecurityCredentials&) throw (DmException);
  void setSecurityContext(const SecurityContext*) throw (DmException);

  void        changeDir     (const std::string&) throw (DmException);
  std::string getWorkingDir (void)               throw (DmException);
  ino_t       getWorkingDirI(void)               throw (DmException);

  ExtendedStat extendedStat(const std::string&, bool) throw (DmException);

  SymLink readLink(ino_t) throw (DmException);

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

  GroupInfo getGroup(gid_t)              throw (DmException);
  GroupInfo getGroup(const std::string&) throw (DmException);

  UserInfo getUser(uid_t)              throw (DmException);
  UserInfo getUser(const std::string&) throw (DmException);

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
  
  // Not supported (yet)
  GroupInfo newGroup(const std::string& gname) throw (DmException);
  UserInfo newUser(const std::string& uname, const std::string& ca) throw (DmException);
  void getIdMap(const std::string& userName,
                const std::vector<std::string>& groupNames,
                UserInfo* user,
                std::vector<GroupInfo>* groups) throw (DmException);
  
protected:
  StackInstance* si_;
  
  unsigned    retryLimit_;
  std::string cwdPath_;

  // Need to keep this in memory, as dpns/dpm API do not make
  // copy of them (sigh)
  // setAuthorizationId does, though (don't ask me why)
  char **fqans_;
  int    nFqans_;
  char  *vo_;

private:
};

};

#endif	// NS_ADAPTER_H
