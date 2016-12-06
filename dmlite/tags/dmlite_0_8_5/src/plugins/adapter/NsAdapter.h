/// @file   NsAdapter.h
/// @brief  Adapter Plugin: Cns wrapper implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef NS_ADAPTER_H
#define	NS_ADAPTER_H

#include <dirent.h>
#include <dpns_api.h>
#include <dmlite/cpp/catalog.h>

namespace dmlite {
  
  const int kAclEntriesMax = 300;
  const int kAclSize       = 13;
  const int kCommentMax    = 255;
  // CA_MAXPATHLEN +1 as from h/Castor_limits.h
  const int kPathMax       = 1024;

  /// @brief The private structure used by NsDummy to handle
  ///        openDir/readDir/closeDir
  struct PrivateDir: public Directory {
    virtual ~PrivateDir() {};
    dpns_DIR     *dpnsDir; ///< Used for calls to the dpns API.
    ExtendedStat  stat;    ///< Where the data is actually stored.
  };

  /// Catalog implemented as a wrapper around Cns API
  class NsAdapterCatalog: public Catalog, public Authn
  {
   public:
    /// Constructor
    /// @param retryLimit Limit of retrials.
    NsAdapterCatalog(unsigned retryLimit, bool hostDnIsRoot, std::string hostDn) throw (DmException);

    /// Destructor
    ~NsAdapterCatalog();

    // Overloading
    std::string getImplId(void) const throw ();

    void setStackInstance(StackInstance* si) throw (DmException);

    SecurityContext* createSecurityContext(const SecurityCredentials&) throw (DmException);
    SecurityContext* createSecurityContext(void) throw (DmException);
    void setSecurityContext(const SecurityContext*) throw (DmException);

    void        changeDir     (const std::string&) throw (DmException);
    std::string getWorkingDir (void)               throw (DmException);

    ExtendedStat extendedStat(const std::string&, bool) throw (DmException);
    ExtendedStat extendedStatByRFN(const std::string& rfn) throw (DmException);

    bool access(const std::string&, int) throw (DmException);
    bool accessReplica(const std::string&, int) throw (DmException);

    SymLink readLink(ino_t) throw (DmException);

    void addReplica   (const Replica&) throw (DmException);
    void deleteReplica(const Replica&) throw (DmException);
    std::vector<Replica> getReplicas(const std::string&) throw (DmException);

    void        symlink (const std::string&, const std::string&) throw (DmException);
    std::string readLink(const std::string& path) throw (DmException);
    
    void unlink(const std::string&)                     throw (DmException);

    void create(const std::string&, mode_t) throw (DmException);

    mode_t umask   (mode_t)                           throw ();
    void   setMode (const std::string&, mode_t)       throw (DmException);
    void   setOwner(const std::string&, uid_t, gid_t, bool) throw (DmException);

    void setSize    (const std::string&, size_t) throw (DmException);
    
    void setAcl(const std::string&, const Acl&) throw (DmException);

    void utime(const std::string&, const struct utimbuf*) throw (DmException);

    std::string getComment(const std::string&) throw (DmException);
    void        setComment(const std::string&,
                          const std::string&) throw (DmException);

    void setGuid(const std::string&, const std::string&) throw (DmException);

    void updateExtendedAttributes(const std::string&,
                                  const Extensible&) throw (DmException);
    
    GroupInfo getGroup   (gid_t)              throw (DmException);
    GroupInfo getGroup   (const std::string&) throw (DmException);
    GroupInfo getGroup   (const std::string& key,
                          const boost::any& value) throw (DmException);
    GroupInfo newGroup   (const std::string& gname) throw (DmException);
    void      updateGroup(const GroupInfo& group) throw (DmException);
    void      deleteGroup(const std::string& groupName) throw (DmException);
    
    UserInfo  getUser   (const std::string&) throw (DmException);
    UserInfo  getUser   (const std::string& key,
                         const boost::any& value) throw (DmException);
    UserInfo  newUser   (const std::string& uname) throw (DmException);
    void      updateUser(const UserInfo& user) throw (DmException);
    void      deleteUser(const std::string& userName) throw (DmException);
    
    std::vector<GroupInfo> getGroups(void) throw (DmException);
    std::vector<UserInfo>  getUsers (void) throw (DmException);

    Directory* openDir (const std::string&) throw (DmException);
    void       closeDir(Directory*)         throw (DmException);

    struct dirent* readDir (Directory*) throw (DmException);
    ExtendedStat*  readDirx(Directory*) throw (DmException);

    void makeDir(const std::string&, mode_t) throw (DmException);

    void rename     (const std::string&, const std::string&) throw (DmException);
    void removeDir  (const std::string&)                     throw (DmException);

    Replica getReplicaByRFN(const std::string& rfn)        throw (DmException);
    void    updateReplica(const Replica& replica) throw (DmException);
       
    void getIdMap(const std::string& userName,
                  const std::vector<std::string>& groupNames,
                  UserInfo* user,
                  std::vector<GroupInfo>* groups) throw (DmException);

   protected:
    void setDpnsApiIdentity();

    StackInstance* si_;

    unsigned    retryLimit_;
    std::string cwdPath_;

    // Need to keep this in memory, as dpns/dpm API do not make
    // copy of them (sigh)
    // setAuthorizationId does, though (don't ask me why)
    char   **fqans_;
    size_t   nFqans_;

    // Host DN is root
    bool hostDnIsRoot_;
    std::string hostDn_;

    std::string userId_;
    
    const SecurityContext* secCtx_;
  };

  class NsAdapterINode: public INode {
    public:

    /// Constructor
    NsAdapterINode(unsigned retryLimit, bool hostDnIsRoot, std::string hostDn,
        std::string dpnsHost) throw (DmException);

    /// Destructor
    ~NsAdapterINode();

    // Overloading
    std::string getImplId(void) const throw ();

    void setStackInstance(StackInstance* si) throw (DmException);
    void setSecurityContext(const SecurityContext* ctx) throw (DmException);

    void begin(void) throw (DmException);
    void commit(void) throw (DmException);
    void rollback(void) throw (DmException);

    ExtendedStat create(const ExtendedStat&) throw (DmException);

    void symlink(ino_t inode, const std::string &link) throw (DmException);

    void unlink(ino_t inode) throw (DmException);

    void move  (ino_t inode, ino_t dest) throw (DmException);
    void rename(ino_t inode, const std::string& name) throw (DmException);

    ExtendedStat extendedStat(ino_t inode) throw (DmException);
    ExtendedStat extendedStat(ino_t parent, const std::string& name) throw (DmException);
    ExtendedStat extendedStat(const std::string& guid) throw (DmException);

    SymLink readLink(ino_t inode) throw (DmException);

    void addReplica   (const Replica&) throw (DmException);  
    void deleteReplica(const Replica&) throw (DmException);

    std::vector<Replica> getReplicas(ino_t inode) throw (DmException);

    Replica getReplica   (int64_t rid) throw (DmException);
    Replica getReplica   (const std::string& sfn) throw (DmException);
    void    updateReplica(const Replica& replica) throw (DmException);

    void utime(ino_t inode, const struct utimbuf* buf) throw (DmException);

    void setMode(ino_t inode, uid_t uid, gid_t gid,
                mode_t mode, const Acl& acl) throw (DmException);

    void setSize    (ino_t inode, size_t size) throw (DmException);
    void setChecksum(ino_t, const std::string&, const std::string&) throw (DmException);
    
    std::string getComment   (ino_t inode) throw (DmException);
    void        setComment   (ino_t inode, const std::string& comment) throw (DmException);
    void        deleteComment(ino_t inode) throw (DmException);

    void setGuid(ino_t inode, const std::string& guid) throw (DmException);
    
    void updateExtendedAttributes(ino_t inode,
                                  const Extensible& attr) throw (DmException);

    IDirectory*    openDir (ino_t inode) throw (DmException);
    void           closeDir(IDirectory* dir) throw (DmException);  
    ExtendedStat*  readDirx(IDirectory* dir) throw (DmException);
    struct dirent* readDir (IDirectory* dir) throw (DmException);

   protected:
    void setDpnsApiIdentity();

    StackInstance* si_;

    unsigned    retryLimit_;
    std::string dpnsHost_;

    // Need to keep this in memory, as dpns/dpm API do not make
    // copy of them (sigh)
    // setAuthorizationId does, though (don't ask me why)
    char   **fqans_;
    size_t   nFqans_;

    // Host DN is root
    bool hostDnIsRoot_;
    std::string hostDn_;

    const SecurityContext* secCtx_;
  };

};

#endif	// NS_ADAPTER_H
