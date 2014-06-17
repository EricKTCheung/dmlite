/// @file   FilesystemDriver.h
/// @brief  Regular Filesystem pool
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef FILESYSTEMDRIVER_H
#define	FILESYSTEMDRIVER_H

#include <dmlite/cpp/pooldriver.h>
#include <dpm_api.h>

#define NO_FLAG 0
#define OVERWRITE_FLAG 1
#define REPLICATE_FLAG 4

namespace dmlite {
  
  /// Filesystem driver.
  class FilesystemPoolDriver: public PoolDriver {
  public:
    FilesystemPoolDriver(const std::string&, bool, unsigned, unsigned,
        const std::string&);
    ~FilesystemPoolDriver();

    std::string getImplId() const throw();

    void setStackInstance(StackInstance* si) throw (DmException);
    void setSecurityContext(const SecurityContext*) throw (DmException);

    PoolHandler* createPoolHandler(const std::string& poolName) throw (DmException);

    void toBeCreated(const Pool& pool) throw (DmException);
    void justCreated(const Pool& pool) throw (DmException);
    void update(const Pool& pool) throw (DmException);
    void toBeDeleted(const Pool& pool) throw (DmException);

  private:
    friend class FilesystemPoolHandler;

    void setDpmApiIdentity();

    const SecurityContext* secCtx_;

    StackInstance* si_;

    std::string tokenPasswd_;
    bool        tokenUseIp_;
    unsigned    tokenLife_;
    std::string userId_;
    unsigned    retryLimit_;

    // Need to keep this in memory, as dpns/dpm API do not make
    // copy of them (sigh)
    // setAuthorizationId does, though (don't ask me why)
    char **fqans_;
    int    nFqans_;

    /// Admin username for replication.
    const std::string adminUsername_;
  };

  class FilesystemPoolHandler: public PoolHandler {
  public:
    FilesystemPoolHandler(FilesystemPoolDriver*, const std::string& poolName);
    ~FilesystemPoolHandler();

    std::string getPoolType    (void) throw (DmException);
    std::string getPoolName    (void) throw (DmException);
    uint64_t    getTotalSpace  (void) throw (DmException);
    uint64_t    getFreeSpace   (void) throw (DmException);
    bool        poolIsAvailable(bool) throw (DmException);

    bool     replicaIsAvailable(const Replica& replica) throw (DmException);
    Location whereToRead       (const Replica& replica) throw (DmException);

    void removeReplica(const Replica&) throw (DmException);

    Location whereToWrite(const std::string&) throw (DmException);

    void cancelWrite(const Location& loc) throw (DmException);

  private:
    FilesystemPoolDriver* driver_;
    std::string           poolName_;
    uint64_t              total_, free_;

    void update(void) throw (DmException);

    std::vector<dpm_fs> getFilesystems(const std::string&) throw (DmException);
    dpm_fs              chooseFilesystem(std::string& requestedFs, std::vector<dpm_fs>& fsV) throw (DmException);
    // Create a date string as used in the DPM storage hierarchy -- not used for now
    // std::string         getDateString(void) throw ();
  };

};

#endif	// FILESYSTEMDRIVER_H
