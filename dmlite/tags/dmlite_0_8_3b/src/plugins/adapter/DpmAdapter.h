/// @file   DpmAdapter.h
/// @brief  DPM wrapper (implements get, put, putDone, etc.)
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DPM_ADAPTER_H
#define	DPM_ADAPTER_H

#include <syslog.h>
#include <dmlite/cpp/poolmanager.h>
#include "NsAdapter.h"

namespace dmlite {

  /// Overrides some functions using the DPM API.
  class DpmAdapterCatalog: public NsAdapterCatalog {
  public:
    /// Constructor
    /// @param retryLimit The limit of retrials.
    DpmAdapterCatalog(NsAdapterFactory* factory, unsigned retryLimit, bool hostDnIsRoot, std::string hostDn) throw (DmException);

    /// Destructor
    ~DpmAdapterCatalog();

    // Overload
    std::string getImplId(void) const throw ();

    void setSecurityContext(const SecurityContext*) throw (DmException); 

    void unlink(const std::string&) throw (DmException);
    void getChecksum(const std::string& path,
                     const std::string& csumtype,
                     std::string& csumvalue,
                     const std::string &pfn,
                     const bool forcerecalc, const int waitsecs) throw (DmException);
                     
  private:
    void setDpmApiIdentity();

    /// The corresponding factory.
    NsAdapterFactory* factory_;
  };



  class DpmAdapterPoolManager: public PoolManager {
  public:
    DpmAdapterPoolManager(DpmAdapterFactory* factory, unsigned retryLimit,
                          const std::string&, bool, unsigned) throw (DmException);
    ~DpmAdapterPoolManager();

    std::string getImplId() const throw ();

    void setStackInstance(StackInstance* si) throw (DmException);
    void setSecurityContext(const SecurityContext*) throw (DmException);

    std::vector<Pool> getPools(PoolAvailability availability = kAny) throw (DmException);
    Pool getPool(const std::string&) throw (DmException);

    void newPool(const Pool& pool) throw (DmException);
    void updatePool(const Pool& pool) throw (DmException);
    void deletePool(const Pool& pool) throw (DmException);

    Location whereToRead (const std::string& path) throw (DmException);
    Location whereToRead (ino_t inode)             throw (DmException);
    Location whereToWrite(const std::string& path) throw (DmException);

    void cancelWrite(const Location& loc) throw (DmException);

  private:
    void setDpmApiIdentity();

    StackInstance* si_;

    std::string dpmHost_;
    unsigned    retryLimit_;

    std::string tokenPasswd_;
    bool        tokenUseIp_;
    unsigned    tokenLife_;
    std::string userId_;

    // Need to keep this in memory, as dpns/dpm API do not make
    // copy of them (sigh)
    // setAuthorizationId does, though (don't ask me why)
    char **fqans_;
    size_t nFqans_;

    /// The corresponding factory.
    DpmAdapterFactory* factory_;

    const SecurityContext* secCtx_;
  };

};

#endif	// DPM_ADAPTER_H
