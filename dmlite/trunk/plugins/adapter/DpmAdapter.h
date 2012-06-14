/// @file   plugins/adapter/DpmAdapter.h
/// @brief  DPM wrapper (implements get, put, putDone, etc.)
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DPM_ADAPTER_H
#define	DPM_ADAPTER_H

#include "NsAdapter.h"
#include <dmlite/dm_pool.h>
#include <syslog.h>

namespace dmlite {

/// Overrides some functions using the DPM API.
class DpmAdapterCatalog: public NsAdapterCatalog {
public:
  /// Constructor
  /// @param retryLimit The limit of retrials.
  DpmAdapterCatalog(unsigned retryLimit, StackInstance* si) throw (DmException);

  /// Destructor
  ~DpmAdapterCatalog();

  // Overload
  std::string getImplId(void) throw ();

  void setSecurityContext(const SecurityContext*) throw (DmException);
  
  Uri         get      (const std::string&)       throw (DmException);
  std::string put      (const std::string&, Uri*) throw (DmException);
  std::string put      (const std::string&, Uri*, const std::string&) throw (DmException);
  void        putDone  (const std::string&, const Uri&, const std::string&) throw (DmException);
  void        unlink   (const std::string&)                           throw (DmException);
 
private:
  std::string    dpmHost_;
  StackInstance* si_;
};



class DpmAdapterPoolManager: public PoolManager {
public:
  DpmAdapterPoolManager(unsigned retryLimit) throw (DmException);
  ~DpmAdapterPoolManager();

  std::string getImplId() throw ();

  void setSecurityContext(const SecurityContext*) throw (DmException);
  
  PoolMetadata* getPoolMetadata(const Pool& pool) throw (DmException);

  std::vector<Pool> getPools(void) throw (DmException);
  Pool getPool(const std::string& poolname) throw (DmException);
  
  virtual std::vector<Pool> getAvailablePools(bool write = true) throw (DmException);
  
private:
  std::string dpmHost_;
  unsigned    retryLimit_;
};


/// Used to retry n times before failing.
#define RETRY(f, n) \
for(unsigned ri = 0;;++ri)\
{\
  if (f >= 0)\
    break;\
  else if (ri == n) {\
    syslog(LOG_USER | LOG_DEBUG, #f" failed (%d), %d retries exhausted", serrno, n);\
    ThrowExceptionFromSerrno(serrno);\
  }\
  syslog(LOG_USER | LOG_DEBUG, #f" failed (%d), retrying %d...", serrno, ri);\
}

};

#endif	// DPM_ADAPTER_H
