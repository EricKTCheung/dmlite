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
  /// @param dpmHost    The DPM host
  /// @param retryLimit The limit of retrials.
  DpmAdapterCatalog(const std::string& dpmHost, unsigned retryLimit) throw (DmException);

  /// Destructor
  ~DpmAdapterCatalog();

  // Overload
  std::string getImplId(void) throw ();

  void set(const std::string&, va_list) throw (DmException);

  void setSecurityCredentials(const SecurityCredentials&) throw (DmException);
  void setSecurityContext(const SecurityContext&);
  
  FileReplica get      (const std::string&)       throw (DmException);
  std::string put      (const std::string&, Uri*) throw (DmException);
  std::string put      (const std::string&, Uri*, const std::string&) throw (DmException);
  void        putStatus(const std::string&, const std::string&, Uri*) throw (DmException);
  void        putDone  (const std::string&, const std::string&)       throw (DmException);
  void        unlink   (const std::string&)                           throw (DmException);
protected:
private:
  std::string dpmHost_;
  std::string spaceToken_;
};



class DpmAdapterPoolManager: public PoolManager {
public:
  DpmAdapterPoolManager(const std::string& dpmHost, unsigned retryLimit) throw (DmException);
  ~DpmAdapterPoolManager();

  std::string getImplId() throw ();

  void setSecurityCredentials(const SecurityCredentials&) throw (DmException);
  const SecurityContext& getSecurityContext() throw (DmException);
  void setSecurityContext(const SecurityContext&);

  std::vector<Pool> getPools(void) throw (DmException);

protected:
  SecurityContext secCtx_;
  
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
