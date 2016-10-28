/// @file   DomeAdapterDiskCatalog.h
/// @brief  Dome adapter
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
#ifndef DOME_ADAPTER_CATALOG_H
#define	DOME_ADAPTER_CATALOG_H

#include <dmlite/cpp/catalog.h>
#include <dmlite/cpp/dmlite.h>
#include "DomeAdapter.h"

namespace dmlite {

  extern Logger::bitmask domeadapterlogmask;
  extern Logger::component domeadapterlogname;

  struct DomeDir : public Directory {
    std::string path_;
    size_t pos_;
    std::vector<dmlite::ExtendedStat> entries_;

    virtual ~DomeDir() {}
    DomeDir(std::string path) : path_(path), pos_(0) {}
  };

  class DomeAdapterDiskCatalog: public Catalog, public Authn {
  public:
  	/// Constructor
    DomeAdapterDiskCatalog(DomeAdapterFactory *factory) throw (DmException);

    // Overload
    std::string getImplId(void) const throw ();

    void setStackInstance(StackInstance* si) throw (DmException);
    void setSecurityContext(const SecurityContext* secCtx) throw (DmException);

    SecurityContext* createSecurityContext(const SecurityCredentials& cred) throw (DmException);
    SecurityContext* createSecurityContext() throw (DmException);

    virtual void getChecksum(const std::string& path,
                             const std::string& csumtype,
                             std::string& csumvalue,
                             const std::string& pfn,
                             const bool forcerecalc = false, const int waitsecs = 0) throw (DmException);
                             
    ExtendedStat extendedStat(const std::string&, bool) throw (DmException);
    ExtendedStat extendedStatByRFN(const std::string &) throw (DmException);

    void getIdMap(const std::string& userName,
                  const std::vector<std::string>& groupNames,
                  UserInfo* user,
                  std::vector<GroupInfo>* groups) throw (DmException);

    bool accessReplica(const std::string& replica, int mode) throw (DmException);
    Replica getReplicaByRFN(const std::string& rfn) throw (DmException);

    Directory* openDir (const std::string&) throw (DmException);
    void       closeDir(Directory*)         throw (DmException);
    ExtendedStat*  readDirx(Directory*) throw (DmException);

    void updateExtendedAttributes(const std::string&,
                                  const Extensible&) throw (DmException);

  protected:
    StackInstance* si_;
    const SecurityContext *sec_;
    DomeAdapterFactory* factory_;

    std::string cwd_;
  };
}


#endif
