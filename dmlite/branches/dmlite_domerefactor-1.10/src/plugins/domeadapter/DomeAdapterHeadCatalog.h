/// @file   DomeAdapterIO.h
/// @brief  Filesystem IO using dome.
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
#ifndef DOME_ADAPTER_HEAD_CATALOG_H
#define DOME_ADAPTER_HEAD_CATALOG_H

#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/catalog.h>
#include <dmlite/cpp/dummy/DummyCatalog.h>
#include <fstream>
#include "utils/DavixPool.h"

namespace dmlite {
  extern Logger::bitmask domeadapterlogmask;
  extern Logger::component domeadapterlogname;

  class DomeAdapterHeadCatalog;

  class DomeAdapterHeadCatalogFactory : public CatalogFactory {
  public:
    DomeAdapterHeadCatalogFactory(CatalogFactory *nested);
    virtual ~DomeAdapterHeadCatalogFactory();

    void configure(const std::string& key, const std::string& value) throw (DmException);
    Catalog *createCatalog(PluginManager* pm) throw (DmException);
  private:
    CatalogFactory *nested_;
    std::string domehead_;

    DavixCtxFactory davixFactory_;
    DavixCtxPool davixPool_;

    friend class DomeAdapterHeadCatalog;
  };

  class DomeAdapterHeadCatalog : public DummyCatalog {
  public:
    DomeAdapterHeadCatalog(DomeAdapterHeadCatalogFactory *factory, Catalog *nested);
    virtual ~DomeAdapterHeadCatalog();

    std::string getImplId() const throw();

    void setSecurityContext(const SecurityContext* ctx) throw (DmException);
    void setStackInstance(StackInstance* si) throw (DmException);

    DmStatus extendedStat(ExtendedStat &xstat, const std::string&, bool) throw (DmException);
    ExtendedStat extendedStat(const std::string&, bool followSym = true) throw (DmException);

    void changeDir(const std::string& path) throw (DmException);

    // std::vector<Replica> getReplicas(const std::string& path) throw (DmException);
    void deleteReplica(const Replica&) throw (DmException);

    virtual void getChecksum(const std::string& path,
                             const std::string& csumtype,
                             std::string& csumvalue,
                             const std::string& pfn,
                             const bool forcerecalc = false, const int waitsecs = 0) throw (DmException);

    void makeDir(const std::string& path, mode_t mode) throw (DmException);
    void create(const std::string& path, mode_t mode) throw (DmException);
    void removeDir(const std::string& path) throw (DmException);
    void setGuid(const std::string& path, const std::string& guid) throw (DmException);
    void setSize(const std::string& path, size_t newSize) throw (DmException);

    void rename(const std::string& oldPath, const std::string& newPath) throw (DmException);
    void unlink(const std::string& path) throw (DmException);

    Directory* openDir (const std::string&) throw (DmException);
    void       closeDir(Directory*)         throw (DmException);
    ExtendedStat*  readDirx(Directory*) throw (DmException);
   private:
     struct DomeDir : public Directory {
       std::string path_;
       size_t pos_;
       std::vector<dmlite::ExtendedStat> entries_;

       virtual ~DomeDir() {}
       DomeDir(std::string path) : path_(path), pos_(0) {}
     };

     std::string cwdPath_;

     Catalog *decorated_;
     std::string decorated_id;
     const SecurityContext* secCtx_;
     StackInstance* si_;

     DomeAdapterHeadCatalogFactory &factory_;
  };
}

#endif
