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
    DomeAdapterHeadCatalogFactory();
    virtual ~DomeAdapterHeadCatalogFactory();

    void configure(const std::string& key, const std::string& value) throw (DmException);
    Catalog *createCatalog(PluginManager* pm) throw (DmException);
  private:
    std::string domehead_;

    DavixCtxFactory davixFactory_;
    DavixCtxPool davixPool_;

    friend class DomeAdapterHeadCatalog;
  };

  class DomeAdapterHeadCatalog : public Catalog {
  public:
    DomeAdapterHeadCatalog(DomeAdapterHeadCatalogFactory *factory);
    virtual ~DomeAdapterHeadCatalog();

    std::string getImplId() const throw();

    void setStackInstance(StackInstance* si) throw (DmException);

    void setSecurityContext(const SecurityContext* ctx) throw (DmException);

    void changeDir(const std::string& path) throw (DmException);
    std::string getWorkingDir() throw (DmException);

    DmStatus extendedStat(ExtendedStat &xstat, const std::string&, bool) throw (DmException);
    ExtendedStat extendedStat(const std::string&, bool followSym = true) throw (DmException);
    ExtendedStat extendedStatByRFN(const std::string& rfn)  throw (DmException);

    bool access(const std::string&, int) throw (DmException);
    bool accessReplica(const std::string& replica, int mode) throw (DmException);

    void addReplica   (const Replica&) throw (DmException);
    void deleteReplica(const Replica&) throw (DmException);
    std::vector<Replica> getReplicas(const std::string&) throw (DmException);

    void symlink(const std::string&, const std::string&) throw (DmException);
    std::string readLink(const std::string& path) throw (DmException);

    void unlink(const std::string& path) throw (DmException);

    void create(const std::string& path, mode_t mode) throw (DmException);

    // mode_t umask   (mode_t)                           throw ();
    void   setMode (const std::string&, mode_t)       throw (DmException);
    void   setOwner(const std::string&, uid_t, gid_t, bool) throw (DmException);

    void setSize(const std::string& path, size_t newSize) throw (DmException);

    void setAcl(const std::string&, const Acl&) throw (DmException);

    Replica getReplica   (int64_t rid) throw (DmException);
    // Replica getReplica   (const std::string& sfn) throw (DmException);







    virtual void getChecksum(const std::string& path,
                             const std::string& csumtype,
                             std::string& csumvalue,
                             const std::string& pfn,
                             const bool forcerecalc = false, const int waitsecs = 0) throw (DmException);






    std::string getComment(const std::string& path) throw (DmException);
    void setComment(const std::string& path, const std::string& comment) throw (DmException);
    // void deleteComment(const std::string& path) throw (DmException);

    void setGuid(const std::string& path, const std::string& guid) throw (DmException);

    void updateExtendedAttributes(const std::string&,
                                  const Extensible&) throw (DmException);

    Directory* openDir (const std::string&) throw (DmException);
    void       closeDir(Directory*)         throw (DmException);

    ExtendedStat*  readDirx(Directory*) throw (DmException);
    struct dirent* readDir(Directory* dir) throw (DmException);

    void makeDir(const std::string& path, mode_t mode) throw (DmException);

    void rename(const std::string& oldPath, const std::string& newPath) throw (DmException);
    void removeDir(const std::string& path) throw (DmException);

    Replica getReplicaByRFN(const std::string& rfn) throw (DmException);
    void    updateReplica(const Replica& replica) throw (DmException);

   private:
     std::string absPath(const std::string &relpath);

     struct DomeDir : public Directory {
       std::string path_;
       size_t pos_;
       std::vector<dmlite::ExtendedStat> entries_;
       std::vector<dirent> dirents_;

       virtual ~DomeDir() {}
       DomeDir(std::string path) : path_(path), pos_(0) {}
     };

     std::string cwdPath_;

     const SecurityContext* secCtx_;
     StackInstance* si_;

     DomeAdapterHeadCatalogFactory &factory_;
  };
}

#endif
