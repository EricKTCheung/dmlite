/// @file   DomeAdapterCatalog.h
/// @brief  Dome adapter
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
#ifndef DOME_ADAPTER_CATALOG_H
#define	DOME_ADAPTER_CATALOG_H

#include <dmlite/cpp/catalog.h>
#include <dmlite/cpp/dmlite.h>

namespace dmlite {

 struct DomeDir: public Directory {
    virtual ~DomeDir() {};
    std::string path_;
    DomeDir(std::string path) : path_(path) {}
  };

  class DomeAdapterCatalog: public Catalog, public Authn {
  public:
  	/// Constructor
    DomeAdapterCatalog() throw (DmException);

    // Overload
    std::string getImplId(void) const throw ();

    void setStackInstance(StackInstance* si) throw (DmException);
    void setSecurityContext(const SecurityContext* secCtx) throw (DmException);

    void        changeDir     (const std::string&) throw (DmException);
    std::string getWorkingDir (void)               throw (DmException);

    Directory* openDir (const std::string&) throw (DmException);
    void       closeDir(Directory*)         throw (DmException);

    struct dirent* readDir (Directory*) throw (DmException);
    ExtendedStat*  readDirx(Directory*) throw (DmException);

  protected:
    StackInstance* si_;
    const SecurityContext* secCtx_;

    std::string cwd_;
  };
}


#endif
