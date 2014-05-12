/// @file    plugins/proc/Proc.h
/// @brief   This plugin can be used to enable metadata as files
/// @author  Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#ifndef PLUGIN_PROC_H
#define PLUGIN_PROC_H

#include <list>
#include <dmlite/cpp/catalog.h>
#include <dmlite/cpp/io.h>
#include <dmlite/cpp/poolmanager.h>
#include <dmlite/cpp/pooldriver.h>
#include "Provider.h"


namespace dmlite {
  class ProcFactory: public CatalogFactory, public PoolManagerFactory, public IOFactory {
  public:
    ProcFactory();
    virtual ~ProcFactory();

    void configure(const std::string& key, const std::string& value) throw (DmException);

    Catalog* createCatalog(PluginManager* pm) throw (DmException);

    PoolManager* createPoolManager(PluginManager* pm) throw (DmException);

    IODriver* createIODriver(PluginManager* pm) throw (DmException);
  };

  /// Proc catalog
  class ProcCatalog: public Catalog {
  public:
    ProcCatalog();
    virtual ~ProcCatalog();

    std::string getImplId() const throw();
    void setStackInstance(StackInstance* si) throw (DmException);
    void setSecurityContext(const SecurityContext* ctx) throw (DmException);

    void changeDir(const std::string& path) throw (DmException);
    std::string getWorkingDir(void) throw (DmException);

    ExtendedStat extendedStat(const std::string& path, bool followSym) throw (DmException);

    Directory*     openDir(const std::string& path) throw (DmException);
    void           closeDir(Directory* dir) throw (DmException);
    struct dirent* readDir(Directory* dir) throw (DmException);
    ExtendedStat*  readDirx(Directory* dir) throw (DmException);

    std::vector<Replica> getReplicas(const std::string& path) throw (DmException);

  protected:
    ProcProvider* root_;
    std::string cwd_;
  };

  // Proc pool manager
  class ProcPoolManager: public PoolManager {
  public:
    ProcPoolManager();
    virtual ~ProcPoolManager();

    std::string getImplId(void) const throw();

    void setStackInstance(StackInstance* si) throw (DmException);
    void setSecurityContext(const SecurityContext* ctx) throw (DmException);

    Location whereToRead(const std::string& path) throw (DmException);

  protected:
    StackInstance* si_;
  };
}

#endif // PLUGIN_PROC_H
