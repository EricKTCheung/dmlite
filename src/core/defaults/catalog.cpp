#include <dmlite/cpp/catalog.h>
#include "NotImplemented.h"


using namespace dmlite;



CatalogFactory::~CatalogFactory()
{
  // Nothing
}



Catalog* CatalogFactory::createCatalog(CatalogFactory* factory, PluginManager* pm) throw (DmException)
{
  return factory->createCatalog(pm);
}



FACTORY_NOT_IMPLEMENTED(Catalog* CatalogFactory::createCatalog(PluginManager* pm) throw (DmException));



Directory::~Directory()
{
  // Nothing
}



Catalog::~Catalog()
{
  // Nothing
}



NOT_IMPLEMENTED(void Catalog::changeDir(const std::string&) throw (DmException));
NOT_IMPLEMENTED(std::string Catalog::getWorkingDir(void) throw (DmException));
NOT_IMPLEMENTED(ExtendedStat Catalog::extendedStat(const std::string&, bool) throw (DmException));
NOT_IMPLEMENTED(void Catalog::addReplica(const Replica&) throw (DmException));
NOT_IMPLEMENTED(void Catalog::deleteReplica(const Replica&) throw (DmException));
NOT_IMPLEMENTED(std::vector<Replica> Catalog::getReplicas(const std::string&) throw (DmException));
NOT_IMPLEMENTED(void Catalog::symlink(const std::string&, const std::string&) throw (DmException));
NOT_IMPLEMENTED(std::string Catalog::readLink(const std::string&) throw (DmException));
NOT_IMPLEMENTED(void Catalog::unlink(const std::string&) throw (DmException));
NOT_IMPLEMENTED(void Catalog::create(const std::string&, mode_t) throw (DmException));

mode_t Catalog::umask(mode_t) throw () {
  return 0;
}

NOT_IMPLEMENTED(void Catalog::setMode(const std::string&, mode_t) throw (DmException));
NOT_IMPLEMENTED(void Catalog::setOwner(const std::string&, uid_t, gid_t, bool) throw (DmException));
NOT_IMPLEMENTED(void Catalog::setSize(const std::string&, size_t) throw (DmException));
NOT_IMPLEMENTED(void Catalog::setChecksum(const std::string&, const std::string&, const std::string&) throw (DmException));
NOT_IMPLEMENTED(void Catalog::setAcl(const std::string&, const Acl&) throw (DmException));
NOT_IMPLEMENTED(void Catalog::utime(const std::string&, const struct utimbuf*) throw (DmException));
NOT_IMPLEMENTED(std::string Catalog::getComment(const std::string&) throw (DmException));
NOT_IMPLEMENTED(void Catalog::setComment(const std::string&, const std::string&) throw (DmException));
NOT_IMPLEMENTED(void Catalog::setGuid(const std::string&, const std::string&) throw (DmException));
NOT_IMPLEMENTED(void Catalog::updateExtendedAttributes(const std::string&, const Extensible&) throw (DmException));
NOT_IMPLEMENTED(Directory* Catalog::openDir(const std::string&) throw (DmException));
NOT_IMPLEMENTED(void Catalog::closeDir(Directory*) throw (DmException));
NOT_IMPLEMENTED(struct dirent* Catalog::readDir(Directory*) throw (DmException));
NOT_IMPLEMENTED(ExtendedStat* Catalog::readDirx(Directory*) throw (DmException));
NOT_IMPLEMENTED(void Catalog::makeDir(const std::string&, mode_t) throw (DmException));
NOT_IMPLEMENTED(void Catalog::rename(const std::string&, const std::string&) throw (DmException));
NOT_IMPLEMENTED(void Catalog::removeDir(const std::string&) throw (DmException));
NOT_IMPLEMENTED(Replica Catalog::getReplicaByRFN(const std::string&) throw (DmException));
NOT_IMPLEMENTED(void Catalog::updateReplica(const Replica&) throw (DmException));
