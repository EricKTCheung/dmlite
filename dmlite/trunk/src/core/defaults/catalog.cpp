#include <dmlite/cpp/catalog.h>
#include "NotImplemented.h"
#include "utils/checksums.h"
#include "utils/logger.h"

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
NOT_IMPLEMENTED(ExtendedStat Catalog::extendedStatByRFN(const std::string&) throw (DmException));
NOT_IMPLEMENTED(bool Catalog::access(const std::string& path, int mode) throw (DmException));
NOT_IMPLEMENTED(bool Catalog::accessReplica(const std::string& rfn, int mode) throw (DmException));
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

void Catalog::setChecksum(const std::string &path, const std::string &csumtype, const std::string &csumvalue) throw (DmException) {
  
  // This is a convenience function, which could be overridden, but normally should not
  // We translate a legacy checksum (e.g. AD for adler32)  into the proper extended xattrs to be set
  // (e.g. checksum.adler32)
  // We can also pass a long checksum name (e.g. checksum.adler32)
  
  Log(Logger::Lvl4, Logger::unregistered, Logger::unregisteredname, " csumtype:" << csumtype << " csumvalue:" << csumvalue);
  
  ExtendedStat ckx = this->extendedStat(path);
  
  std::string k = csumtype;
  
  // If it looks like a legacy chksum then try to xlate its name
  if (csumtype.length() == 2)
    k = checksums::fullChecksumName(csumtype);
  
  if (!checksums::isChecksumFullName(k))
    throw DmException(EINVAL, "'" + csumtype + "' is not a valid checksum type.");
    
  if (csumvalue.length() == 0)
    throw DmException(EINVAL, "'" + csumvalue + "' is not a valid checksum value.");
  
  
  ckx[k] = csumvalue;
  updateExtendedAttributes(path, ckx);
  
  Log(Logger::Lvl3, Logger::unregistered, Logger::unregisteredname, "Exiting");
  
}


void Catalog::getChecksum(const std::string& path,
                          const std::string& csumtype,
                          std::string& csumvalue, const bool forcerecalc, const int waitsecs) throw (DmException) {
                          // We can also pass a long checksum name (e.g. checksum.adler32)

  // Gets a checksum of the required type. Utility function
  // By default it is extracted from the extendedstat information.
  // By default we assume that the backends are not able to calcolate it on the fly.
  // Other backends (e.g. DOME) may  support calculating it on the fly. In this case this func will have to be specialised in a plugin

  Log(Logger::Lvl4, Logger::unregistered, Logger::unregisteredname, "csumtype:" << csumtype << " path:" << path);
  
  ExtendedStat ckx = this->extendedStat(path);
  
  std::string k = csumtype;
  
  // If it looks like a legacy chksum then try to xlate its name
  if (csumtype.length() == 2)
    k = checksums::fullChecksumName(csumtype);
                            
  if (!checksums::isChecksumFullName(k))
    throw DmException(EINVAL, "'" + csumtype + "' is not a valid checksum type.");
    
  if (csumvalue.length() == 0)
    throw DmException(EINVAL, "'" + csumvalue + "' is not a valid checksum value.");
  
  csumvalue = ckx.getString((const std::string)k, "");
  
  if (csumvalue.length() > 0) return;
  
  // If we did not find the wanted chksum in the hash, then we may want
  // to calculate it in a plugin that is more specialized than this one
  if (forcerecalc) {
    
      throw DmException(EINVAL, "'" + csumtype + "' cannot be calculated by the base Catalog implementation. You may want to use a more specialized plugin.");
    
  }
  
}

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
