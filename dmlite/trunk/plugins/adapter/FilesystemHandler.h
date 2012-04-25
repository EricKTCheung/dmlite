/// @file   plugins/adapter/FilesystemHandler.h
/// @brief  Regular Filesystem pool
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef FILESYSTEMHANDLER_H
#define	FILESYSTEMHANDLER_H

#include <dmlite/dm_poolhandler.h>
#include <dpm_api.h>

namespace dmlite {
  
/// Filesystem handler.
class FilesystemPoolHandler: public PoolHandler {
public:
  FilesystemPoolHandler(PoolManager*, Pool*);
  ~FilesystemPoolHandler();
  
  void setSecurityContext(const SecurityContext*) throw (DmException);

  std::string getPoolType(void) throw (DmException);
  std::string getPoolName(void) throw (DmException);
  uint64_t getTotalSpace(void) throw (DmException);
  uint64_t getFreeSpace(void) throw (DmException);
  bool isAvailable(bool) throw (DmException);

  bool replicaAvailable(const std::string&, const FileReplica&) throw (DmException);
  Uri  getLocation     (const std::string&, const FileReplica&) throw (DmException);
  void remove          (const std::string&, const FileReplica&) throw (DmException);
  
  std::string putLocation(const std::string&, Uri*) throw (DmException);
  void putDone(const std::string&, const Uri&, const std::string&) throw (DmException);

private:
  PoolManager *manager_;
  Pool        *pool_;
  uint64_t     total_, free_;

  void update(void) throw (DmException);
  std::vector<dpm_fs> getFilesystems(const std::string&) throw (DmException);
};

};

#endif	// FILESYSTEMHANDLER_H
