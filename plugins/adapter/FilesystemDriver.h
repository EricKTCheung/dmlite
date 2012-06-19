/// @file   plugins/adapter/FilesystemDriver.h
/// @brief  Regular Filesystem pool
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef FILESYSTEMDRIVER_H
#define	FILESYSTEMDRIVER_H

#include <dmlite/dm_pooldriver.h>
#include <dpm_api.h>

namespace dmlite {
  
/// Filesystem driver.
class FilesystemPoolDriver: public PoolDriver {
public:
  FilesystemPoolDriver(StackInstance*, const Pool&,
                       const std::string&, bool, unsigned);
  ~FilesystemPoolDriver();
   
  void setSecurityContext(const SecurityContext*) throw (DmException);

  std::string getPoolType(void) throw (DmException);
  std::string getPoolName(void) throw (DmException);
  uint64_t getTotalSpace(void) throw (DmException);
  uint64_t getFreeSpace(void) throw (DmException);
  bool isAvailable(bool) throw (DmException);

  Location getLocation (const std::string&, const FileReplica&) throw (DmException);
  void remove          (const std::string&, const FileReplica&) throw (DmException);
  
  Location putLocation(const std::string&) throw (DmException);
  void putDone(const FileReplica&,
               const std::map<std::string, std::string>&) throw (DmException);

private:
  const SecurityContext* secCtx_;
  PoolManager* manager_;
  Pool         pool_;
  uint64_t     total_, free_;
  
  std::string tokenPasswd_;
  bool        tokenUseIp_;
  unsigned    tokenLife_;
  const char* userId_;

  void update(void) throw (DmException);
  std::vector<dpm_fs> getFilesystems(const std::string&) throw (DmException);
  bool replicaAvailable(const std::string&, const FileReplica&) throw (DmException);
};

};

#endif	// FILESYSTEMDRIVER_H
