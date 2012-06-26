/// @file   plugins/adapter/FilesystemDriver.h
/// @brief  Regular Filesystem pool
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef FILESYSTEMDRIVER_H
#define	FILESYSTEMDRIVER_H

#include <dmlite/cpp/dm_pooldriver.h>
#include <dpm_api.h>

namespace dmlite {
  
/// Filesystem driver.
class FilesystemPoolDriver: public PoolDriver {
public:
  FilesystemPoolDriver(const std::string&, bool, unsigned);
  ~FilesystemPoolDriver();
   
  void setStackInstance(StackInstance* si) throw (DmException);
  void setSecurityContext(const SecurityContext*) throw (DmException);
  
  PoolHandler* createPoolHandler(const std::string& poolName) throw (DmException);
  
  struct stat pStat(const std::string& pfn) throw (DmException);
  
private:
  friend class FilesystemPoolHandler;
  
  const SecurityContext* secCtx_;
  
  std::string tokenPasswd_;
  bool        tokenUseIp_;
  unsigned    tokenLife_;
  const char* userId_;
};

class FilesystemPoolHandler: public PoolHandler {
public:
  FilesystemPoolHandler(FilesystemPoolDriver*, const std::string& poolName);
  ~FilesystemPoolHandler();
  
  std::string getPoolType(void) throw (DmException);
  std::string getPoolName(void) throw (DmException);
  uint64_t getTotalSpace(void) throw (DmException);
  uint64_t getFreeSpace(void) throw (DmException);
  bool isAvailable(bool) throw (DmException);

  Location getLocation(const FileReplica& replica) throw (DmException);
  void remove         (const FileReplica&) throw (DmException);
  
  Location putLocation(const std::string&) throw (DmException);
  void putDone(const FileReplica&,
               const std::map<std::string, std::string>&) throw (DmException);

private:
  FilesystemPoolDriver* driver_;
  std::string           poolName_;
  uint64_t              total_, free_;

  void update(void) throw (DmException);
  std::vector<dpm_fs> getFilesystems(const std::string&) throw (DmException);
  bool replicaAvailable(const FileReplica&) throw (DmException);
};

};

#endif	// FILESYSTEMDRIVER_H
