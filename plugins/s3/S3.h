/// @file    plugins/s3/S3.h
/// @brief   plugin to store data in a s3 backend.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
#ifndef S3_H
#define	S3_H

#include <vector>

#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/utils/dm_security.h>
#include <dmlite/cpp/dummy/Dummy.h>
#include <dmlite/cpp/utils/dm_urls.h>

#include "s3driver.h"
//#include "libs3.h"
//#include <libaws/aws.h>

namespace dmlite {

/// PoolDriver
class S3PoolDriver: public PoolDriver {
public:
  S3PoolDriver(std::string host, std::string bucket, std::string s3AccessKeyID, std::string s3SecretAccessKey) throw (DmException);

  ~S3PoolDriver();
  
  void setStackInstance(StackInstance*) throw (DmException);
  void setSecurityContext(const SecurityContext*) throw (DmException);

  PoolHandler* createPoolHandler(const std::string& poolName) throw (DmException);

private:
  friend class S3PoolHandler;

  StackInstance* stack; 

  /// The S3 host to connect to.
  std::string host_;

  /// The bucket to use.
  std::string bucketName_;

  /// The Amazon Access ID.
  std::string s3AccessKeyID_;
  
  /// The Secret Access Key.
  std::string s3SecretAccessKey_;

  /// The S3 Connection.
  S3Driver s3connection_;
};

/// PoolHandler
class S3PoolHandler: public PoolHandler {
public:
  S3PoolHandler(S3PoolDriver* driver, const std::string& poolName);
  ~S3PoolHandler();

  std::string getPoolType(void) throw (DmException);
  std::string getPoolName(void) throw (DmException);
  uint64_t getTotalSpace(void) throw (DmException);
  uint64_t getUsedSpace(void) throw (DmException);
  uint64_t getFreeSpace(void) throw (DmException);
  bool isAvailable(bool) throw (DmException);
  
  Location getLocation (const FileReplica&) throw (DmException);
  void remove          (const FileReplica&) throw (DmException);
  
  Location putLocation (const std::string&) throw (DmException);
  void putDone(const FileReplica& replica, const std::map<std::string, std::string>& extras) throw (DmException);


private:
  S3PoolDriver*  driver_;
  std::string    poolName; 

  bool replicaAvailable(const FileReplica&) throw (DmException);
};

/// IO Factory
class S3Factory: public PoolDriverFactory {
public:
  S3Factory() throw (DmException);
  void configure(const std::string& key, const std::string& value) throw (DmException);

  std::string implementedPool() throw();
  PoolDriver* createPoolDriver() throw (DmException);

protected:

private:
  /// The S3 host to connect to.
  std::string host_;

  /// The bucket to use.
  std::string bucketName_;

  /// The Amazon Access ID.
  std::string s3AccessKeyID_;
  
  /// The Secret Access Key.
  std::string s3SecretAccessKey_;
};

};
#endif	// S3_H
