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

#include "S3Factory.h"

#include "s3driver.h"
#include "s3objects.pb.h"

namespace dmlite {

/// PoolDriver
class S3PoolDriver: public PoolDriver {
public:
  S3PoolDriver(S3Factory *factory) throw (DmException);

  ~S3PoolDriver();
  
  void setStackInstance(StackInstance*) throw (DmException);
  void setSecurityContext(const SecurityContext*) throw (DmException);

  PoolHandler* createPoolHandler(const std::string& poolName) throw (DmException);

private:
  friend class S3PoolHandler;

  StackInstance* stack; 

  S3Factory*     factory_;
};

/// PoolHandler
class S3PoolHandler: public PoolHandler {
public:
  S3PoolHandler(S3Factory *factory, const std::string& poolName, StackInstance* si);
  ~S3PoolHandler();

  std::string getPoolType(void) throw (DmException);
  std::string getPoolName(void) throw (DmException);
  uint64_t getTotalSpace(void) throw (DmException);
  uint64_t getUsedSpace(void) throw (DmException);
  uint64_t getFreeSpace(void) throw (DmException);
  bool isAvailable(bool) throw (DmException);
  
  /// Return the location of the replica.
  /// This function returns the replica location to which the client can 
  /// connect directly. The location is a signed link to S3 valid for a given time.
  /// For compatibility with dmlite 0.3, this function performs a check, if the
  /// replica has been successfully uploaded. In later version, this functionality 
  /// will be called from outside and can be removed here.
  /// @param replica      The file replica.
  /// @return             The file location.
  Location getLocation (const FileReplica&) throw (DmException);
  void remove          (const FileReplica&) throw (DmException);
  
  Location putLocation (const std::string&) throw (DmException);
  void putDone(const FileReplica& replica, const std::map<std::string, std::string>& extras) throw (DmException);


private:
  S3Factory*     factory_;

  std::string    bucket_;

  /// The S3 Connection.
  S3Driver*      conn_;
  std::string    poolName; 
  StackInstance* stack;  


  /// Check, if the replica exists on S3.
  /// If the replica has status 'Pending', it checks if the upload has
  /// been completed successfully.
  /// If it is marked as deleted, it return false, if '-' (active), it
  /// return true.
  /// Any other status returns false.
  /// @param replica      The file replica.
  /// @return             Whether the file is available or not.
  bool replicaAvailable(const FileReplica&) throw (DmException);
};

/// IO Factory

};
#endif	// S3_H
