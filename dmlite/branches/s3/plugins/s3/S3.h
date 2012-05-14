/// @file    plugins/s3/S3.h
/// @brief   plugin to store data in a s3 backend.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
#ifndef S3_H
#define	S3_H

#include <vector>

#include <dmlite/dm_io.h>
#include <dmlite/dmlite++.h>
#include <dmlite/common/Security.h>
#include <dmlite/dummy/Dummy.h>
#include <dmlite/common/Uris.h>

#include "libs3.h"
#include <libaws/aws.h>

namespace dmlite {

#define CACHE_SIZE 5*1024*1024 // 5 MByte

class S3IOHandler: public IOHandler{
public:

  S3IOHandler(const std::string& uri, std::iostream::openmode openmode) throw (DmException);
  ~S3IOHandler();

  void close(void) throw (DmException);
  size_t read(char* buffer, size_t count) throw (DmException);
  size_t write(const char* buffer, size_t count) throw (DmException);
  void seek(long offset, std::ios_base::seekdir whence) throw (DmException);
  long tell(void) throw (DmException);
  void flush(void) throw (DmException);
  bool eof(void) throw (DmException);
  struct stat pstat(void) throw (DmException);

private:
  bool isEof;		// Set to true if end of the file is reached
  std::string path_;

  struct UploadCache {
    char cache[CACHE_SIZE];
    uint64_t contentLength;
  };

  /// The S3 host to connect to.
  std::string host_;

  /// The bucket to use.
  std::string bucketName_;

  /// The Amazon Access ID.
  std::string s3AccessKeyID_;
  
  /// The Secret Access Key.
  std::string s3SecretAccessKey_;
};

class S3PoolHandler: public PoolHandler {
public:
  S3PoolHandler(StackInstance*, const Pool& pool, 
                std::string bucketName,
                std::string s3AccessKeyID,
                std::string s3SecretAccessKey) throw (DmException);

  ~S3PoolHandler();
  
  void setSecurityContext(const SecurityContext*) throw (DmException);

  std::string getPoolType(void) throw (DmException);
  std::string getPoolName(void) throw (DmException);
  uint64_t getTotalSpace(void) throw (DmException);
  uint64_t getUsedSpace(void) throw (DmException);
  uint64_t getFreeSpace(void) throw (DmException);
  bool isAvailable(bool) throw (DmException);
  
  bool replicaAvailable(const std::string&, const FileReplica&) throw (DmException);
  Uri  getLocation     (const std::string&, const FileReplica&) throw (DmException);
  void remove          (const std::string&, const FileReplica&) throw (DmException);
  
  std::string putLocation (const std::string&, Uri*) throw (DmException);
  void putDone(const std::string&, const Uri&, const std::string&) throw (DmException);

private:
  StackInstance* stack;
  Pool           pool;
  
  /// The S3 host to connect to.
  std::string host_;

  /// The bucket to use.
  std::string bucketName_;

  /// The Amazon Access ID.
  std::string s3AccessKeyID_;
  
  /// The Secret Access Key.
  std::string s3SecretAccessKey_;

  /// The S3 Connection.
  S3ConnectionPtr lS3Rest;
};

class S3IOFactory: public IOFactory, public PoolHandlerFactory {
public:
  S3IOFactory() throw (DmException);
  void configure(const std::string& key, const std::string& value) throw (DmException);
  IOHandler *createIO(const std::string& uri, std::iostream::openmode openmode) throw (DmException);

  std::string implementedPool() throw();
  PoolHandler* createPoolHandler(StackInstance*, const Pool&) throw (DmException);

protected:

  /// The S3 host to connect to.
  std::string s3host_;

  /// The bucket to use.
  std::string bucketName_;

  /// The Amazon Access ID.
  std::string s3AccessKeyID_;
  
  /// The Secret Access Key.
  std::string s3SecretAccessKey_;
private:
};

};
#endif	// S3_H
