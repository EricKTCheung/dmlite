/// @file    plugins/hadoop/Hadoop.h
/// @brief   plugin to store data in a hadoop backend.
/// @author  Alexandre Beche <abeche@cern.ch>
#ifndef HADOOP_H
#define	HADOOP_H

#include <vector>

#include <dmlite/dm_io.h>
#include <dmlite/dmlite++.h>
#include <dmlite/common/Security.h>
#include <dmlite/dummy/Dummy.h>
#include <dmlite/common/Urls.h>

#include "hdfs.h"

namespace dmlite {

// IO Handler
class HadoopIOHandler: public IOHandler{
public:

  HadoopIOHandler(const std::string& pfn, std::iostream::openmode openmode) throw (DmException);
  ~HadoopIOHandler();

  void close(void) throw (DmException);
  size_t read(char* buffer, size_t count) throw (DmException);
  size_t write(const char* buffer, size_t count) throw (DmException);
  void seek(long offset, std::ios_base::seekdir whence) throw (DmException);
  long tell(void) throw (DmException);
  void flush(void) throw (DmException);
  bool eof(void) throw (DmException);
  struct stat pstat(void) throw (DmException);

private:
  hdfsFS fs;		// Hadoop file system
  hdfsFile file;	// Hadoop file descriptor
  bool isEof;		// Set to true if end of the file is reached
  std::string path_;
};


/// PoolDriver
class HadoopPoolDriver: public PoolDriver {
public:
  HadoopPoolDriver() throw (DmException);
  ~HadoopPoolDriver();
  
  void setStackInstance(StackInstance*) throw (DmException);
  void setSecurityContext(const SecurityContext*) throw (DmException);
  
  PoolHandler* createPoolHandler(const std::string& poolName) throw (DmException);
  
private:
  StackInstance* stack;
};

/// PoolHandler
class HadoopPoolHandler: public PoolHandler {
public:
  HadoopPoolHandler(const std::string& poolName, hdfsFS fs, StackInstance* si);
  ~HadoopPoolHandler();
  
  std::string getPoolType(void) throw (DmException);
  std::string getPoolName(void) throw (DmException);
  uint64_t getTotalSpace(void) throw (DmException);
  uint64_t getUsedSpace(void) throw (DmException);
  uint64_t getFreeSpace(void) throw (DmException);
  bool isAvailable(bool) throw (DmException);
  
  Location getLocation(const std::string&, const FileReplica&) throw (DmException);
  void remove         (const std::string&, const FileReplica&) throw (DmException);
  
  Location putLocation(const std::string&) throw (DmException);
  void     putDone    (const FileReplica&, const std::map<std::string, std::string>&) throw (DmException);
  
private:
  hdfsFS         fs;
  std::string    poolName;
  StackInstance* stack;
  
  bool replicaAvailable(const std::string&, const FileReplica&) throw (DmException);
};


/// IO Factory
class HadoopIOFactory: public IOFactory, public PoolDriverFactory {
public:
  HadoopIOFactory() throw (DmException);
  void configure(const std::string& key, const std::string& value) throw (DmException);
  
  IOHandler *createIO(const StackInstance*,
                      const std::string& pfn, std::iostream::openmode openmode,
                      const std::map<std::string, std::string>& extras) throw (DmException);
  
  struct stat pStat(const StackInstance* si,
                    const std::string& pfn) throw (DmException);

  std::string implementedPool() throw();
  PoolDriver* createPoolDriver() throw (DmException);

protected:
private:
};

};
#endif	// HADOOP_H
