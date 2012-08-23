/// @file    plugins/hadoop/Hadoop.h
/// @brief   plugin to store data in a hadoop backend.
/// @author  Alexandre Beche <abeche@cern.ch>
#ifndef HADOOP_H
#define	HADOOP_H

#include <vector>

#include <dmlite/cpp/dm_io.h>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/dummy/Dummy.h>
#include <dmlite/cpp/utils/dm_urls.h>

#include "hdfs.h"

namespace dmlite {
  
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
  
  Location getLocation(const FileReplica&) throw (DmException);
  void     remove     (const FileReplica&) throw (DmException);
  
  Location putLocation(const std::string&) throw (DmException);
  void     putDone    (const FileReplica&, const std::map<std::string, std::string>&) throw (DmException);
  
private:
  hdfsFS         fs;
  std::string    poolName;
  StackInstance* stack;
  
  bool replicaAvailable(const FileReplica&) throw (DmException);
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



// IO Handler
class HadoopIODriver;

class HadoopIOHandler: public IOHandler{
public:

  HadoopIOHandler(HadoopIODriver* driver, const std::string& pfn, int openmode) throw (DmException);
  ~HadoopIOHandler();

  void close(void) throw (DmException);
  size_t read(char* buffer, size_t count) throw (DmException);
  size_t write(const char* buffer, size_t count) throw (DmException);
  void seek(long offset, int whence) throw (DmException);
  long tell(void) throw (DmException);
  void flush(void) throw (DmException);
  bool eof(void) throw (DmException);

private:
  HadoopIODriver* driver;
  
  hdfsFile file;  // Hadoop file descriptor
  bool     isEof; // Set to true if end of the file is reached
  std::string path;
};



/// IO Driver
class HadoopIODriver: public IODriver {
public:
  HadoopIODriver();
  ~HadoopIODriver();
  
  void setStackInstance(StackInstance*) throw (DmException);
  void setSecurityContext(const SecurityContext*) throw (DmException);
  
  IOHandler *createIOHandler(const std::string& pfn,
                             int openmode,
                             const std::map<std::string, std::string>& extras) throw (DmException);
  
  struct stat pStat(const std::string& pfn) throw (DmException);
private:
  friend class HadoopIOHandler;
  
  hdfsFS fs;
};



/// IO Factory
class HadoopFactory: public IOFactory, public PoolDriverFactory {
public:
  HadoopFactory() throw (DmException);
  
  void configure(const std::string& key, const std::string& value) throw (DmException);
   
  std::string implementedPool() throw();
  PoolDriver* createPoolDriver() throw (DmException);
  
  IODriver* createIODriver(PluginManager*) throw (DmException);

protected:
private:
};

};

#endif	// HADOOP_H
