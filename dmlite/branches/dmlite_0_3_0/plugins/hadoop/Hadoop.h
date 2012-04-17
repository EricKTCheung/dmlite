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
#include <dmlite/common/Uris.h>

#include "hdfs.h"

namespace dmlite {

class HadoopIOHandler: public IOHandler{
public:

  HadoopIOHandler(const std::string& uri, std::iostream::openmode openmode) throw (DmException);
  ~HadoopIOHandler();

  void close(void) throw (DmException);
  size_t read(char* buffer, size_t count) throw (DmException);
  size_t write(const char* buffer, size_t count) throw (DmException);
  void seek(long offset, std::ios_base::seekdir whence) throw (DmException);
  long tell(void) throw (DmException);
  void flush(void) throw (DmException);
  bool eof(void) throw (DmException);
  void deleteFile (const char *filename) throw (DmException);

private:
  hdfsFS fs;		// Hadoop file system
  hdfsFile file;	// Hadoop file descriptor
  bool isEof;		// Set to true if end of the file is reached
};

class HadoopPoolHandler: public PoolHandler {
public:
  HadoopPoolHandler(Pool* pool);
  ~HadoopPoolHandler();

  std::string getPoolType(void) throw (DmException);
  std::string getPoolName(void) throw (DmException);
  uint64_t getTotalSpace(void) throw (DmException);
  uint64_t getUsedSpace(void) throw (DmException);
  uint64_t getFreeSpace(void) throw (DmException);
  bool replicaAvailable(const FileReplica& replica) throw (DmException);
  Uri getPhysicalLocation(const FileReplica& replica) throw (DmException);

private:
  hdfsFS fs;
  Pool* pool;
};

class HadoopIOFactory: public IOFactory, public PoolHandlerFactory {
public:
  HadoopIOFactory() throw (DmException);
  void configure(const std::string& key, const std::string& value) throw (DmException);
  IOHandler *createIO(const std::string& uri, std::iostream::openmode openmode) throw (DmException);

  std::string implementedPool() throw();
  PoolHandler* createPoolHandler(Pool*) throw (DmException);
protected:
private:
};

};
#endif	// HADOOP_H
