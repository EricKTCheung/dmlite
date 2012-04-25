/// @file    plugins/hadoop/HadoopIO.h
/// @brief   plugin to store dpm data in a hadoop backend.
/// @author  Alexandre Beche <abeche@cern.ch>
#ifndef HADOOPIO_H
#define	HADOOPIO_H

#include <vector>

#include <dmlite/dm_io.h>
#include <dmlite/dmlite++.h>
#include <dmlite/common/Security.h>
#include <dmlite/dummy/Dummy.h>
#include <dmlite/common/Uris.h>

#include "hdfs.h"

namespace dmlite {

class HadoopIOHandler{
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

class HadoopIOFactory{
public:
  HadoopIOFactory() throw (DmException);
  void configure(const std::string& key, const std::string& value) throw (DmException);
  HadoopIOHandler *createIO(const std::string& uri, std::iostream::openmode openmode) throw (DmException);
protected:
private:
};

};
#endif	// HADOOPIO_H
