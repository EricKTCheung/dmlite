/// @file   core/builtin/IO.h
/// @brief  Built-in IO plugin.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#ifndef IO_H
#define	IO_H

#include <dmlite/dmlite++.h>
#include <fstream>

namespace dmlite {

class StdIOFactory: public IOFactory {
public:
  StdIOFactory();
  ~StdIOFactory();
  void configure(const std::string& key, const std::string& value) throw (DmException);
  IOHandler* createIO(const StackInstance*,
                      const std::string& pfn, std::iostream::openmode openmode,
                      const std::map<std::string, std::string>& extras) throw (DmException);
  
  struct stat pStat(const StackInstance*, const std::string& pfn) throw (DmException);
  
protected:
private:
  std::string passwd;
  bool useIp;
};



class StdIOHandler: public IOHandler {
public:
  StdIOHandler(const std::string& path, std::iostream::openmode openmode) throw (DmException);
  ~StdIOHandler();
  void   close(void) throw (DmException);
  size_t read (char* buffer, size_t count) throw (DmException);
  size_t write(const char* buffer, size_t count) throw (DmException);
  void   seek (long offset, std::ios_base::seekdir whence) throw (DmException);
  long   tell (void) throw (DmException);
  void   flush(void) throw (DmException);
  bool   eof  (void) throw (DmException);
  struct stat pstat(void) throw (DmException);
protected:
  std::fstream stream_;
  std::string  path_;
};

};

#endif	// IO_H
