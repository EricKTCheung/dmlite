/// @file   core/Private.h
/// @brief  Internal declarations.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#ifndef PRIVATE_H
#define	PRIVATE_H

#include <dmlite/dmlite++.h>
#include <fstream>
#include <string>

/// Open the try statement.
#define TRY(context, method)\
try {\
  context->errorCode = DM_NO_ERROR;\
  context->errorString.clear();

/// Catch block.
#define CATCH(context, method)\
  return 0;\
} catch (dmlite::DmException e) {\
  context->errorCode   = e.code();\
  context->errorString = e.what();\
  return e.code();\
} catch (...) {\
  context->errorCode   = DM_UNEXPECTED_EXCEPTION;\
  context->errorString = "An unexpected exception was thrown while executing "#method;\
  return DM_UNEXPECTED_EXCEPTION;\
}



/// Catch block for functions that return a pointer.
#define CATCH_POINTER(context, method)\
} catch (dmlite::DmException e) {\
  context->errorCode   = e.code();\
  context->errorString = e.what();\
  return NULL;\
} catch (...) {\
  context->errorCode   = DM_UNEXPECTED_EXCEPTION;\
  context->errorString = "An unexpected exception was thrown while executing "#method;\
  return NULL;\
}



/// Safe strings (empty instead of NULL)
#define SAFE_STRING(s) s==NULL?"":s


/// Throw an exception if the string is NULL
#define NOT_NULL(s) if (s==NULL) throw dmlite::DmException(DM_NULL_POINTER, #s" is a NULL pointer")

/// Plugin manager handle for C API.
struct dm_manager {
  dmlite::PluginManager* manager;
  int                    errorCode;
  std::string            errorString;
};

/// Context handle for C API.
struct dm_context {
  dmlite::StackInstance* stack;
  dmlite::Catalog*       catalog;
  dmlite::PoolManager*   pool;
  dmlite::IOFactory*     io;
  int                    errorCode;
  std::string            errorString;
};

/// Built-in IO
namespace dmlite {

class StdIOFactory: public IOFactory {
public:
  StdIOFactory();
  ~StdIOFactory();
  void configure(const std::string& key, const std::string& value) throw (DmException);
  IOHandler* createIO(const std::string& uri, std::iostream::openmode openmode) throw (DmException);
  struct stat pstat(const std::string& uri) throw (DmException);
protected:
private:
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
protected:
  std::fstream stream_;
};

};

#endif	/* PRIVATE_H */

