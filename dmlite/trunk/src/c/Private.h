/// @file   core/Private.h
/// @brief  Internal declarations.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#ifndef PRIVATE_H
#define	PRIVATE_H

#include <dmlite/c/inode.h>
#include <dmlite/cpp/catalog.h>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/utils/extensible.h>
#include <string>

/// Open the try statement.
#define TRY(context, method)\
try {\
  context->errorCode = DM_NO_ERROR;\
  context->errorString.clear();

/// Catch block.
#define CATCH(context, method)\
  return 0;\
} catch (dmlite::DmException& e) {\
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
} catch (dmlite::DmException& e) {\
  context->errorCode   = e.code();\
  context->errorString = e.what();\
  return NULL;\
} catch (...) {\
  context->errorCode   = DM_UNEXPECTED_EXCEPTION;\
  context->errorString = "An unexpected exception was thrown while executing "#method;\
  return NULL;\
}

/// Try-catch in one macro
#define TRY_CATCH(handle, method, ...)\
try {\
  handle->manager->method(__VA_ARGS__);\
}\
catch (dmlite::DmException& e) {\
  handle->errorCode   = e.code();\
  handle->errorString = e.what();\
  return e.code();\
}\
catch (...) {\
  return DM_UNEXPECTED_EXCEPTION;\
}\
return DM_NO_ERROR;

/// Safe strings (empty instead of NULL)
#define SAFE_STRING(s) s==NULL?"":s

/// Throw an exception if the string is NULL
#define NOT_NULL(s) if (s==NULL) throw dmlite::DmException(DM_NULL_POINTER, #s" is a NULL pointer")

/// Plugin manager handle for C API.
struct dmlite_manager {
  dmlite::PluginManager* manager;
  int                    errorCode;
  std::string            errorString;
};

/// Context handle for C API.
struct dmlite_context {
  dmlite::StackInstance* stack;
  int                    errorCode;
  std::string            errorString;
};

/// dmlite_any
struct dmlite_any {
  boost::any value;
};

/// dmlite_any_dict
struct dmlite_any_dict {
  dmlite::Extensible extensible;
};

/// Directory handler
struct dmlite_dir {
  dmlite::Directory* dir;
  dmlite_xstat       xstat;
};

#endif	/* PRIVATE_H */

