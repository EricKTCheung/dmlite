/// @file   core/Private.h
/// @brief  Internal declarations.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#ifndef PRIVATE_H
#define	PRIVATE_H

#include <string>

/// Plugin manager handle for C API.
struct dm_manager {
  dmlite::PluginManager* manager;
  int                    errorCode;
  std::string            errorString;
};

/// Context handle for C API.
struct dm_context {
  dmlite::Catalog* catalog;
  int              errorCode;
  std::string      errorString;
};

#endif	/* PRIVATE_H */

