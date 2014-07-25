/// @file   core/PluginManager-C.cpp
/// @brief  C wrapper for dmlite::PluginManager.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <dmlite/dmlite++.h>
#include <dmlite/dmlite.h>

#include "Private.h"

#define TRY_CATCH(handle, method, ...)\
try {\
  handle->manager->method(__VA_ARGS__);\
}\
catch (dmlite::DmException e) {\
  handle->errorCode   = e.code();\
  handle->errorString = e.what();\
  return e.code();\
}\
catch (...) {\
  return DM_UNEXPECTED_EXCEPTION;\
}\
return DM_NO_ERROR;


dm_manager* dm_manager_new(void)
{
  dm_manager* handle;

  handle = new dm_manager;

  handle->manager = new dmlite::PluginManager();
  return handle;
}



void dm_manager_free(dm_manager* handle)
{
  if (handle != NULL) {
    delete handle->manager;
    delete handle;
  }
}



int dm_manager_load_plugin(dm_manager* handle, const char* lib, const char* id)
{
  if (handle == NULL)
    return DM_NULL_POINTER;

  TRY_CATCH(handle, loadPlugin, lib, id);
}



int dm_manager_set(dm_manager* handle, const char* key, const char* value)
{
  if (handle == NULL)
    return -1;

  TRY_CATCH(handle, set, key, value);
}



int dm_manager_load_configuration(dm_manager* handle, const char* file)
{
  if (handle == NULL)
    return DM_NULL_POINTER;

  TRY_CATCH(handle, loadConfiguration, file);
}



int dm_manager_errno(dm_manager* handle)
{
  if (handle == NULL)
    return DM_NULL_POINTER;

  return handle->errorCode;
}



const char* dm_manager_error(dm_manager* handle)
{
  if (handle == NULL)
    return "The manager is a NULL pointer";

  return handle->errorString.c_str();
}



dm_context* dm_context_new(dm_manager* handle)
{
  dm_context*         ctx;
  dmlite::CatalogFactory* factory;

  if (handle == NULL)
    return NULL;

  ctx = new dm_context();

  try {
    factory = handle->manager->getCatalogFactory();
    ctx->catalog = factory->create();
  }
  catch (dmlite::DmException e) {
    delete ctx;
    handle->errorCode   = e.code();
    handle->errorString = e.what();
    return NULL;
  }

  return ctx;
}



int dm_context_free(dm_context* context)
{
  if (context != NULL) {
    delete context->catalog;
    delete context;
  }
  return 0;
}