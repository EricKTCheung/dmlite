/// @file   c/PluginManager-C.cpp
/// @brief  C wrapper for dmlite::PluginManager.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <dmlite/c/dmlite.h>
#include <dmlite/cpp/authn.h>
#include <dmlite/cpp/dmlite.h>
#include "Private.h"



static char* safe_dup(const char* str)
{
  if (!str) return NULL;
  char *p = new char[strlen(str) + 1];
  strcpy(p, str);
  return p;
}



static void dmlite_free_secctx(dmlite_security_context* secCtx)
{
  if (secCtx != NULL) {
    delete [] secCtx->credentials->client_name;
    delete [] secCtx->credentials->mech;
    delete [] secCtx->credentials->remote_address;
    delete [] secCtx->credentials->session_id;
    delete secCtx->credentials->extra;
    for (unsigned i = 0; i < secCtx->credentials->nfqans; ++i)
      delete [] secCtx->credentials->fqans[i];
    delete [] secCtx->credentials->fqans;
    
    delete secCtx->credentials;
    for (unsigned i = 0; i < secCtx->ngroups; ++i) {
      delete secCtx->groups[i].extra;
    }
    delete [] secCtx->groups;
    delete secCtx->user.extra;
    delete secCtx;
  }
}



unsigned dmlite_api_version(void)
{
  return dmlite::API_VERSION;
}



dmlite_manager* dmlite_manager_new(void)
{
  dmlite_manager* handle;

  handle = new dmlite_manager;
  handle->manager = new dmlite::PluginManager();
  
  return handle;
}



int dmlite_manager_free(dmlite_manager* handle)
{
  if (handle != NULL) {
    delete handle->manager;
    delete handle;
  }
  return 0;
}



int dmlite_manager_load_plugin(dmlite_manager* handle, const char* lib, const char* id)
{
  if (handle == NULL)
    return DMLITE_SYSERR(EFAULT);

  TRY_CATCH(handle, loadPlugin, lib, id);
}



int dmlite_manager_set(dmlite_manager* handle, const char* key, const char* value)
{
  if (handle == NULL)
    return DMLITE_SYSERR(EFAULT);

  TRY_CATCH(handle, configure, key, value);
}



int dmlite_manager_load_configuration(dmlite_manager* handle, const char* file)
{
  if (handle == NULL)
    return DMLITE_SYSERR(EFAULT);

  TRY_CATCH(handle, loadConfiguration, file);
}



int dmlite_manager_get(dmlite_manager* handle, const char* key, char* buffer, size_t bufsize)
{
  if (handle == NULL)
    return DMLITE_SYSERR(EFAULT);

  try {
    std::string value = handle->manager->getConfiguration(key);
    strncpy(buffer, value.c_str(), bufsize);
    return 0;
  }
  catch (dmlite::DmException& e) {
    if (bufsize > 0) buffer[0] = '\0';
    handle->errorCode   = e.code();
    handle->errorString = e.what();
    return handle->errorCode;
  }
}



int dmlite_manager_errno(dmlite_manager* handle)
{
  if (handle == NULL)
    return DMLITE_SYSERR(EFAULT);

  return DMLITE_ERRNO(handle->errorCode);
}



int dmlite_manager_errtype(dmlite_manager* handle)
{
  if (handle == NULL)
    return DMLITE_SYSTEM_ERROR;
  
  return DMLITE_ETYPE(handle->errorCode);
}



const char* dmlite_manager_error(dmlite_manager* handle)
{
  if (handle == NULL)
    return "The manager is a NULL pointer";

  return handle->errorString.c_str();
}



dmlite_context* dmlite_context_new(dmlite_manager* handle)
{
  dmlite_context*         ctx;

  if (handle == NULL)
    return NULL;

  ctx = new dmlite_context();
  ctx->errorCode = 0;
  ctx->stack     = NULL;
  ctx->secCtx    = NULL;
  
  try {
    ctx->stack = new dmlite::StackInstance(handle->manager);
  }
  catch (dmlite::DmException& e) {
    handle->errorCode   = e.code();
    handle->errorString = e.what();
    delete ctx;
    return NULL;
  }

  return ctx;
}



int dmlite_context_free(dmlite_context* context)
{
  if (context != NULL) {
    dmlite_free_secctx(context->secCtx);
    delete context->stack;
    delete context;
  }
  return 0;
}



int dmlite_errno(dmlite_context* context)
{
  if (context == NULL)
    return EFAULT;
  else
    return DMLITE_ERRNO(context->errorCode);
}



int dmlite_errtype(dmlite_context* context)
{
  if (context == NULL)
    return DMLITE_SYSTEM_ERROR;
  else
    return DMLITE_ETYPE(context->errorCode);
}



const char* dmlite_error(dmlite_context* context)
{
  if (context == NULL)
    return "The context is a NULL pointer";
  return context->errorString.c_str();
}



int dmlite_setcredentials(dmlite_context* context, const dmlite_credentials* cred)
{
  TRY(context, setcredentials)
  NOT_NULL(cred);
  dmlite::SecurityCredentials credpp;
  
  if (cred->extra != NULL)
    credpp.copy(cred->extra->extensible);
  
  credpp.mech          = SAFE_STRING(cred->mech);
  credpp.clientName    = SAFE_STRING(cred->client_name);
  credpp.remoteAddress = SAFE_STRING(cred->remote_address);
  credpp.sessionId     = SAFE_STRING(cred->session_id);
  
  for (unsigned i = 0; i < cred->nfqans; ++i)
    credpp.fqans.push_back(cred->fqans[i]);
  
  context->stack->setSecurityCredentials(credpp);
  
  /* Copy the sec. context */
  const dmlite::SecurityContext* secCtx = context->stack->getSecurityContext();
  dmlite_free_secctx(context->secCtx);
  
  context->secCtx = new dmlite_security_context();
  
  context->secCtx->ngroups = secCtx->groups.size();
  context->secCtx->groups  = new dmlite_security_ent[context->secCtx->ngroups];
  
  context->secCtx->user.name = secCtx->user.name.c_str();
  context->secCtx->user.extra = new dmlite_any_dict();
  context->secCtx->user.extra->extensible.copy(secCtx->user);
  
  for (unsigned i = 0; i < secCtx->groups.size(); ++i) {
    context->secCtx->groups[i].name  = secCtx->groups[i].name.c_str();
    context->secCtx->groups[i].extra = new dmlite_any_dict();
    context->secCtx->groups[i].extra->extensible.copy(secCtx->groups[i]);
  }
  
  context->secCtx->credentials = new dmlite_credentials();
  context->secCtx->credentials->client_name    = safe_dup(cred->client_name);
  context->secCtx->credentials->mech           = safe_dup(cred->mech);
  context->secCtx->credentials->remote_address = safe_dup(cred->remote_address);
  context->secCtx->credentials->session_id     = safe_dup(cred->session_id);
  context->secCtx->credentials->extra          = dmlite_any_dict_copy(cred->extra);
  
  context->secCtx->credentials->nfqans = cred->nfqans;
  context->secCtx->credentials->fqans = new const char*[cred->nfqans];
  for (unsigned i = 0; i < cred->nfqans; ++i)
    context->secCtx->credentials->fqans[i] = safe_dup(cred->fqans[i]);
  
  CATCH(context, setcredentials)
}



const dmlite_security_context* dmlite_get_security_context(dmlite_context* context)
{ 
  if (context->secCtx == NULL) {
    context->errorCode   = DMLITE_NO_SECURITY_CONTEXT;
    context->errorString = "dmlite_setcredentials must be called first";
  }
  else
    context->errorCode = 0;
  return context->secCtx;
}



int dmlite_set(dmlite_context* context, const char* k, const dmlite_any* v)
{
  TRY(context, set)
  NOT_NULL(k);
  context->stack->set(k, v->value);
  CATCH(context, set)
}



int dmlite_set_array(dmlite_context* context, const char* k,
                     unsigned n, dmlite_any* const* v)
{
  TRY(context, set_array)
  NOT_NULL(v);
  std::vector<boost::any> array;

  for (unsigned i = 0; i < n; ++i)
    array.push_back(v[i]->value);

  context->stack->set(k, array);
  CATCH(context, set_array);
}



int dmlite_unset(dmlite_context* context, const char* k)
{
  TRY(context, unset)  
  NOT_NULL(k);
  context->stack->erase(k);
  CATCH(context, unset)
}



int dmlite_unset_all(dmlite_context* context)
{
  TRY(context, unset_all)
  context->stack->eraseAll();
  CATCH(context, unset)
}

