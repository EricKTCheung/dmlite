/// @file    core/dummy/DummyCatalog.cpp
/// @brief   DummyPoolManager implementation.
/// @details It makes sense as a base for other decorator plug-ins.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <dmlite/dummy/Dummy.h>

using namespace dmlite;



/// Little of help here to avoid redundancy
#define DELEGATE(method, ...) \
if (this->decorated_ == 0x00)\
  throw DmException(DM_NOT_IMPLEMENTED, "There is no plugin in the stack that implements "#method);\
this->decorated_->method(__VA_ARGS__);


/// Little of help here to avoid redundancy
#define DELEGATE_RETURN(method, ...) \
if (this->decorated_ == 0x00)\
  throw DmException(DM_NOT_IMPLEMENTED, "There is no plugin in the stack that implements "#method);\
return this->decorated_->method(__VA_ARGS__);



void DummyPoolManager::setSecurityCredentials(const SecurityCredentials& creds) throw (DmException)
{
  DELEGATE(setSecurityCredentials, creds);
}



const SecurityContext& DummyPoolManager::getSecurityContext() throw (DmException)
{
  DELEGATE_RETURN(getSecurityContext);
}



void DummyPoolManager::setSecurityContext(const SecurityContext& ctx)
{
  DELEGATE(setSecurityContext, ctx);
}



std::vector<Pool> DummyPoolManager::getPools(void) throw (DmException)
{
  DELEGATE_RETURN(getPools);
}



std::vector<FileSystem> DummyPoolManager::getPoolFilesystems(const std::string& poolname) throw (DmException)
{
  DELEGATE_RETURN(getPoolFilesystems, poolname);
}


FileSystem DummyPoolManager::getFilesystem(const std::string& pool,
                                           const std::string& server,
                                           const std::string& fs) throw (DmException)
{
  DELEGATE_RETURN(getFilesystem, pool, server, fs);
}
