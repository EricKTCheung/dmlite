/// @file    core/dummy/Dummy.cpp
/// @brief   A dummy plugin that just delegates calls to a decorated one.
/// @details It makes sense as a base for other decorator plug-ins.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <dmlite/Dummy.h>
#include <dmlite/dm_errno.h>

using namespace dmlite;

DummyFactory::DummyFactory(CatalogFactory* catalogFactory) throw (DmException)
{
  this->nested_factory_ = catalogFactory;
}



DummyFactory::~DummyFactory() throw (DmException)
{
  // Nothing
}



void DummyFactory::set(const std::string& key, const std::string& value) throw (DmException)
{
  throw DmException(DM_UNKNOWN_OPTION, std::string(key) + " not recognised");
}



Catalog* DummyFactory::create() throw (DmException)
{
  if (this->nested_factory_ == 0x00)
    return new DummyCatalog(0x00);
  else
    return new DummyCatalog(this->nested_factory_->create());
}



static void registerDummyPlugin(PluginManager* pm) throw(DmException)
{
  try {
    pm->registerCatalogFactory(new DummyFactory(pm->getCatalogFactory()));
  }
  catch (DmException e) {
    if (e.code() == DM_NO_FACTORY)
      throw DmException(DM_NO_FACTORY, "Dummy plugin can not be loaded alone");
    else
      throw;
  }
}



/// This is what the PluginManager looks for
PluginIdCard plugin_dummy = {
  API_VERSION,
  registerDummyPlugin
};
