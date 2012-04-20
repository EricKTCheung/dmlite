#include "Hadoop.h"

using namespace dmlite;
using namespace std;

HadoopFactory::HadoopFactory(CatalogFactory* catalogFactory) throw (DmException): nestedFactory(catalogFactory){
}

HadoopFactory::~HadoopFactory() throw(DmException){
}

void HadoopFactory::configure(const std::string& key, const std::string& value) throw(DmException){
  if (key == "Namenode")
    this->namenode = value;  
  else if (key == "Port")
    this->port = value;  
  else if (key == "User")
    this->user = value;  
  else
    throw DmException(DM_UNKNOWN_OPTION, std::string("Unknown option ") + key);
}



Catalog* HadoopFactory::createCatalog() throw(DmException)
{
  Catalog* nested = 0x00;

  if (this->nestedFactory != 0x00)
    nested = this->nestedFactory->createCatalog();

  return new HadoopCatalog(nested, this->namenode, this->port, this->user);
}


static void registerPluginHadoop(PluginManager* pm) throw(DmException)
{
  CatalogFactory* nested = 0x00;
  try {
    nested = pm->getCatalogFactory();
  }
  catch (DmException e) {
    if (e.code() != DM_NO_FACTORY)
      throw;
  }
  pm->registerCatalogFactory(new HadoopFactory(nested));
}

/// This is what the PluginManager looks for
PluginIdCard plugin_hadoop = {
  API_VERSION,
  registerPluginHadoop
};

