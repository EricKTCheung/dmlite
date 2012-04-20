#ifndef HADOOP_H
#define HADOOP_H

#include <dmlite/dmlite++.h>
#include <dmlite/dummy/Dummy.h>

#include "HadoopCatalog.h"
#include "HadoopIO.h"

namespace dmlite{

class HadoopFactory: public CatalogFactory{
public:
  HadoopFactory(CatalogFactory *catalogFactory) throw (DmException);
  ~HadoopFactory() throw (DmException);

  void configure(const std::string& key, const std::string& value) throw (DmException);
  Catalog* createCatalog() throw (DmException);
protected:
  CatalogFactory* nestedFactory;
private:
  std::string namenode;
  std::string port;
  std::string user;
};
};

#endif //HADOOP_H
