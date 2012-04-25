/// @file	plugins/hadoop/HadoopCatalog.h
/// @brief	plugin to store dpm data in a hadoop backend
/// @ author	Alexandre Beche <abeche@cern.ch>
#ifndef HADOOPCATALOG_H
#define HADOOPCATALOG_H

#include <dmlite/dmlite++.h>
#include <dmlite/dummy/Dummy.h>
#include <dmlite/dummy/DummyCatalog.h>

#include <hdfs.h>

namespace dmlite{

class HadoopCatalog: public DummyCatalog{
public:
  HadoopCatalog(Catalog *decorates, std::string, std::string, std::string) throw (DmException);
  ~HadoopCatalog() throw (DmException);

  FileReplica get (const std::string&) throw (DmException);
protected:
private:
  hdfsFS fs;
};
};

#endif //HADOOPCATALOG_H
