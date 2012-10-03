#ifndef DMLITE_PYTHON_PYTHONFACTORIES_H
#define DMLITE_PYTHON_PYTHONFACTORIES_H

#include <boost/python.hpp>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/catalog.h>
#include <dmlite/cpp/poolmanager.h>
#include <dmlite/cpp/utils/extensible.h>

namespace dmlite {

class PythonMain: public Extensible {

};

class PythonINodeFactory: public INodeFactory {
public:
  PythonINodeFactory() throw(DmException);

  INode* createINode(PluginManager* pm) throw (DmException);
  void configure(const std::string& key, const std::string& value) throw(DmException);

private:
  PythonMain py;

};
};

#endif

