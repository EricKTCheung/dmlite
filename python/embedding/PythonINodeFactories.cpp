#include "PythonINodeFactories.h"
#include "PythonINode.h"

using namespace dmlite;
using namespace boost::python;

PythonINodeFactory::PythonINodeFactory() throw(DmException)
{
  Py_Initialize();

  object inodeFac;
  object pyinodeFac;
  try {
    inodeFac = import("dmlite.PythonINode.inodeFactories");
    pyinodeFac = inodeFac.attr("pyINodeFactory")("f");
  } catch (error_already_set const &) {
    PyErr_Print();
  }
 
  this->py["pyinodeFac"] = pyinodeFac;
}

void PythonINodeFactory::configure(const std::string& key, const std::string& value) throw(DmException)
{
  try {
  object pyinodeFac = boost::any_cast<object>(this->py["pyinodeFac"]);
  pyinodeFac.attr("configure")(key, value);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}

INode* PythonINodeFactory::createINode(PluginManager* pm) throw(DmException)
{
  object inode;
  try {
  object pyinodeFac = boost::any_cast<object>(this->py["pyinodeFac"]);
  inode = pyinodeFac.attr("createINode")("ba");
  } catch (error_already_set const &) {
    PyErr_Print();
  }

  return new PythonINode(inode);
}

static void registerPluginNs(PluginManager* pm) throw(DmException)
{
  PythonINodeFactory* nsFactory = new PythonINodeFactory();
  pm->registerINodeFactory(nsFactory);
}

/// This is what the PluginManager looks for
PluginIdCard plugin_python_ns = {
  PLUGIN_ID_HEADER,
  registerPluginNs
};
