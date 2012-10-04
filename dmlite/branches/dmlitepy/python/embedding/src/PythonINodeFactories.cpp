#include <dlfcn.h>

#include <dmlite/python/PythonINodeFactories.h>
#include <dmlite/python/PythonINode.h>

using namespace dmlite;
using namespace boost::python;

PythonINodeFactory::PythonINodeFactory(std::string pymodule) throw(DmException)
{
  // refer to this bug, please: http://bugs.python.org/issue4434
  void *handle = dlopen("libpython2.6.so", RTLD_LAZY | RTLD_GLOBAL);
  if (handle == NULL) {
    throw DmException(DMLITE_SYSERR(DMLITE_INTERNAL_ERROR), "dlopen of libpython2.6.so failed");
  }

  Py_Initialize();

  object inodeFac;
  object pyinodeFac;
  try {
    inodeFac = import(pymodule.c_str());
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
