/// @file   python/embedding/src/PythonPoolDriver.cpp
/// @brief  Python PythonPoolDriver API.
/// @author Martin Philipp Hellmich <martin.hellmich@cern.ch>

#include <dmlite/python/python_pooldriver.h>

using namespace dmlite;
using namespace boost::python;


PythonPoolHandler::PythonPoolHandler(boost::python::object module_obj)
{
  this->py["module"] = module_obj;
}

PythonPoolHandler::~PythonPoolHandler()
{
}


std::string PythonPoolHandler::getPoolType(void) throw (DmException)
{
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("getPoolType")();
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_SYSERR(DMLITE_UNEXPECTED_EXCEPTION), "%s: You shouldn't return None here", "PythonPoolHandler::getPoolType");
    }
    return extract<std::string>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}


std::string PythonPoolHandler::getPoolName(void) throw (DmException)
{
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("getPoolName")();
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_SYSERR(DMLITE_UNEXPECTED_EXCEPTION), "%s: You shouldn't return None here", "PythonPoolHandler::getPoolName");
    }
    return extract<std::string>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}


uint64_t PythonPoolHandler::getTotalSpace(void) throw (DmException)
{
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("getTotalSpace")();
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_SYSERR(DMLITE_UNEXPECTED_EXCEPTION), "%s: Yo shouldn't return None here", "PythonPoolHandler::getTotalSpace");
    }
    return extract<uint64_t>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}


uint64_t PythonPoolHandler::getFreeSpace(void) throw (DmException)
{
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("getFreeSpace")();
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_SYSERR(DMLITE_UNEXPECTED_EXCEPTION), "%s: You shouldn't return None here", "PythonPoolHandler::getFreeSpace");
    }
    return extract<uint64_t>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}


bool PythonPoolHandler::poolIsAvailable(bool write) throw (DmException)
{
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("poolIsAvailable")(write);
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_SYSERR(DMLITE_UNEXPECTED_EXCEPTION), "%s: You shouldn't return None here", "PythonPoolHandler::poolIsAvailable");
    }
    return extract<bool>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}


bool PythonPoolHandler::replicaIsAvailable(const Replica& replica) throw (DmException)
{
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("replicaIsAvailable")(replica);
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_SYSERR(DMLITE_UNEXPECTED_EXCEPTION), "%s: You shouldn't return None here", "PythonPoolHandler::replicaIsAvailable");
    }
    return extract<bool>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}


Location PythonPoolHandler::whereToRead(const Replica& replica) throw (DmException)
{
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("whereToRead")(replica);
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_SYSERR(DMLITE_UNEXPECTED_EXCEPTION), "%s: You shouldn't return None here", "PythonPoolHandler::whereToRead");
    }
    return extract<Location>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}


void PythonPoolHandler::removeReplica(const Replica& replica) throw (DmException)
{
  CALL_PYTHON(removeReplica, replica);
}


Location PythonPoolHandler::whereToWrite(const std::string& path) throw (DmException)
{
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("whereToWrite")(path);
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_SYSERR(DMLITE_UNEXPECTED_EXCEPTION), "%s: You shouldn't return None here", "PythonPoolHandler::whereToWrite");
    }
    return extract<Location>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}

PythonPoolDriver::PythonPoolDriver(boost::python::object module_obj)
{
  this->py["module"] = module_obj;
}

PythonPoolDriver::~PythonPoolDriver()
{
}

std::string PythonPoolDriver::getImplId(void) const throw()
{
  return std::string("PythonPoolDriver");
}



void PythonPoolDriver::setStackInstance(StackInstance* si) throw (DmException)
{
  CALL_PYTHON(setStackInstance, ptr(si));
}



void PythonPoolDriver::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  CALL_PYTHON(setSecurityContext, ptr(ctx));
}



PythonPoolHandler* PythonPoolDriver::createPoolHandler(const std::string& poolName) throw (DmException)
{
  object module;
  try {
    object pymoduleFac = boost::any_cast<object>(this->py["pymoduleFac"]);
    module = pymoduleFac.attr("createPoolHandler")(poolName);
  } catch (error_already_set const &) {
    PyErr_Print();
  }

  return new PythonPoolHandler(module);
}


void PythonPoolDriver::toBeCreated(const Pool& pool) throw (DmException)
{
  CALL_PYTHON(toBeCreated, pool);
}


void PythonPoolDriver::justCreated(const Pool& pool) throw (DmException)
{
  CALL_PYTHON(justCreated, pool);
}


void PythonPoolDriver::update(const Pool& pool) throw (DmException)
{
  CALL_PYTHON(update, pool);
}


void PythonPoolDriver::toBeDeleted(const Pool& pool) throw (DmException)
{
  CALL_PYTHON(toBeDeleted, pool);
}

PythonPoolDriverFactory::PythonPoolDriverFactory(std::string pymodule)
{
  Py_Initialize();

  object moduleFac;
  object pymoduleFac;
  try {
    moduleFac = import(pymodule.c_str());
    pymoduleFac = moduleFac.attr("pyPoolDriverFactory")();
  } catch (error_already_set const &) {
    PyErr_Print();
  }
 
  this->py["pymoduleFac"] = pymoduleFac;
}

PythonPoolDriverFactory::~PythonPoolDriverFactory()
{
}


void PythonPoolDriverFactory::configure(const std::string& key, const std::string& value) throw(DmException)
{
  try {
    object pymoduleFac = boost::any_cast<object>(this->py["pymoduleFac"]);
    pymoduleFac.attr("configure")(key, value);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}

std::string PythonPoolDriverFactory::implementedPool() throw ()
{
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("implementedPool")();
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_SYSERR(DMLITE_UNEXPECTED_EXCEPTION), "%s: You shouldn't return None here", "PythonPoolDriverFactory::implementedPool");
    }
    return extract<std::string>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}


PythonPoolDriver* PythonPoolDriverFactory::createPoolDriver(void) throw (DmException)
{
  object module;
  try {
    object pymoduleFac = boost::any_cast<object>(this->py["pymoduleFac"]);
    module = pymoduleFac.attr("createPoolDriver")();
  } catch (error_already_set const &) {
    PyErr_Print();
  }

  return new PythonPoolDriver(module);
}
