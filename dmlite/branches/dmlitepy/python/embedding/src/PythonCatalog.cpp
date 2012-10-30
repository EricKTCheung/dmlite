/// @file   python/embedding/src/python_catalog.cpp
/// @brief  Python Catalog API.
/// @author Martin Philipp Hellmich <martin.hellmich@cern.ch>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/utils/security.h>
#include <dmlite/cpp/utils/urls.h>
#include <dmlite/python/python_catalog.h>
#include <vector>


using namespace dmlite;
using namespace boost::python;



PythonCatalogFactory::PythonCatalogFactory(std::string pymodule)
{
  Py_Initialize();

  object moduleFac;
  object pymoduleFac;
  try {
    moduleFac = import(pymodule.c_str());
    pymoduleFac = moduleFac.attr("pyCatalogFactory")();
  } catch (error_already_set const &) {
    PyErr_Print();
  }
 
  this->py["pymoduleFac"] = pymoduleFac;
}



PythonCatalogFactory::~PythonCatalogFactory()
{
  
}
  


void PythonCatalogFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  try {
    object pymoduleFac = boost::any_cast<object>(this->py["pymoduleFac"]);
    pymoduleFac.attr("configure")(key, value);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}



PythonCatalog* PythonCatalogFactory::createCatalog(PluginManager* pm) throw (DmException)
{
  object module;
  try {
    object pymoduleFac = boost::any_cast<object>(this->py["pymoduleFac"]);
    module = pymoduleFac.attr("createCatalog")(ptr(pm));
  } catch (error_already_set const &) {
    PyErr_Print();
  }

  return new PythonCatalog(module);
}



PythonCatalog::PythonCatalog(boost::python::object module_obj) throw (DmException)
{
  this->py["module"] = module_obj;
}



PythonCatalog::~PythonCatalog()
{
  // Nothing
}



std::string PythonCatalog::getImplId(void) const throw()
{
  return std::string("PythonCatalog");
}


void PythonCatalog::setStackInstance(StackInstance* si) throw (DmException)
{
  CALL_PYTHON(setStackInstance, ptr(si));
}



void PythonCatalog::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  CALL_PYTHON(setSecurityContext, ptr(ctx));
}



void PythonCatalog::changeDir(const std::string& path) throw (DmException)
{
  CALL_PYTHON(changeDir, path);
}



std::string PythonCatalog::getWorkingDir (void) throw (DmException)
{
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("getWorkingDir")();
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_SYSERR(DMLITE_UNEXPECTED_EXCEPTION), "%s: You shouldn't return None here", "Pythoncatalog::getWorkingDir");
    }
    return extract<std::string>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}



ExtendedStat PythonCatalog::extendedStat(const std::string& path, bool followSym) throw (DmException)
{
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("extendedStat")(path, followSym);
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_SYSERR(DMLITE_UNEXPECTED_EXCEPTION), "%s: You shouldn't return None here", "PythonCatalog::extendedStat");
    }
    return extract<ExtendedStat>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}



void PythonCatalog::addReplica(const Replica& replica) throw (DmException)
{
  CALL_PYTHON(addReplica, replica);
}



void PythonCatalog::deleteReplica(const Replica& replica) throw (DmException)
{
  CALL_PYTHON(deleteReplica, replica);
}



std::vector<Replica> PythonCatalog::getReplicas(const std::string& path) throw (DmException)
{
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("getReplicas")(path);
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_SYSERR(DMLITE_UNEXPECTED_EXCEPTION), "%s: You shouldn't return None here", "PythonCatalog::getReplicas");
    }
    return extract<std::vector<Replica> >(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}



void PythonCatalog::symlink(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
  CALL_PYTHON(symlink, oldPath, newPath);
}



std::string PythonCatalog::readLink(const std::string& path) throw (DmException)
{
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("readLink")(path);
    if (result.ptr() == Py_None) {
      throw DmException(EINVAL,
                      path + " is not a symbolic link");
    }
    return extract<std::string>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}



void PythonCatalog::unlink(const std::string& path) throw (DmException)
{
  CALL_PYTHON(unlink, path);
}



void PythonCatalog::create(const std::string& path, mode_t mode) throw (DmException)
{
  CALL_PYTHON(create, path, mode);
}



void PythonCatalog::makeDir(const std::string& path, mode_t mode) throw (DmException)
{
  CALL_PYTHON(makeDir, path, mode);
}



void PythonCatalog::removeDir(const std::string& path) throw (DmException)
{
  CALL_PYTHON(removeDir, path);
}



void PythonCatalog::rename(const std::string& oldPath,
                            const std::string& newPath) throw (DmException)
{
  CALL_PYTHON(rename, oldPath, newPath);
}



mode_t PythonCatalog::umask(mode_t mask) throw ()
{
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("umask")(mask);
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_SYSERR(DMLITE_UNEXPECTED_EXCEPTION), "%s: You shouldn't return None here", "PythonCatalog::umask");
    }
    return extract<mode_t>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}



void PythonCatalog::setMode(const std::string& path, mode_t mode) throw (DmException)
{
  CALL_PYTHON(setMode, path, mode);
}



void PythonCatalog::setOwner(const std::string& path,
                              uid_t newUid, gid_t newGid,
                              bool followSymLink) throw (DmException)
{
  CALL_PYTHON(setOwner, path, newUid, newGid, followSymLink);
}



void PythonCatalog::setSize(const std::string& path, size_t newSize) throw (DmException)
{
  CALL_PYTHON(setSize, path, newSize);
}



void PythonCatalog::setChecksum(const std::string& path,
                                    const std::string& csumtype,
                                    const std::string& csumvalue) throw (DmException)
{
  CALL_PYTHON(setChecksum, path, csumtype, csumvalue);
}



void PythonCatalog::setAcl(const std::string& path, const Acl& acl) throw (DmException)
{
  CALL_PYTHON(setAcl, path, acl);
}



void PythonCatalog::utime(const std::string& path, const struct utimbuf* buf) throw (DmException)
{
  CALL_PYTHON(utime, path, buf);
}



std::string PythonCatalog::getComment(const std::string& path) throw (DmException)
{
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("getComment")(path);
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_SYSERR(DMLITE_UNEXPECTED_EXCEPTION), "%s: You shouldn't return None here", "PythonCatalog::getComment");
    }
    return extract<std::string>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}



void PythonCatalog::setComment(const std::string& path, const std::string& comment) throw (DmException)
{
  CALL_PYTHON(setComment, path, comment);
}



void PythonCatalog::setGuid(const std::string& path, const std::string &guid) throw (DmException)
{
  CALL_PYTHON(setGuid, path, guid);
}



void PythonCatalog::updateExtendedAttributes(const std::string& path,
                                              const Extensible& attr) throw (DmException)
{
  CALL_PYTHON(updateExtendedAttributes, path, attr);
}



Directory* PythonCatalog::openDir(const std::string& path) throw (DmException)
{
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("openDir")(path);
    if (result.ptr() == Py_None) {
      throw DmException(EACCES,
                        "Not enough permissions to read " + path);
    }
    return extract<Directory*>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}



void PythonCatalog::closeDir(Directory* dir) throw (DmException)
{
  CALL_PYTHON(closeDir, dir);
}



struct dirent* PythonCatalog::readDir(Directory* dir) throw (DmException)
{
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("readDir")(dir);
    if (result.ptr() == Py_None) {
      return NULL;
    }
    return extract<struct dirent*>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}



ExtendedStat* PythonCatalog::readDirx(Directory* dir) throw (DmException)
{
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("readDirx")(dir);
    if (result.ptr() == Py_None) {
      return NULL;
    }
    return extract<ExtendedStat*>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}



Replica PythonCatalog::getReplica(const std::string& rfn) throw (DmException)
{
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("getReplica")(rfn);
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_SYSERR(DMLITE_UNEXPECTED_EXCEPTION), "%s: You shouldn't return None here", "PythonCatalog::getReplica");
    }
    return extract<Replica>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}



void PythonCatalog::updateReplica(const Replica& replica) throw (DmException)
{
  CALL_PYTHON(updateReplica, replica);
}
