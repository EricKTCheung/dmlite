/// @file   python/embedding/src/python_catalog.h
/// @brief  Python Catalog API.
/// @author Martin Philipp Hellmich <martin.hellmich@cern.ch>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/utils/security.h>
#include <dmlite/cpp/utils/urls.h>
#include <dmlite/python/python_catalog.h>
#include <vector>


using namespace dmlite;
using namespace boost::python;


#define CALL_PYTHON(funcname, ...) \
try { \
  object mod = boost::any_cast<object>(this->py["module"]); \
  object result = mod.attr("#funcname")(__VA_ARGS__); \
} catch (error_already_set const &) { \
  extractException(); \
}


PythonCatalogFactory::PythonCatalogFactory(std::string pymodule)
{
  Py_Initialize();

  object moduleFac;
  object pymoduleFac;
  try {
    moduleFac = import(pymodule.c_str());
    pymoduleFac = moduleFac.attr("pyCatalogFactory")("f");
  } catch (error_already_set const &) {
    extractException();
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
    extractException();
  }
}



PythonCatalog* PythonCatalogFactory::createCatalog(PluginManager*) throw (DmException)
{
  object module;
  try {
    object pymoduleFac = boost::any_cast<object>(this->py["pymoduleFac"]);
    module = pymoduleFac.attr("createINode")("ba");
  } catch (error_already_set const &) {
    extractException();
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
  CALL_PYTHON(setStackInstance, si);
}



void PythonCatalog::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  CALL_PYTHON(setSecurityContext, ctx);
}



void PythonCatalog::changeDir(const std::string& path) throw (DmException)
{
  CALL_PYTHON(changeDir, path);
}



std::string PythonCatalog::getWorkingDir (void) throw (DmException)
{
  std::string cpp_result;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("getWorkingDir")();
    return extract<std::string>(result);
  } catch (error_already_set const &) {
    extractException();
  }

  return cpp_result;
}



ExtendedStat PythonCatalog::extendedStat(const std::string& path, bool followSym) throw (DmException)
{
  ExtendedStat cpp_result;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("extendedStat")(path, followSym);
    return extract<ExtendedStat>(result);
  } catch (error_already_set const &) {
    extractException();
  }

  return cpp_result;
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
  std::vector<Replica> cpp_result;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("getReplicas")(path);
    return extract<std::vector<Replica> >(result);
  } catch (error_already_set const &) {
    extractException();
  }

  return cpp_result;
}



void PythonCatalog::symlink(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
  CALL_PYTHON(symlink, oldPath, newPath);
}



std::string PythonCatalog::readLink(const std::string& path) throw (DmException)
{
  std::string cpp_result;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("readLink")(path);
    return extract<std::string>(result);
  } catch (error_already_set const &) {
    extractException();
  }

  return cpp_result;
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
  mode_t cpp_result;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("umask")(mask);
    return extract<mode_t>(result);
  } catch (error_already_set const &) {
    extractException();
  }

  return cpp_result;
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
  std::string cpp_result;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("getComment")(path);
    return extract<std::string>(result);
  } catch (error_already_set const &) {
    extractException();
  }

  return cpp_result;
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
  Directory* cpp_result;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("openDir")(path);
    return extract<Directory*>(result);
  } catch (error_already_set const &) {
    extractException();
  }

  return cpp_result;
}



void PythonCatalog::closeDir(Directory* dir) throw (DmException)
{
  CALL_PYTHON(closeDir, dir);
}



struct dirent* PythonCatalog::readDir(Directory* dir) throw (DmException)
{
  struct dirent* cpp_result;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("readDir")(dir);
    return extract<struct dirent*>(result);
  } catch (error_already_set const &) {
    extractException();
  }

  return cpp_result;
}



ExtendedStat* PythonCatalog::readDirx(Directory* dir) throw (DmException)
{
  ExtendedStat* cpp_result;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("readDirx")(dir);
    return extract<ExtendedStat*>(result);
  } catch (error_already_set const &) {
    extractException();
  }

  return cpp_result;
}



Replica PythonCatalog::getReplica(const std::string& rfn) throw (DmException)
{
  Replica cpp_result;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("getReplica")(rfn);
    return extract<Replica>(result);
  } catch (error_already_set const &) {
    extractException();
  }

  return cpp_result;
}



void PythonCatalog::updateReplica(const Replica& replica) throw (DmException)
{
  CALL_PYTHON(updateReplica, replica);
}
