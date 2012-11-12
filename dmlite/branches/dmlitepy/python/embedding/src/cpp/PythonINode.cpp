/// @file   python/embedding/src/PythonINode.cpp
/// @brief  Python PythonINode API.
/// @author Martin Philipp Hellmich <martin.hellmich@cern.ch>

#include <dmlite/python/python_inode.h>

using namespace dmlite;
using namespace boost::python;

PythonINode::PythonINode(object module_obj) throw (DmException)
{
  this->py["module"] = module_obj;
}

std::string PythonINode::getImplId() const throw ()
{
  return std::string("PythonINode");
}



void PythonINode::setStackInstance(StackInstance* si) throw (DmException)
{
  CALL_PYTHON(setStackInstance, ptr(si));
}



void PythonINode::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  CALL_PYTHON(setSecurityContext, ptr(ctx));
}




void PythonINode::begin(void) throw (DmException)
{
  CALL_PYTHON(begin);
}



void PythonINode::commit(void) throw (DmException)
{
  CALL_PYTHON(commit);
}



void PythonINode::rollback(void) throw (DmException)
{
  CALL_PYTHON(rollback);
}



ExtendedStat PythonINode::create(const ExtendedStat& nf) throw (DmException)
{
  ExtendedStat meta;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("create")(nf);
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_SYSERR(DMLITE_UNEXPECTED_EXCEPTION), "%s: You shouldn't return None here", "PythonINode::create");
    }
    meta = extract<ExtendedStat>(result);
  } catch (error_already_set const &) {
  std::string excString;

  PyObject *exc,*val,*tb;
  PyErr_Fetch(&exc,&val,&tb);
  PyErr_NormalizeException(&exc,&val,&tb);
  handle<> hexc(exc),hval(allow_null(val)),htb(allow_null(tb));

  if(!hval) {
    excString = extract<std::string>(str(hexc));
  }
    else {
    object traceback(import("traceback"));
    object format_exception(traceback.attr("format_exception"));
    object formatted_list(format_exception(hexc,hval,htb));
    object formatted(str("").join(formatted_list));
    excString = extract<std::string>(formatted);
  }

  throw DmException(DMLITE_SYSERR(DMLITE_UNKNOWN_ERROR),
                    excString);
  }

  return meta;
}



void PythonINode::symlink(ino_t inode, const std::string &link) throw (DmException)
{
  CALL_PYTHON(symlink, inode, link);
}



void PythonINode::unlink(ino_t inode) throw (DmException)
{
  CALL_PYTHON(unlink, inode);
}



void PythonINode::move(ino_t inode, ino_t dest) throw (DmException)
{
  CALL_PYTHON(move, inode, dest);
}



void PythonINode::rename(ino_t inode, const std::string& name) throw (DmException)
{
  CALL_PYTHON(rename, inode, name);
}



ExtendedStat PythonINode::extendedStat(ino_t inode) throw (DmException)
{
  ExtendedStat meta;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("extendedStat_inode")(inode);
    if (result.ptr() == Py_None) {
      throw DmException(ENOENT, "Inode %ld not found", inode);
    }
    meta = extract<ExtendedStat>(result);
  } catch (error_already_set const &) {
  std::string excString;

  PyObject *exc,*val,*tb;
  PyErr_Fetch(&exc,&val,&tb);
  PyErr_NormalizeException(&exc,&val,&tb);
  handle<> hexc(exc),hval(allow_null(val)),htb(allow_null(tb));

  if(!hval) {
    excString = extract<std::string>(str(hexc));
  }
    else {
    object traceback(import("traceback"));
    object format_exception(traceback.attr("format_exception"));
    object formatted_list(format_exception(hexc,hval,htb));
    object formatted(str("").join(formatted_list));
    excString = extract<std::string>(formatted);
  }

  throw DmException(DMLITE_SYSERR(DMLITE_UNKNOWN_ERROR),
                    excString);
  }
  

  return meta;
}



ExtendedStat PythonINode::extendedStat(ino_t parent, const std::string& name) throw (DmException)
{
  ExtendedStat meta;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("extendedStat_parent_name")(parent, name);
    if (result.ptr() == Py_None) {
      throw DmException(ENOENT, name + " not found");
    }
    meta = extract<ExtendedStat>(result);
  } catch (error_already_set const &) {
    extractException();
  }

  return meta;
}



ExtendedStat PythonINode::extendedStat(const std::string& guid) throw (DmException)
{
 ExtendedStat meta;
 
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("extendedStat_guid")(guid);
    if (result.ptr() == Py_None) {
      throw DmException(ENOENT, "File with guid " + guid + " not found");
    }
    meta = extract<ExtendedStat>(result);
  } catch (error_already_set const &) {
    extractException();
  }
  
  return meta;
}



SymLink PythonINode::readLink(ino_t inode) throw (DmException)
{
  SymLink link;
 
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("readLink")(inode);
    if (result.ptr() == Py_None) {
      throw DmException(ENOENT, "Link %ld not found", inode);
    }
    link = extract<SymLink>(result);
  } catch (error_already_set const &) {
    extractException();
  }

  return link;
}



void PythonINode::addReplica(const Replica& replica) throw (DmException)
{
  CALL_PYTHON(addReplica, replica);
}



void PythonINode::deleteReplica(const Replica& replica) throw (DmException)
{
  CALL_PYTHON(deleteReplica, replica);
}



std::vector<Replica> PythonINode::getReplicas(ino_t inode) throw (DmException)
{
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("getReplicas")(inode);
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_NO_SUCH_REPLICA, "Replica %d not found", inode);
    }
    return extract<std::vector<Replica> >(result);
  } catch (error_already_set const &) {
    extractException();
  }
}



Replica PythonINode::getReplica(int64_t rid) throw (DmException)
{
  Replica r;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("getReplica_rid")(rid);
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_NO_SUCH_REPLICA, "Replica %d not found", rid);
    }
    r = extract<Replica>(result);
  } catch (error_already_set const &) {
    extractException();
  }

  return r;
}



Replica PythonINode::getReplica(const std::string& rfn) throw (DmException)
{
  Replica r;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("getReplica")(rfn);
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_NO_SUCH_REPLICA, "Replica %s not found", rfn.c_str());
    }
    r = extract<Replica>(result);
  } catch (error_already_set const &) {
    extractException();
  }

  return r;
}



void PythonINode::updateReplica(const Replica& rdata) throw (DmException)
{
  CALL_PYTHON(updateReplica, rdata);
}

 

void PythonINode::utime(ino_t inode, const struct utimbuf* buf) throw (DmException)
{
  CALL_PYTHON(utime, inode, buf);
}



void PythonINode::setMode(ino_t inode, uid_t uid, gid_t gid,
                         mode_t mode, const Acl& acl) throw (DmException)
{
  CALL_PYTHON(setMode, inode, uid, gid, mode, acl);
}



void PythonINode::setSize(ino_t inode, size_t size) throw (DmException)
{
  CALL_PYTHON(setSize, inode, size);
}



void PythonINode::setChecksum(ino_t inode, const std::string& csumtype,
                                const std::string& csumvalue) throw (DmException)
{
  CALL_PYTHON(setChecksum, inode, csumtype, csumvalue);
}



std::string PythonINode::getComment(ino_t inode) throw (DmException)
{
  std::string comment;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("getComment")(inode);
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_NO_COMMENT, "There is no comment for inode %ld", inode);
    }
    comment = extract<std::string>(result);
  } catch (error_already_set const &) {
    extractException();
  }
  
  return comment;
}



void PythonINode::setComment(ino_t inode, const std::string& comment) throw (DmException)
{
  CALL_PYTHON(setComment, inode, comment);
}



void PythonINode::deleteComment(ino_t inode) throw (DmException)
{
  CALL_PYTHON(deleteComment, inode);
}



void PythonINode::setGuid(ino_t inode, const std::string& guid) throw (DmException)
{
  CALL_PYTHON(setGuid, inode, guid);
}



void PythonINode::updateExtendedAttributes(ino_t inode,
                                          const Extensible& attr) throw (DmException)
{
  CALL_PYTHON(updateExtendedAttributes, inode, attr);
}



IDirectory* PythonINode::openDir(ino_t inode) throw (DmException)
{
  IDirectory *dirp;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("openDir")(inode);
    if (result.ptr() == Py_None) {
      throw DmException(ENOTDIR, "Inode %ld is not a directory", inode);
    }
    dirp = extract<IDirectory *>(result);
  } catch (error_already_set const &) {
    extractException();
  }
  return dirp;
}



void PythonINode::closeDir(IDirectory* dir) throw (DmException)
{
  CALL_PYTHON(closeDir, dir);
}



ExtendedStat* PythonINode::readDirx(IDirectory* dir) throw (DmException)
{
  ExtendedStat *metap;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("readDirx")(dir);
    if (result.ptr() == Py_None) {
      return NULL;
    }
    metap = extract<ExtendedStat *>(result);
  } catch (error_already_set const &) {
    extractException();
  }
  return metap;
}



struct dirent* PythonINode::readDir (IDirectory* dir) throw (DmException)
{
  dirent *dirp;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("readDir")(dir);
    if (result.ptr() == Py_None) {
      return NULL;
    }
    dirp = extract<dirent *>(result);
  } catch (error_already_set const &) {
    extractException();
  }
  return dirp;
}

PythonINodeFactory::PythonINodeFactory(std::string pymodule) throw(DmException)
{
  
  Py_Initialize();

  object moduleFac;
  object pymoduleFac;
  try {
    moduleFac = import(pymodule.c_str());
    pymoduleFac = moduleFac.attr("pyINodeFactory")();
  } catch (error_already_set const &) {
    extractException();
  }
 
  this->py["pymoduleFac"] = pymoduleFac;
}

void PythonINodeFactory::configure(const std::string& key, const std::string& value) throw(DmException)
{
  try {
    object pymoduleFac = boost::any_cast<object>(this->py["pymoduleFac"]);
    pymoduleFac.attr("configure")(key, value);
  } catch (error_already_set const &) {
    extractException();
  }
}

PythonINode* PythonINodeFactory::createINode(PluginManager* pm) throw(DmException)
{
  object module;
  try {
    object pymoduleFac = boost::any_cast<object>(this->py["pymoduleFac"]);
    module = pymoduleFac.attr("createINode")(ptr(pm));
  } catch (error_already_set const &) {
    extractException();
  }

  return new PythonINode(module);
}
