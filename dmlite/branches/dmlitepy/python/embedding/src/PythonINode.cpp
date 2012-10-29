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
      throw DmException(DMLITE_SYSERR(DMLITE_UNEXPECTED_EXCEPTION), "You shouldn't return None here");
    }
    meta = extract<ExtendedStat>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }

  return meta;
}



void PythonINode::symlink(ino_t inode, const std::string &link) throw (DmException)
{
  CALL_PYTHON(symlink, inode);
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
    PyErr_Print();
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
    PyErr_Print();
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
    PyErr_Print();
  }
  
  return meta;
}



SymLink PythonINode::readLink(ino_t inode) throw (DmException)
{
  SymLink link;
 
  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("symlink")(inode);
    if (result.ptr() == Py_None) {
      throw DmException(ENOENT, "Link %ld not found", inode);
    }
    link = extract<SymLink>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }

  return link;
}



void PythonINode::addReplica(const Replica& replica) throw (DmException)
{
  CALL_PYTHON(replica);
}



void PythonINode::deleteReplica(const Replica& replica) throw (DmException)
{
  CALL_PYTHON(replica);
}



std::vector<Replica> PythonINode::getReplicas(ino_t inode) throw (DmException)
{
  std::vector<Replica> replicas;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("getReplicas")(inode);
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_NO_SUCH_REPLICA, "Replica %d not found", inode);
    }
    replicas = extract<std::vector<Replica> >(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }

  return replicas;
}



Replica PythonINode::getReplica(int64_t rid) throw (DmException)
{
  Replica r;

  try {
    object mod = boost::any_cast<object>(this->py["module"]);
    object result = mod.attr("getReplica")(rid);
    if (result.ptr() == Py_None) {
      throw DmException(DMLITE_NO_SUCH_REPLICA, "Replica %d not found", rid);
    }
    r = extract<Replica>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
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
    PyErr_Print();
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
    PyErr_Print();
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
    PyErr_Print();
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
    PyErr_Print();
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
    PyErr_Print();
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
    PyErr_Print();
  }
 
  this->py["pymoduleFac"] = pymoduleFac;
}

void PythonINodeFactory::configure(const std::string& key, const std::string& value) throw(DmException)
{
  try {
    object pymoduleFac = boost::any_cast<object>(this->py["pymoduleFac"]);
    pymoduleFac.attr("configure")(key, value);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
}

PythonINode* PythonINodeFactory::createINode(PluginManager* pm) throw(DmException)
{
  object module;
  try {
    object pymoduleFac = boost::any_cast<object>(this->py["pymoduleFac"]);
    module = pymoduleFac.attr("createINode")(ptr(pm));
  } catch (error_already_set const &) {
    PyErr_Print();
  }

  return new PythonINode(module);
}
