#include <dmlite/python/PythonINode.h>
#include <dmlite/python/PythonINodeFactories.h>

using namespace dmlite;
using namespace boost::python;

#define CALL_PYTHON(funcname, ...) \
try { \
  object inode_mod = boost::any_cast<object>(this->py["inode"]); \
  object result = inode_mod.attr("#funcname")(__VA_ARGS__); \
} catch (error_already_set const &) { \
  PyErr_Print(); \
}

PythonINode::PythonINode(object inode_obj) throw (DmException)
{
  this->py["inode"] = inode_obj;
}

std::string PythonINode::getImplId() const throw ()
{
  return std::string("PythonINode");
}



void PythonINode::setStackInstance(StackInstance* si) throw (DmException)
{
  // Nothing
}



void PythonINode::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  // Nothing
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
    object inode_mod = boost::any_cast<object>(this->py["inode"]);
    object result = inode_mod.attr("create")(nf);
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
    object inode_mod = boost::any_cast<object>(this->py["inode"]);
    object result = inode_mod.attr("extendedStat")(inode);
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
    object inode_mod = boost::any_cast<object>(this->py["inode"]);
    object result = inode_mod.attr("extendedStat")(parent, name);
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
    object inode_mod = boost::any_cast<object>(this->py["inode"]);
    object result = inode_mod.attr("extendedStat")(guid);
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
    object inode_mod = boost::any_cast<object>(this->py["inode"]);
    object result = inode_mod.attr("symlink")(inode);
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
    object inode_mod = boost::any_cast<object>(this->py["inode"]);
    object result = inode_mod.attr("getReplicas")(inode);
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
    object inode_mod = boost::any_cast<object>(this->py["inode"]);
    object result = inode_mod.attr("getReplica")(rid);
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
    object inode_mod = boost::any_cast<object>(this->py["inode"]);
    object result = inode_mod.attr("getReplica")(rfn);
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
  CALL_PYTHON(inode, size);
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
    object inode_mod = boost::any_cast<object>(this->py["inode"]);
    object result = inode_mod.attr("getComment")(inode);
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
    object inode_mod = boost::any_cast<object>(this->py["inode"]);
    object result = inode_mod.attr("openDir")(inode);
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
    object inode_mod = boost::any_cast<object>(this->py["inode"]);
    object result = inode_mod.attr("readDirx")(dir);
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
    object inode_mod = boost::any_cast<object>(this->py["inode"]);
    object result = inode_mod.attr("readDir")(dir);
    dirp = extract<dirent *>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
  return dirp;
}
