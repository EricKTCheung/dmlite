#include "PythonINode.h"
#include "PythonINodeFactories.h"

using namespace dmlite;
using namespace boost::python;

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
}



void PythonINode::commit(void) throw (DmException)
{
}



void PythonINode::rollback(void) throw (DmException)
{
}



ExtendedStat PythonINode::create(const ExtendedStat& nf) throw (DmException)
{
  ExtendedStat meta;

  return meta;
}



void PythonINode::symlink(ino_t inode, const std::string &link) throw (DmException)
{
}



void PythonINode::unlink(ino_t inode) throw (DmException)
{
}



void PythonINode::move(ino_t inode, ino_t dest) throw (DmException)
{
}



void PythonINode::rename(ino_t inode, const std::string& name) throw (DmException)
{
}



ExtendedStat PythonINode::extendedStat(ino_t inode) throw (DmException)
{
  ExtendedStat meta;

  return meta;
}



ExtendedStat PythonINode::extendedStat(ino_t parent, const std::string& name) throw (DmException)
{
  ExtendedStat meta;

  return meta;
}



ExtendedStat PythonINode::extendedStat(const std::string& guid) throw (DmException)
{
  ExtendedStat meta;
  
  return meta;
}



SymLink PythonINode::readLink(ino_t inode) throw (DmException)
{
  SymLink   link;

  return link;
}



void PythonINode::addReplica(const Replica& replica) throw (DmException)
{
}



void PythonINode::deleteReplica(const Replica& replica) throw (DmException)
{
}



std::vector<Replica> PythonINode::getReplicas(ino_t inode) throw (DmException)
{
  std::vector<Replica> replicas;

  return replicas;
}



Replica PythonINode::getReplica(int64_t rid) throw (DmException)
{
  Replica r;

  return r;
}



Replica PythonINode::getReplica(const std::string& rfn) throw (DmException)
{
  Replica r;

  return r;
}



void PythonINode::updateReplica(const Replica& rdata) throw (DmException)
{
}

 

void PythonINode::utime(ino_t inode, const struct utimbuf* buf) throw (DmException)
{
}



void PythonINode::setMode(ino_t inode, uid_t uid, gid_t gid,
                         mode_t mode, const Acl& acl) throw (DmException)
{
}



void PythonINode::setSize(ino_t inode, size_t size) throw (DmException)
{
}



void PythonINode::setChecksum(ino_t inode, const std::string& csumtype,
                                const std::string& csumvalue) throw (DmException)
{
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
}



void PythonINode::deleteComment(ino_t inode) throw (DmException)
{
}



void PythonINode::setGuid(ino_t inode, const std::string& guid) throw (DmException)
{
}



void PythonINode::updateExtendedAttributes(ino_t inode,
                                          const Extensible& attr) throw (DmException)
{
}



IDirectory* PythonINode::openDir(ino_t inode) throw (DmException)
{
  return NULL;
}



void PythonINode::closeDir(IDirectory* dir) throw (DmException)
{
}



ExtendedStat* PythonINode::readDirx(IDirectory* dir) throw (DmException)
{
  return NULL;
}



struct dirent* PythonINode::readDir (IDirectory* dir) throw (DmException)
{
  return NULL;
}
