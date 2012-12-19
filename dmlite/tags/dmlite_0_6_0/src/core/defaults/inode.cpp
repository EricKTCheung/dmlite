#include <dmlite/cpp/inode.h>
#include "NotImplemented.h"

using namespace dmlite;



INodeFactory::~INodeFactory()
{
  // Nothing
}



INode* INodeFactory::createINode(INodeFactory* factory, PluginManager* pm) throw (DmException)
{
  return factory->createINode(pm);
}



FACTORY_NOT_IMPLEMENTED(INode* INodeFactory::createINode(PluginManager*) throw (DmException));



IDirectory::~IDirectory()
{
  // Nothing
}



INode::~INode()
{
  // Nothing
}



NOT_IMPLEMENTED(void INode::begin(void) throw (DmException));
NOT_IMPLEMENTED(void INode::commit(void) throw (DmException));
NOT_IMPLEMENTED(void INode::rollback(void) throw (DmException));
NOT_IMPLEMENTED(ExtendedStat INode::create(const ExtendedStat&) throw (DmException));
NOT_IMPLEMENTED(void INode::symlink(ino_t, const std::string &) throw (DmException));
NOT_IMPLEMENTED(void INode::unlink(ino_t) throw (DmException));
NOT_IMPLEMENTED(void INode::move(ino_t, ino_t) throw (DmException));
NOT_IMPLEMENTED(void INode::rename(ino_t, const std::string&) throw (DmException));
NOT_IMPLEMENTED(ExtendedStat INode::extendedStat(ino_t) throw (DmException));
NOT_IMPLEMENTED(ExtendedStat INode::extendedStat(ino_t, const std::string&) throw (DmException));
NOT_IMPLEMENTED(ExtendedStat INode::extendedStat(const std::string&) throw (DmException));
NOT_IMPLEMENTED(SymLink INode::readLink(ino_t) throw (DmException));
NOT_IMPLEMENTED(void INode::addReplica(const Replica&) throw (DmException));
NOT_IMPLEMENTED(void INode::deleteReplica(const Replica&) throw (DmException));
NOT_IMPLEMENTED(Replica INode::getReplica(int64_t) throw (DmException));
NOT_IMPLEMENTED(Replica INode::getReplica(const std::string&) throw (DmException));
NOT_IMPLEMENTED(void INode::updateReplica(const Replica&) throw (DmException));
NOT_IMPLEMENTED(std::vector<Replica> INode::getReplicas(ino_t) throw (DmException));
NOT_IMPLEMENTED(void INode::utime(ino_t, const struct utimbuf*) throw (DmException));
NOT_IMPLEMENTED(void INode::setMode(ino_t, uid_t, gid_t, mode_t, const Acl&) throw (DmException));
NOT_IMPLEMENTED(void INode::setSize(ino_t, size_t) throw (DmException));
NOT_IMPLEMENTED(void INode::setChecksum(ino_t, const std::string&, const std::string&) throw (DmException));
NOT_IMPLEMENTED(std::string INode::getComment(ino_t) throw (DmException));
NOT_IMPLEMENTED(void INode::setComment(ino_t, const std::string&) throw (DmException));
NOT_IMPLEMENTED(void INode::deleteComment(ino_t) throw (DmException));
NOT_IMPLEMENTED(void INode::setGuid(ino_t, const std::string&) throw (DmException));
NOT_IMPLEMENTED(void INode::updateExtendedAttributes(ino_t,const Extensible&) throw (DmException));
NOT_IMPLEMENTED(IDirectory* INode::openDir(ino_t) throw (DmException));
NOT_IMPLEMENTED(void INode::closeDir(IDirectory*) throw (DmException));
NOT_IMPLEMENTED(ExtendedStat* INode::readDirx(IDirectory*) throw (DmException));
NOT_IMPLEMENTED(struct dirent* INode::readDir (IDirectory*) throw (DmException));
