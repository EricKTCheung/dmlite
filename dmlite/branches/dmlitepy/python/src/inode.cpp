/*
 * inode.cpp
 *
 * Python bindings for inode.h from the c++ dmlite library
 * via Boost:Python.
 */

#include <dmlite/python/pydmlite.h>
#include "inodewrapper.cpp"

void export_inode()
{
    class_<IDirectory, boost::noncopyable >("IDirectory", no_init)
        ;

    class_<ExtendedStat>("ExtendedStat", init<>())
        .def_readwrite("parent", &ExtendedStat::parent)
        .def_readwrite("stat", &ExtendedStat::stat)
        .def_readwrite("status", &ExtendedStat::status)
        .def_readwrite("name", &ExtendedStat::name)
        .def_readwrite("guid", &ExtendedStat::guid)

        .def_readwrite("csumtype", &ExtendedStat::csumtype)
        .def_readwrite("csumvalue", &ExtendedStat::csumvalue)
        .def_readwrite("acl", &ExtendedStat::acl)

        .def(self < other<ExtendedStat>())
        .def(self > other<ExtendedStat>())
        .def(self != other<ExtendedStat>())
        .def(self == other<ExtendedStat>())
        ;


    enum_<ExtendedStat::FileStatus>("FileStatus")
        .value("kOnline", ExtendedStat::kOnline)
        .value("kMigrated", ExtendedStat::kMigrated)
        .export_values()
        ;


    class_<struct stat>("stat", init<>())
        .def_readwrite("st_dev", &stat::st_dev)
        .def_readwrite("st_ino", &stat::st_ino)
        .def_readwrite("st_mode", &stat::st_mode)
        .def_readwrite("st_nlink", &stat::st_nlink)
        .def_readwrite("st_uid", &stat::st_uid)
        .def_readwrite("st_gid", &stat::st_gid)
        .def_readwrite("st_rdev", &stat::st_rdev)
        .def_readwrite("st_size", &stat::st_size)
        .def_readwrite("st_blksize", &stat::st_blksize)
        .def_readwrite("st_blocks", &stat::st_blocks)
        // The following lines produce a weird error Google doesn't know.
        // Thus, getters have been created instead. 
        //.def_readwrite("st_atime", &stat::st_atime)
        //.def_readwrite("st_mtime", &stat::st_mtime)
        //.def_readwrite("st_ctime", &stat::st_ctime)
        .def("getATime", &StatGetATime)
        .def("setATime", &StatSetATime)
        .def("getMTime", &StatGetMTime)
        .def("setMTime", &StatSetMTime)
        .def("getCTime", &StatGetCTime)
        .def("setCTime", &StatSetCTime)
        .def("isDir", &StatIsDir)
        .def("isReg", &StatIsReg)
        .def("isLnk", &StatIsLnk)
        ;
    
    class_<SymLink, bases< Extensible > >("SymLink", init<>())
        .def_readwrite("inode", &SymLink::inode)
        .def_readwrite("link", &SymLink::link)

        .def(self < other<SymLink>())
        .def(self > other<SymLink>())
        .def(self != other<SymLink>())
        .def(self == other<SymLink>())
        ;

    class_<Replica, bases< Extensible > >("Replica", init<>())
        .def_readwrite("replicaid", &Replica::replicaid)
        .def_readwrite("fileid", &Replica::fileid)
        .def_readwrite("nbaccesses", &Replica::nbaccesses)

        .def_readwrite("atime", &Replica::atime)
        .def_readwrite("ptime", &Replica::ptime)
        .def_readwrite("ltime", &Replica::ltime)

        .def_readwrite("status", &Replica::status)
        .def_readwrite("type", &Replica::type)

        .def_readwrite("server", &Replica::server)
        .def_readwrite("rfn", &Replica::rfn)

        .def(self < other<Replica>())
        .def(self > other<Replica>())
        .def(self != other<Replica>())
        .def(self == other<Replica>())
        ;
    
    class_< std::vector< Replica > >("vector_Replica")
        .def(vector_indexing_suite< std::vector< Replica > >()) // only works with operator== and != in Replica
        ;

    enum_<Replica::ReplicaStatus>("ReplicaStatus")
        .value("kAvailable", Replica::kAvailable)
        .value("kBeingPopulated", Replica::kBeingPopulated)
        .value("kToBeDeleted", Replica::kToBeDeleted)
        ;

    enum_<Replica::ReplicaType>("ReplicaType")
        .value("kVolatile", Replica::kVolatile)
        .value("kPermanent", Replica::kPermanent)
        ;

    class_<INodeWrapper, bases< BaseInterface >, boost::noncopyable>("INode", no_init)
        .def("begin", boost::python::pure_virtual(&INode::begin))
        .def("commit", boost::python::pure_virtual(&INode::commit))
        .def("rollback", boost::python::pure_virtual(&INode::rollback))

        .def("create", boost::python::pure_virtual(&INode::create))

        .def("symlink", boost::python::pure_virtual(&INode::symlink))
        .def("unlink", boost::python::pure_virtual(&INode::unlink))
        .def("move", boost::python::pure_virtual(&INode::move))
        .def("rename", boost::python::pure_virtual(&INode::rename))

        .def("extendedStat", boost::python::pure_virtual(static_cast< ExtendedStat (INodeWrapper::*)(ino_t) > (&INode::extendedStat)))
        .def("extendedStat", boost::python::pure_virtual(static_cast< ExtendedStat (INodeWrapper::*)(ino_t, const std::string&) > (&INode::extendedStat)))
        .def("extendedStat", boost::python::pure_virtual(static_cast< ExtendedStat (INodeWrapper::*)(const std::string&) > (&INode::extendedStat)))
        
        .def("readLink", boost::python::pure_virtual(&INode::readLink))
        .def("addReplica", boost::python::pure_virtual(&INode::addReplica))
        .def("deleteReplica", boost::python::pure_virtual(&INode::deleteReplica))
        .def("getReplica", boost::python::pure_virtual(static_cast< Replica (INodeWrapper::*)(int64_t) > (&INode::getReplica)))
        .def("getReplica", boost::python::pure_virtual(static_cast< Replica (INodeWrapper::*)(const std::string&) > (&INode::getReplica)))
        .def("updateReplica", boost::python::pure_virtual(&INode::updateReplica))
        .def("getReplicas", boost::python::pure_virtual(&INode::getReplicas))
        .def("utime", boost::python::pure_virtual(&INode::utime))
        .def("setMode", boost::python::pure_virtual(&INode::setMode))
        .def("setSize", boost::python::pure_virtual(&INode::setSize))
        .def("setChecksum", boost::python::pure_virtual(&INode::setChecksum))
        .def("getComment", boost::python::pure_virtual(&INode::getComment))
        .def("setComment", boost::python::pure_virtual(&INode::setComment))
        .def("deleteComment", boost::python::pure_virtual(&INode::deleteComment))
        .def("setGuid", boost::python::pure_virtual(&INode::setGuid))
        .def("openDir", boost::python::pure_virtual(&INode::openDir), return_value_policy<manage_new_object>())
        .def("closeDir", boost::python::pure_virtual(&INode::closeDir))
        .def("readDirx", boost::python::pure_virtual(&INode::readDirx), return_value_policy<reference_existing_object>())
        .def("readDir", boost::python::pure_virtual(&INode::readDir), return_value_policy<reference_existing_object>())
        ;

    class_<INodeFactoryWrapper, bases< BaseFactory >, boost::noncopyable>("INodeFactory", no_init)
        .def("createINode", boost::python::pure_virtual(&INodeFactoryWrapper::createINode), return_value_policy<manage_new_object>())
        ;
}
