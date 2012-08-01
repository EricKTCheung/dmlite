/*
 * inode.cpp
 *
 * Python bindings for inode.h from the c++ dmlite library
 * via Boost:Python.
 * This file is included by pydmlite.cpp.
 */

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
		;

    enum_<ExtendedStat::FileStatus>("FileStatus")
        .value("kOnline", ExtendedStat::kOnline)
        .value("kMigrated", ExtendedStat::kMigrated)
        ;

	class_<SymLink, bases< Extensible > >("SymLink", init<>())
		.def_readwrite("inode", &SymLink::inode)
		.def_readwrite("link", &SymLink::link)
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
		;
	
    enum_<Replica::ReplicaStatus>("ReplicaStatus")
        .value("kOnline", Replica::kAvailable)
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
		//.def("openDir", boost::python::pure_virtual(&INode::openDir), return_value_policy<manage_new_object>())
		.def("closeDir", boost::python::pure_virtual(&INode::closeDir))
		.def("readDirx", boost::python::pure_virtual(&INode::readDirx), return_value_policy<reference_existing_object>())
		.def("readDir", boost::python::pure_virtual(&INode::readDir), return_value_policy<reference_existing_object>())
		;

	class_<INodeFactoryWrapper, bases< BaseFactory >, boost::noncopyable>("INodeFactory", no_init)
		.def("createINode", boost::python::pure_virtual(&INodeFactoryWrapper::createINode), return_value_policy<manage_new_object>())
		;
