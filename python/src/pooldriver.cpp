/*
 * pooldriver.cpp
 *
 * Python bindings for pooldriver.h from the c++ dmlite library
 * via Boost:Python.
 */

#include <dmlite/python/pydmlite.h>
#include "pooldriverwrapper.cpp"

void export_pooldriver()
{
    class_<Chunk, bases< Extensible > >("Chunk", init<>())
        .def_readwrite("host", &Chunk::host)
        .def_readwrite("path", &Chunk::path)
        .def_readwrite("offset", &Chunk::offset)
        .def_readwrite("size", &Chunk::size)
        ;

    class_< std::vector< Chunk > >("vector_Chunk")
        .def(vector_indexing_suite< std::vector< Chunk > >()) // only works with operator== and != in Chunk
        ;

    class_<PoolHandlerWrapper, boost::noncopyable>("PoolHandler", no_init)
        .def("getPoolType", boost::python::pure_virtual(&PoolHandler::getPoolType))
        .def("getPoolName", boost::python::pure_virtual(&PoolHandler::getPoolName))
        .def("getTotalSpace", boost::python::pure_virtual(&PoolHandler::getTotalSpace))
        .def("getFreeSpace", boost::python::pure_virtual(&PoolHandler::getFreeSpace))
        .def("poolIsAvailable", boost::python::pure_virtual(&PoolHandler::poolIsAvailable))
        .def("replicaIsAvailable", boost::python::pure_virtual(&PoolHandler::replicaIsAvailable))
        .def("whereToRead", boost::python::pure_virtual(&PoolHandler::whereToRead))
        .def("removeReplica", boost::python::pure_virtual(&PoolHandler::removeReplica))
        .def("whereToWrite", boost::python::pure_virtual(&PoolHandler::whereToWrite))
        ;

    class_<PoolDriverWrapper, boost::noncopyable>("PoolDriver", no_init)
        .def("createPoolHandler", boost::python::pure_virtual(&PoolDriver::createPoolHandler), return_value_policy<manage_new_object>())
        ;

    class_<PoolDriverFactoryWrapper, boost::noncopyable>("PoolDriverFactory", no_init)
        .def("implementedPool", boost::python::pure_virtual(&PoolDriverFactory::implementedPool))
        .def("createPoolDriver", boost::python::pure_virtual(&PoolDriverFactoryWrapper::createPoolDriver), return_value_policy<manage_new_object>())
        ;
}
