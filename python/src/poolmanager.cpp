/*
 * poolmanager.cpp
 *
 * Python bindings for poolmanager.h from the c++ dmlite library
 * via Boost:Python.
 */

#include <dmlite/python/pydmlite.h>
#include "poolmanagerwrapper.cpp"

void export_poolmanager()
{
    class_<Pool, bases< Extensible > >("Pool", init<>())
        .def_readwrite("name", &Pool::name)
        .def_readwrite("type", &Pool::type)

        .def(self < other<Pool>())
        .def(self > other<Pool>())
        .def(self != other<Pool>())
        .def(self == other<Pool>())
        ;

    class_< std::vector< Pool > >("vector_Pool")
        .def(vector_indexing_suite< std::vector< Pool > >()) // only works with operator== and != in Pool
        ;
    
    class_<PoolManagerWrapper, bases< BaseInterface >, boost::noncopyable>("PoolManager", no_init)
        .def("getPools", boost::python::pure_virtual(&PoolManager::getPools))
        .def("getPool", boost::python::pure_virtual(&PoolManager::getPool))
        .def("whereToRead", boost::python::pure_virtual( static_cast< Location(PoolManager::*)(const std::string&) >  (&PoolManager::whereToRead) ))
        .def("whereToWrite", boost::python::pure_virtual(&PoolManager::whereToWrite))
    
        .def("newPool", boost::python::pure_virtual(&PoolManager::newPool))
        .def("updatePool", boost::python::pure_virtual(&PoolManager::updatePool))
        .def("deletePool", boost::python::pure_virtual(&PoolManager::deletePool))
        ;

    enum_<PoolManager::PoolAvailability>("PoolAvailability")
        .value("kAny", PoolManager::kAny)
        .value("kNone", PoolManager::kNone)
        .value("kForRead", PoolManager::kForRead)
        .value("kForWrite", PoolManager::kForWrite)
        .value("kForBoth", PoolManager::kForBoth)
        ;

    class_<PoolManagerFactoryWrapper, bases< BaseFactory >, boost::noncopyable>("PoolManagerFactory", no_init)
        .def("createPoolManager", boost::python::pure_virtual(&PoolManagerFactoryWrapper::createPoolManager), return_value_policy<manage_new_object>())
        ;
}
