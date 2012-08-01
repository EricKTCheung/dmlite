/*
 * poolmanager.cpp
 *
 * Python bindings for poolmanager.h from the c++ dmlite library
 * via Boost:Python.
 * This file is included by pydmlite.cpp.
 */
	
	class_<Pool, bases< Extensible > >("Pool", init<>())
		.def_readwrite("name", &Pool::name)
		.def_readwrite("type", &Pool::type)
		;
	
	class_<PoolManagerWrapper, bases< BaseInterface >, boost::noncopyable>("PoolManager", no_init)
		.def("getPools", boost::python::pure_virtual(&PoolManager::getPools))
		.def("getPool", boost::python::pure_virtual(&PoolManager::getPool))
		.def("whereToRead", boost::python::pure_virtual(&PoolManager::whereToRead))
		.def("whereToWrite", boost::python::pure_virtual(&PoolManager::whereToWrite))
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
