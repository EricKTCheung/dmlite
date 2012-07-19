/*
 * pydmlite.cpp
 *
 * Python bindings for the c++ dmlite library via Boost:Python.
 * The resulting python module pydmlite gives access to most
 * the available functionality from dmlite.
 */

#include <boost/python.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>
#include "dmlite.h"

using namespace dmlite;


BOOST_PYTHON_MODULE(pydmlite)
{
	using namespace boost::python;

	scope().attr("API_VERSION") = API_VERSION; 
	
	class_<PluginManager, boost::noncopyable>("PluginManager")
		.def("loadPlugin", &PluginManager::loadPlugin)
		.def("configure", &PluginManager::configure)
		.def("loadConfiguration", &PluginManager::loadConfiguration)

		.def("registerFactory", static_cast< void(PluginManager::*)(UserGroupDbFactory*) > (&PluginManager::registerFactory))
		.def("registerFactory", static_cast< void(PluginManager::*)(INodeFactory*) > (&PluginManager::registerFactory))
		.def("registerFactory", static_cast< void(PluginManager::*)(CatalogFactory*) > (&PluginManager::registerFactory))
		.def("registerFactory", static_cast< void(PluginManager::*)(INodeFactory*) > (&PluginManager::registerFactory))
		.def("registerFactory", static_cast< void(PluginManager::*)(PoolManagerFactory*) > (&PluginManager::registerFactory))
		.def("registerFactory", static_cast< void(PluginManager::*)(IOFactory*) > (&PluginManager::registerFactory))
		.def("registerFactory", static_cast< void(PluginManager::*)(PoolDriverFactory*) > (&PluginManager::registerFactory))
		
		.def("getUserGroupDbFactory", &PluginManager::getUserGroupDbFactory, return_value_policy<reference_existing_object>())
		.def("getINodeFactory", &PluginManager::getINodeFactory, return_value_policy<reference_existing_object>())
		.def("getCatalogFactory", &PluginManager::getCatalogFactory, return_value_policy<reference_existing_object>())
		.def("getPoolManagerFactory", &PluginManager::getPoolManagerFactory, return_value_policy<reference_existing_object>())
		.def("getPoolDriverFactory", &PluginManager::getPoolDriverFactory, return_value_policy<reference_existing_object>())
		.def("getIOFactory", &PluginManager::getIOFactory, return_value_policy<reference_existing_object>())
		;
	
	class_<StackInstance>("StackInstance", init<PluginManager*>())
		.def("set", &StackInstance::set)
		.def("get", &StackInstance::get)
		.def("erase", &StackInstance::erase)
		.def("getPluginManager", &StackInstance::getPluginManager, return_value_policy<reference_existing_object>())
		.def("setSecurityCredentials", &StackInstance::setSecurityCredentials)
		.def("setSecurityContext", &StackInstance::setSecurityContext)
		.def("getSecurityContext", &StackInstance::getSecurityContext, return_value_policy<reference_existing_object>())
		.def("getUserGroupDb", &StackInstance::getUserGroupDb, return_value_policy<reference_existing_object>())
		.def("getINode", &StackInstance::getINode, return_value_policy<reference_existing_object>())
		.def("getCatalog", &StackInstance::getCatalog, return_value_policy<reference_existing_object>())
		.def("isTherePoolManager", &StackInstance::isTherePoolManager)
		.def("getPoolManager", &StackInstance::getPoolManager, return_value_policy<reference_existing_object>())
		.def("getPoolDriver", &StackInstance::getPoolDriver, return_value_policy<reference_existing_object>())
		.def("getIODriver", &StackInstance::getIODriver, return_value_policy<reference_existing_object>())
		;

	class_<PluginIdCard>("PluginIdCard")
		// class still to be implemented
		;

#include "pydm_auth.cpp"
//#include "pydm_base.cpp"
#include "pydm_catalog.cpp"
#include "pydm_exceptions.cpp"
#include "pydm_inode.cpp"
#include "pydm_io.cpp"
#include "pydm_pool.cpp"
#include "pydm_pooldriver.cpp"

#include "pydm_errno.cpp"
#include "pydm_types.cpp"

}
