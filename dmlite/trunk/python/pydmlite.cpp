/*
 * pydmlite.cpp
 *
 * Python bindings for the c++ dmlite library via Boost:Python,
 * especially the bindings for dmlite.h are directly here.
 * The resulting python module pydmlite gives access to most
 * the available functionality from dmlite.
 */

#include "pydmlite.h"

void export_extensible();
void export_errno();
void export_types();

void export_authn();
void export_base();
void export_catalog();
void export_exceptions();
void export_inode();
void export_io();
void export_pooldriver();
void export_poolmanager();


BOOST_PYTHON_MODULE(pydmlite)
{
    // These python bindings are compliant with version 20120817.
    scope().attr("API_VERSION") = API_VERSION; 
    
    class_<PluginManager, boost::noncopyable>("PluginManager")
        .def("loadPlugin", &PluginManager::loadPlugin)
        .def("configure", &PluginManager::configure)
        .def("loadConfiguration", &PluginManager::loadConfiguration)

        .def("registerAuthnFactory", &PluginManager::registerAuthnFactory)
        .def("registerINodeFactory", &PluginManager::registerINodeFactory)
        .def("registerCatalogFactory", &PluginManager::registerCatalogFactory)
        .def("registerPoolManagerFactory", &PluginManager::registerPoolManagerFactory)
        .def("registerIOFactory", &PluginManager::registerIOFactory)
        .def("registerPoolDriverFactory", &PluginManager::registerPoolDriverFactory)
        
        .def("getAuthnFactory", &PluginManager::getAuthnFactory, return_value_policy<reference_existing_object>())
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
        .def("getAuthn", &StackInstance::getAuthn, return_value_policy<reference_existing_object>())
        .def("getINode", &StackInstance::getINode, return_value_policy<reference_existing_object>())
        .def("getCatalog", &StackInstance::getCatalog, return_value_policy<reference_existing_object>())
        .def("isTherePoolManager", &StackInstance::isTherePoolManager)
        .def("getPoolManager", &StackInstance::getPoolManager, return_value_policy<reference_existing_object>())
        .def("getPoolDriver", &StackInstance::getPoolDriver, return_value_policy<reference_existing_object>())
        .def("getIODriver", &StackInstance::getIODriver, return_value_policy<reference_existing_object>())
        ;

    class_<PluginIdCard>("PluginIdCard")
        .def_readonly("ApiVersion", &PluginIdCard::ApiVersion)
        // registerPlugin not exposed yet
        ;

    scope().attr("PLUGIN_ID_HEADER") = PLUGIN_ID_HEADER; 

    export_extensible();
    export_errno();
    export_types();

    export_authn();
    export_base();
    export_catalog();
    export_exceptions();
    export_inode();
    export_io();
    export_pooldriver();
    export_poolmanager();
}
