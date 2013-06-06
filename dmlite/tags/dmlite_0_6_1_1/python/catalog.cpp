/*
 * catalog.cpp
 *
 * Python bindings for catalog.h from the c++ dmlite library
 * via Boost:Python.
 */

#include "pydmlite.h"
#include "catalogwrapper.cpp"

void export_catalog()
{
    class_<CatalogWrapper, bases< BaseInterface >, boost::noncopyable >("Catalog", no_init)
        .def("changeDir", boost::python::pure_virtual(&Catalog::changeDir))
        .def("getWorkingDir", boost::python::pure_virtual(&Catalog::getWorkingDir))

        .def("extendedStat", boost::python::pure_virtual(&Catalog::extendedStat))
        .def("addReplica", boost::python::pure_virtual(&Catalog::addReplica))
        .def("deleteReplica", boost::python::pure_virtual(&Catalog::deleteReplica))
        .def("getReplicas", boost::python::pure_virtual(&Catalog::getReplicas))

        .def("symlink", boost::python::pure_virtual(&Catalog::symlink))
        .def("readLink", boost::python::pure_virtual(&Catalog::readLink))
        .def("unlink", boost::python::pure_virtual(&Catalog::unlink))
        .def("create", boost::python::pure_virtual(&Catalog::create))

        .def("umask", &Catalog::umask)
        .def("setMode", &Catalog::setMode)
        .def("setOwner", &Catalog::setOwner)
        .def("setSize", &Catalog::setSize)
        .def("setChecksum", &Catalog::setChecksum)
        .def("setAcl", &Catalog::setAcl)

        .def("utime", &Catalog::utime)
        .def("getComment", &Catalog::getComment)
        .def("setComment", &Catalog::setComment)
        .def("setGuid", &Catalog::setGuid)

        .def("openDir", &Catalog::openDir, return_internal_reference<>())
        .def("closeDir", &Catalog::closeDir)
        .def("readDir", &Catalog::readDir, return_internal_reference<>())
        .def("readDirx", &Catalog::readDirx, return_internal_reference<>())

        .def("makeDir", &Catalog::makeDir)
        .def("rename", &Catalog::rename)
        .def("removeDir", &Catalog::removeDir)

        .def("getReplicaByRFN", &Catalog::getReplicaByRFN)
        .def("updateReplica", &Catalog::updateReplica)
        .def("getImplId", &Catalog::getImplId)
        ;


    class_<CatalogFactoryWrapper, bases< BaseFactory >, boost::noncopyable>("CatalogFactory", no_init)
        .def("createCatalog", static_cast< Catalog*(CatalogFactoryWrapper::*)(PluginManager*) > (&CatalogFactoryWrapper::createCatalog), return_value_policy<manage_new_object>())
        ;
    

    class_<DirectoryWrapper, boost::noncopyable >("Directory", no_init)
        ;
}
