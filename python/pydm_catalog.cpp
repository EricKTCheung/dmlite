/*
 * pydm_catalog.cpp
 *
 * Python bindings for dm_catalog.h from the c++ dmlite library
 * via Boost:Python.
 * This file is included by pydmlite.cpp.
 */

	class_<CatalogWrapper, bases< BaseInterface >, boost::noncopyable >("Catalog", no_init)
		.def("setStackInstance", boost::python::pure_virtual(&Catalog::setStackInstance))
		.def("setSecurityContext", boost::python::pure_virtual(&Catalog::setSecurityContext))
		.def("changeDir", boost::python::pure_virtual(&Catalog::changeDir))
		.def("getWorkingDir", boost::python::pure_virtual(&Catalog::getWorkingDir))

		.def("extendedStat", boost::python::pure_virtual(&Catalog::extendedStat))
		.def("addReplica", boost::python::pure_virtual(&Catalog::addReplica))
		.def("deleteReplica", boost::python::pure_virtual(&Catalog::deleteReplica))
		.def("getReplicas", boost::python::pure_virtual(&Catalog::getReplicas))

		.def("symlink", boost::python::pure_virtual(&Catalog::symlink))
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

		////////////////////////
		// there are severe problems with the void*-parameters / return values of the
		// "Dir"-methods, thus, int-handles now represent directories instead of void*
		// still not sure, if it works correctly -> commented out
		//.def("openDirInt", &CatalogWrapper::openDirInt)
		//.def("closeDirInt", &CatalogWrapper::closeDirInt)
		//.def("readDirInt", &CatalogWrapper::readDirInt, return_internal_reference<>())
		//.def("readDirxInt", &CatalogWrapper::readDirxInt, return_internal_reference<>())

		.def("makeDir", &Catalog::makeDir)
		.def("rename", &Catalog::rename)
		.def("removeDir", &Catalog::removeDir)

		.def("getReplica", &Catalog::getReplica)
		.def("updateReplica", &Catalog::updateReplica)
		;


	class_<CatalogFactoryWrapper, bases< BaseFactory >, boost::noncopyable>("CatalogFactory", no_init)
		.def("createCatalog", static_cast< Catalog*(CatalogFactory::*)(PluginManager*) > (&CatalogFactory::createCatalog), return_internal_reference<>())
		;
	
