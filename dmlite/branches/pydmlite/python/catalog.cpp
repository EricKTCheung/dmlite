/*
 * catalog.cpp
 *
 * Python bindings for catalog.h from the c++ dmlite library
 * via Boost:Python.
 * This file is included by pydmlite.cpp.
 */

	class_<CatalogWrapper, bases< BaseInterface >, boost::noncopyable >("Catalog", no_init)
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
		// there are problems with the void*-parameters / return values of the
		// "Dir"-methods: write a Directory wrapper class for that!
		//.def("openDir", &CatalogWrapper::openDir)
		//.def("closeDir", &CatalogWrapper::closeDir)
		//.def("readDir", &CatalogWrapper::readDir, return_internal_reference<>())
		//.def("readDirx", &CatalogWrapper::readDirx, return_internal_reference<>())

		.def("makeDir", &Catalog::makeDir)
		.def("rename", &Catalog::rename)
		.def("removeDir", &Catalog::removeDir)

		.def("getReplica", &Catalog::getReplica)
		.def("updateReplica", &Catalog::updateReplica)
		;


	class_<CatalogFactoryWrapper, bases< BaseFactory >, boost::noncopyable>("CatalogFactory", no_init)
		.def("createCatalog", static_cast< Catalog*(CatalogFactory::*)(PluginManager*) > (&CatalogFactory::createCatalog), return_internal_reference<>())
		;
	
