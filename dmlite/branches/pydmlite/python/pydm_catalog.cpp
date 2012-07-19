/*
 * pydm_catalog.cpp
 *
 * Python bindings for dm_catalog.h from the c++ dmlite library
 * via Boost:Python.
 * This file is included by pydmlite.cpp.
 */

	class_<Catalog, boost::noncopyable>("Catalog", no_init)
		// since this is a pure virtual class, the virtual functions
		// may still have to be declared in the following format?		
		//.def("f", boost::python::pure_virtual(&Base1::f));
		// (with additional wrapper class)

		//.def("getImplId", &Catalog::getImplId) // function removed
		.def("setStackInstance", &Catalog::setStackInstance)
		.def("setSecurityContext", &Catalog::setSecurityContext)
		.def("changeDir", &Catalog::changeDir)
		.def("getWorkingDir", &Catalog::getWorkingDir)
		//.def("getWorkingDirI", &Catalog::getWorkingDirI) // function removed
		.def("extendedStat", &Catalog::extendedStat)
		.def("addReplica", &Catalog::addReplica)
		.def("deleteReplica", &Catalog::deleteReplica)
		.def("getReplicas", &Catalog::getReplicas)

		//.def("get", &Catalog::get)
		//.def("put", static_cast< Location(Catalog::*)(const std::string&) > (&Catalog::put)) // function removed
		//.def("put", static_cast< Location(Catalog::*)(const std::string&, const std::string&) > (&Catalog::put)) // function removed
		//.def("putDone", &Catalog::putDone)

		.def("symlink", &Catalog::symlink)
		.def("unlink", &Catalog::unlink)
		.def("create", &Catalog::create)
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
		// there are still some problems (about the void*-parameters)
		// with the following bindings:
		//.def("openDir", static_cast< void*(Catalog::*)(const std::string&) > (&Catalog::openDir))
		//.def("closeDir", &Catalog::closeDir)
		//.def("readDir", &Catalog::readDir)
		//.def("readDirx", &Catalog::readDirx)

		.def("makeDir", &Catalog::makeDir)
		.def("rename", &Catalog::rename)
		.def("removeDir", &Catalog::removeDir)

		.def("getReplica", &Catalog::getReplica)
		.def("updateReplica", &Catalog::updateReplica)
		;

	class_<CatalogFactory, boost::noncopyable>("CatalogFactory", no_init)
		.def("configure", &CatalogFactory::configure)
		.def("createCatalog", static_cast< Catalog*(CatalogFactory::*)(PluginManager*) > (&CatalogFactory::createCatalog), return_internal_reference<>())
		;
	
