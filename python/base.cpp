/*
 * base.cpp
 *
 * Python bindings for base.h from the c++ dmlite library
 * via Boost:Python.
 * This file is included by pydmlite.cpp.
 */

	class_<BaseInterfaceWrapper, boost::noncopyable>("BaseInterface", no_init)
		.def("getImplId", boost::python::pure_virtual(&BaseInterface::getImplId))
		;

	class_<BaseFactoryWrapper, boost::noncopyable>("BaseFactory", no_init)
		.def("configure", boost::python::pure_virtual(&BaseFactory::configure))
		;

