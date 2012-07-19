/*
 * pydm_base.cpp
 *
 * Python bindings for dm_base.h from the c++ dmlite library
 * via Boost:Python.
 * This file is included by pydmlite.cpp.
 */

	class_<BaseInterface, boost::noncopyable>("BaseInterface", no_init)
		// pure virtual class still to be implemented
		;

	class_<BaseFactory, boost::noncopyable>("BaseFactory", no_init)
		// pure virtual class still to be implemented
		;
