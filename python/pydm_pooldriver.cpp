/*
 * pydm_pooldriver.cpp
 *
 * Python bindings for dm_pooldriver.h from the c++ dmlite library
 * via Boost:Python.
 * This file is included by pydmlite.cpp.
 */

	class_<PoolHandler, boost::noncopyable>("PoolHandler", no_init)
		// pure virtual class still to be implemented
		;

	class_<PoolDriver, boost::noncopyable>("PoolDriver", no_init)
		// pure virtual class still to be implemented
		;

	class_<PoolDriverFactory, boost::noncopyable>("PoolDriverFactory", no_init)
		// pure virtual class still to be implemented
		;
