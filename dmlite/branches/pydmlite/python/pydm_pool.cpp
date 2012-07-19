/*
 * pydm_pool.cpp
 *
 * Python bindings for dm_pool.h from the c++ dmlite library
 * via Boost:Python.
 * This file is included by pydmlite.cpp.
 */

	class_<PoolMetadata, boost::noncopyable>("PoolMetadata", no_init)
		// pure virtual class still to be implemented
		;

	class_<PoolManager, boost::noncopyable>("PoolManager", no_init)
		// pure virtual class still to be implemented
		;

	class_<PoolManagerFactory, boost::noncopyable>("PoolManagerFactory", no_init)
		// pure virtual class still to be implemented
		;
