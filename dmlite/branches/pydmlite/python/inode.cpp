/*
 * inode.cpp
 *
 * Python bindings for inode.h from the c++ dmlite library
 * via Boost:Python.
 * This file is included by pydmlite.cpp.
 */

	class_<INode, boost::noncopyable>("INode", no_init)
		// pure virtual class still to be implemented
		;

	class_<INodeFactory, boost::noncopyable>("INodeFactory", no_init)
		// pure virtual class still to be implemented
		;
