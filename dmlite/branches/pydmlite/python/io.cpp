/*
 * io.cpp
 *
 * Python bindings for io.h from the c++ dmlite library
 * via Boost:Python.
 * This file is included by pydmlite.cpp.
 */

	class_<IOHandler, boost::noncopyable>("IOHandler", no_init)
		// pure virtual class still to be implemented
		;

	class_<IODriver, boost::noncopyable>("IODriver", no_init)
		// pure virtual class still to be implemented
		;

	class_<IOFactory, boost::noncopyable>("IOFactory", no_init)
		// pure virtual class still to be implemented
		;
