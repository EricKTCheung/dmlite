/*
 * exceptions.cpp
 *
 * Python bindings for exceptions.h from the c++ dmlite library
 * via Boost:Python.
 * This file is included by pydmlite.cpp.
 */

	class_<DmException>("DmException", init<>())
		.def(init<int>())
		.def(init<int, const std::string&>())
		//.def(init<int, const char*, va_list>())

		.def("code", &DmException::code)
		.def("what", &DmException::what)
		;
