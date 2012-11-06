/*
 * exceptions.cpp
 *
 * Python bindings for exceptions.h from the c++ dmlite library
 * via Boost:Python.
 */

#include "pydmlite.h"
//#include "exceptionswrapper.cpp"

void export_exceptions()
{
    class_<DmException> ("RawDmException", init<>())
        .def(init<int>())
        .def(init<int, const std::string&>())
        //.def(init<int, const char*, va_list>())

        .def("code", &DmException::code)
        .def("what", &DmException::what)
        ;
}
