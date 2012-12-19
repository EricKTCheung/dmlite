/*
 * exceptions.cpp
 *
 * Python bindings for exceptions.h from the c++ dmlite library
 * via Boost:Python.
 */

#include "pydmlite.h"

void export_exceptions()
{
    class_<DmException> ("DmException", init<>())
        .def(init<int>())
        .def(init<int, const std::string&>())
        //.def(init<int, const char*, va_list>())

        .def("code", &DmException::code)
        .def("what", &DmException::what)
        ;
}
