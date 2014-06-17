/*
 * io.cpp
 *
 * Python bindings for io.h from the c++ dmlite library
 * via Boost:Python.
 */

#include "pydmlite.h"
#include "iowrapper.cpp"

void export_io()
{
    class_<IOHandlerWrapper, boost::noncopyable>("IOHandler", no_init)
        .def("close", boost::python::pure_virtual(&IOHandler::close))
        .def("read", boost::python::pure_virtual(&IOHandler::read))
        .def("write", boost::python::pure_virtual(&IOHandler::write))
        .def("seek", boost::python::pure_virtual(&IOHandler::seek))
        .def("tell", boost::python::pure_virtual(&IOHandler::tell))
        .def("flush", boost::python::pure_virtual(&IOHandler::flush))
        .def("eof", boost::python::pure_virtual(&IOHandler::eof))
        ;
    
    enum_<IOHandler::Whence>("Whence")
        .value("kSet", IOHandler::kSet)
        .value("kCur", IOHandler::kCur)
        .value("kEnd", IOHandler::kEnd)
        ;

    class_<IODriverWrapper, bases< BaseInterface >, boost::noncopyable>("IODriver", no_init)
        .def("createIOHandler", boost::python::pure_virtual(&IODriver::createIOHandler), return_value_policy<manage_new_object>())
        .def("doneWriting", boost::python::pure_virtual(&IODriver::doneWriting))
        ;

    class_<IOFactoryWrapper, bases< BaseFactory >, boost::noncopyable>("IOFactory", no_init)
        .def("createIODriver", boost::python::pure_virtual(&IOFactoryWrapper::createIODriver), return_value_policy<manage_new_object>())
        ;
}
