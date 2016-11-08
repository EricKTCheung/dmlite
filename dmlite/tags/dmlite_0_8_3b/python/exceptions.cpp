/*
 * exceptions.cpp
 *
 * Python bindings for exceptions.h from the c++ dmlite library
 * via Boost:Python.
 */

#include "pydmlite.h"

PyObject* createExceptionClass(const char* name, PyObject* baseTypeObj)
{
    using std::string;
    namespace bp = boost::python;

    string scopeName = bp::extract<string>(bp::scope().attr("__name__"));
    string qualifiedName0 = scopeName + "." + name;
    char* qualifiedName1 = const_cast<char*>(qualifiedName0.c_str());

    PyObject* typeObj = PyErr_NewException(qualifiedName1, baseTypeObj, 0);
    if(!typeObj) bp::throw_error_already_set();
    bp::scope().attr(name) = bp::handle<>(bp::borrowed(typeObj));
    return typeObj;
}

PyObject* dmExceptionTypeObj = 0;

void translate(const DmException& e)
{
  PyObject_SetAttrString(dmExceptionTypeObj, "code", PyInt_FromLong(e.code()));
  PyErr_SetString(dmExceptionTypeObj, e.what());
}

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
