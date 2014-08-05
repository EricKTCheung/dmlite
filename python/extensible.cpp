/*
 * extensible.cpp
 *
 * Python bindings for extensible.h from the c++ dmlite library
 * via Boost:Python.
 */

#include "pydmlite.h"
#include "extensiblewrapper.cpp"

void export_extensible()
{
    class_<Extensible>("Extensible", init<>())
        .def("hasField", &Extensible::hasField)
        .def("getElement", static_cast< boost::any&(Extensible::*)(const std::string&) > (&Extensible::operator[]), return_value_policy<reference_existing_object>())
        .def("getElementConst", static_cast< const boost::any&(Extensible::*)(const std::string&) const > (&Extensible::operator[]), return_value_policy<reference_existing_object>())
        .def("size", &Extensible::size)
        .def("clear", &Extensible::clear)

        .def("copy", &Extensible::copy, return_value_policy<manage_new_object>())
        
        .def("serialize", &Extensible::serialize)
        .def("deserialize", &Extensible::deserialize)

        .def("getBool", &Extensible::getBool)
        .def("getLong", &Extensible::getLong)
        .def("getUnsigned", &Extensible::getUnsigned)
        .def("getDouble", &Extensible::getDouble)
        .def("getString", &Extensible::getString)
        //.def("getExtensible", &Extensible::getExtensible)
        .def("getVector", &Extensible::getVector)

        .def("setBool", &ExtensibleSetBool)
        .def("setLong", &ExtensibleSetLong)
        .def("setUnsigned", &ExtensibleSetUnsigned)
        .def("setInt", &ExtensibleSetInt)
        .def("setDouble", &ExtensibleSetDouble)
        .def("setString", &ExtensibleSetString)
    
        .def("getKeys", &Extensible::getKeys)
        .def("__iter__", range(&Extensible::begin, &Extensible::end))
        .def("__len__", &Extensible::size)
        ;
    

    class_<boost::any>("boost_any", init<>()) 
        .def("setBool", &anySetBool)
        .def("setLong", &anySetLong)
        .def("setUnsigned", &anySetUnsigned)
        .def("setInt", &anySetInt)
        .def("setFloat", &anySetFloat)
        .def("setDouble", &anySetDouble)
        .def("setString", &anySetString)
        .def("empty", &boost::any::empty) 
        .def("extract", &anyExtract) 
        ; 


    // does not work because operator== and !=  are missing in boost::any
    //class_< std::vector< boost::any > >("vector_Any")
    //    .def(vector_indexing_suite< std::vector< boost::any > >()) 
    //    ;
}
