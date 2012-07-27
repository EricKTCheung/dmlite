/*
 * extensible.cpp
 *
 * Python bindings for extensible.h from the c++ dmlite library
 * via Boost:Python.
 * This file is included by pydmlite.cpp.
 */


	class_<Extensible>("Extensible", init<>())
		.def("hasField", &Extensible::hasField)
		.def("getElement", static_cast< boost::any&(Extensible::*)(const std::string&) > (&Extensible::operator[]), return_value_policy<reference_existing_object>())
		.def("getElementConst", static_cast< const boost::any&(Extensible::*)(const std::string&) const > (&Extensible::operator[]), return_value_policy<reference_existing_object>())
		//.def("size", &Extensible::size)
		.def("clear", &Extensible::clear)

		//.def("copy", &Extensible::copy, return_value_policy<manage_new_object>())
		
		.def("serialize", &Extensible::serialize)
		.def("deserialize", &Extensible::deserialize)

		.def("getBool", &Extensible::getBool)
		.def("getLong", &Extensible::getLong)
		.def("getUnsigned", &Extensible::getUnsigned)
		.def("getDouble", &Extensible::getDouble)
		.def("getString", &Extensible::getString)
		//.def("getExtensible", &Extensible::getExtensible)
		//.def("getVector", &Extensible::getVector)
		;
	

	class_<boost::any>("boost_any", init<>()) 
		.def("setBool", &anySetBool)
		.def("setLong", &anySetLong)
		.def("setInt", &anySetInt)
		.def("setFloat", &anySetFloat)
		.def("setDouble", &anySetDouble)
		.def("setString", &anySetString)
		.def("empty", &boost::any::empty) 
		.def("extract", anyExtract) 
		; 
