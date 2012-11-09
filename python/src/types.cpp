/*
 * types.cpp
 *
 * Python bindings for urls.h and security.h from the
 * c++ dmlite library via Boost:Python.
 */

#include <dmlite/python/pydmlite.h>

void export_types()
{
    class_<Url>("Url", init<const std::string&>())
        .def_readwrite("scheme", &Url::scheme)
        .def_readwrite("domain", &Url::domain)
        .def_readwrite("port", &Url::port)
        .def_readwrite("path", &Url::path)
        .def_readwrite("query", &Url::query)
        .def("splitPath", &Url::splitPath)
        .def("joinPath", &Url::joinPath)
        .def("normalizePath", &Url::normalizePath)
    ;


    enum_<TokenResult>("TokenResult")
        .value("kTokenOK", kTokenOK)
        .value("kTokenMalformed", kTokenMalformed)
        .value("kTokenInvalid", kTokenInvalid)
        .value("kTokenExpired", kTokenExpired)
        .value("kTokenInvalidMode", kTokenInvalidMode)
        .value("kTokenInternalError", kTokenInternalError)
        ;

    scope().attr("kUserObj") = int(AclEntry::kUserObj);
    scope().attr("kUser") = int(AclEntry::kUser);
    scope().attr("kGroupObj") = int(AclEntry::kGroupObj);
    scope().attr("kGroup") = int(AclEntry::kGroup);
    scope().attr("kMask") = int(AclEntry::kMask);
    scope().attr("kOther") = int(AclEntry::kOther);
    scope().attr("kDefault") = int(AclEntry::kDefault);

    class_<AclEntry>("AclEntry", init<>())
        .def_readwrite("type", &AclEntry::type)
        .def_readwrite("perm", &AclEntry::perm)
        .def_readwrite("id", &AclEntry::id)
        ;
    
    class_< std::vector< AclEntry > >("vector_AclEntry")
        .def(vector_indexing_suite< std::vector< AclEntry > >()) // only works with operator== and != in AclEntry
        ;
    
    class_<Acl, bases< std::vector< AclEntry > > >("Acl", init< const std::string& >())
        .def(init< const Acl&, uid_t, gid_t, mode_t, mode_t* >())
        .def("has", &Acl::has)
        .def("serialize", &Acl::serialize)
        .def("validate", &Acl::validate)
        ;

    class_<utimbuf>("utimbuf", init<>())
        .def_readwrite("actime", &utimbuf::actime)
        .def_readwrite("modtime", &utimbuf::modtime)
        ;
    
    class_< std::vector< std::string > >("vector_String")
        .def(vector_indexing_suite< std::vector< std::string > >()) // only works with operator== and != in String
        ;
}

