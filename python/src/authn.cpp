/*
 * authn.cpp
 *
 * Python bindings for authn.h from the c++ dmlite library
 * via Boost:Python.
 */

#include <dmlite/python/pydmlite.h>
#include "authnwrapper.cpp"

void export_authn()
{
    class_<SecurityCredentials, bases< Extensible > >("SecurityCredentials", init<>())
        .def_readwrite("mech", &SecurityCredentials::mech)
        .def_readwrite("clientName", &SecurityCredentials::clientName)
        .def_readwrite("remoteAddress", &SecurityCredentials::remoteAddress)
        .def_readwrite("sessionId", &SecurityCredentials::sessionId)
        .def_readwrite("fqans", &SecurityCredentials::fqans)
        ;

    class_<UserInfo, bases< Extensible > >("UserInfo", init<>())
        .def_readwrite("name", &UserInfo::name)
        ;

    class_<GroupInfo, bases< Extensible > >("GroupInfo", init<>())
        .def_readwrite("name", &GroupInfo::name)
        ;

    class_<SecurityContext>("SecurityContext", init<>())
        .def(init<const SecurityCredentials&, const UserInfo&, std::vector<GroupInfo>& >())
        .def_readwrite("credentials", &SecurityContext::credentials)
        .def_readwrite("user", &SecurityContext::user)
        .def_readwrite("groups", &SecurityContext::groups)
        ;

    class_<AuthnWrapper, boost::noncopyable>("Authn", no_init)
        .def("getImplId", pure_virtual(&Authn::getImplId))

        .def("createSecurityContext", pure_virtual(&Authn::createSecurityContext), return_internal_reference<>())

        .def("newGroup", pure_virtual(&Authn::newGroup))
        .def("getGroup", pure_virtual( static_cast< GroupInfo(Authn::*)(const std::string&) >  (&Authn::getGroup) ))
        .def("getGroup", pure_virtual( static_cast< GroupInfo(Authn::*)(const std::string&, const boost::any&) >  (&Authn::getGroup) ))
        .def("updateGroup", pure_virtual(&Authn::updateGroup))
        .def("deleteGroup", pure_virtual(&Authn::deleteGroup))

        .def("newUser", pure_virtual(&Authn::newUser))
        .def("getUser", pure_virtual( static_cast< UserInfo(Authn::*)(const std::string&) >  (&Authn::getUser) ))
        .def("getUser", pure_virtual( static_cast< UserInfo(Authn::*)(const std::string&, const boost::any&) >  (&Authn::getUser) ))
        .def("updateUser", pure_virtual(&Authn::updateUser))
        .def("deleteUser", pure_virtual(&Authn::deleteUser))
        
        .def("getIdMap", pure_virtual(&Authn::getIdMap))

        .def("getGroups", pure_virtual(&Authn::getGroups))
        .def("getUsers", pure_virtual(&Authn::getUsers))
        ;
        
    class_<AuthnFactoryWrapper, boost::noncopyable>("AuthnFactory", no_init)
        .def("createAuthn", pure_virtual(&AuthnFactoryWrapper::createAuthn), return_value_policy<manage_new_object>())
        ;
    
    
    class_< std::vector< GroupInfo > >("vector_GroupInfo")
        .def(vector_indexing_suite< std::vector< GroupInfo > >()) // only works with operator== and != in GroupInfo
        ;
    
    class_< std::vector< UserInfo > >("vector_UserInfo")
        .def(vector_indexing_suite< std::vector< UserInfo > >()) // only works with operator== and != in UserInfo
        ;
}
