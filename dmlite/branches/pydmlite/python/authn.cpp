/*
 * authn.cpp
 *
 * Python bindings for authn.h from the c++ dmlite library
 * via Boost:Python.
 * This file is included by pydmlite.cpp.
 */

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

	class_<Authn, boost::noncopyable>("Authn", no_init)
		// pure virtual class still to be implemented
		;
		
	class_<AuthnFactory, boost::noncopyable>("AuthnFactory", no_init)
		// pure virtual class still to be implemented
		;
