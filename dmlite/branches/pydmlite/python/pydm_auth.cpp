/*
 * pydm_auth.cpp
 *
 * Python bindings for dm_auth.h from the c++ dmlite library
 * via Boost:Python.
 * This file is included by pydmlite.cpp.
 */

	class_<SecurityCredentials>("SecurityCredentials", init<>())
		.def(init<const Credentials&>())

		.def("getSecurityMechanism", &SecurityCredentials::getSecurityMechanism)
		.def("getClientName", &SecurityCredentials::getClientName)
		.def("getRemoteAddress", &SecurityCredentials::getRemoteAddress)

		.def("getFqans", &SecurityCredentials::getFqans, return_internal_reference<>())

		.def("getSessionId", &SecurityCredentials::getSessionId)
		;

	class_<SecurityContext>("SecurityContext", init<>())
		.def(init<const SecurityCredentials&, const UserInfo&, const std::vector<GroupInfo>&>())

		.def("getUser", static_cast< const UserInfo&(SecurityContext::*)(void) const > (&SecurityContext::getUser), return_internal_reference<>())
		.def("getGroup", static_cast< const GroupInfo&(SecurityContext::*)(unsigned int) const > (&SecurityContext::getGroup), return_internal_reference<>())

		.def("getUser", static_cast< UserInfo&(SecurityContext::*)(void) > (&SecurityContext::getUser), return_internal_reference<>())
		.def("getGroup", static_cast< GroupInfo&(SecurityContext::*)(unsigned int) > (&SecurityContext::getGroup), return_internal_reference<>())

		.def("resizeGroup", &SecurityContext::resizeGroup)
		.def("getCredentials", &SecurityContext::getCredentials, return_internal_reference<>())
		.def("setCredentials", &SecurityContext::setCredentials)

		.def("hasGroup", &SecurityContext::hasGroup)
		;

	class_<UserGroupDb, boost::noncopyable>("UserGroupDb", no_init)
		// pure virtual class still to be implemented
		;
		
	class_<UserGroupDbFactory, boost::noncopyable>("UserGroupDbFactory", no_init)
		// pure virtual class still to be implemented
		;
