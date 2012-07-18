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
//		.def("getUser", static_cast< const UserInfo&(SecurityContext::*)(void) > (&SecurityContext::getUser))
		;


