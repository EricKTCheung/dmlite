/*
 * authnwrapper.cpp
 *
 * Wrapper classes for Python bindings for authn.h from
 * the c++ dmlite library via Boost:Python.
 * This file is included by pydmlite.cpp.
 */
	

	// The class Authn has pure virtual methods: Create a wrapper class!
	class AuthnWrapper: public Authn, public wrapper<Authn> {
		public:
		virtual std::string getImplId(void) const throw() { return this->get_override("getImplId")(); } 
	
		virtual SecurityContext* createSecurityContext(const SecurityCredentials& cred) throw (DmException) { return this->get_override("createSecurityContext")(cred); } 

		virtual GroupInfo newGroup(const std::string& groupName) throw (DmException) { return this->get_override("newGroup")(groupName); } 
		virtual GroupInfo getGroup(const std::string& groupName) throw (DmException) { return this->get_override("getGroup")(groupName); } 

		virtual UserInfo newUser(const std::string& userName) throw (DmException) { return this->get_override("newUser")(userName); } 
		virtual UserInfo getUser(const std::string& userName) throw (DmException) { return this->get_override("getUser")(userName); } 

		virtual void getIdMap(const std::string& userName,
	                          const std::vector<std::string>& groupNames,
	                          UserInfo* user,
	                          std::vector<GroupInfo>* groups) throw (DmException) { this->get_override("getIdMap")(userName, groupNames, user, groups); } 
	};


	// The class AuthnFactory has pure virtual methods: Create a wrapper class!
	class AuthnFactoryWrapper: public AuthnFactory, public wrapper<AuthnFactory> {
		public:
		virtual Authn* createAuthn(PluginManager* pm) throw (DmException) { return this->get_override("createAuthn")(pm); } 
	};
