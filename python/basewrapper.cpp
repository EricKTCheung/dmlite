/*
 * basewrapper.cpp
 *
 * Wrapper classes for Python bindings for base.h from
 * the c++ dmlite library via Boost:Python.
 * This file is included by pydmlite.cpp.
 */
	

	// The class Catalog has pure virtual methods: Create a wrapper class!
	class BaseInterfaceWrapper: public BaseInterface, public wrapper<BaseInterface> {
		public:
		virtual std::string getImplId(void) const throw() { return this->get_override("getImplId")(); } 
	};
	
	// The class CatalogFactory has pure virtual methods: Create a wrapper class!
	class BaseFactoryWrapper: public BaseFactory, public wrapper<BaseFactory> {
		public:
		virtual void configure(const std::string& key, const std::string& value) throw (DmException) { this->get_override("configure")(key, value); } 
	};

