/*
 * poolmanagerwrapper.cpp
 *
 * Wrapper classes for Python bindings for poolmanager.h from
 * the c++ dmlite library via Boost:Python.
 * This file is included by poolmanager.cpp.
 */
    
    // The class PoolManager has pure virtual methods: Create a wrapper class!
    class PoolManagerWrapper: public PoolManager, public wrapper<PoolManager> {
        public:
        virtual std::vector<Pool> getPools(PoolAvailability availability = kAny) throw (DmException) { return this->get_override("getPools")(availability); } 
        virtual Pool getPool(const std::string& poolname) throw (DmException) { return this->get_override("getPool")(poolname); } 
        virtual Location whereToRead(const std::string& path) throw (DmException) { return this->get_override("whereToRead")(path); } 
        virtual Location whereToWrite(const std::string& path) throw (DmException) { return this->get_override("whereToWrite")(path); } 
    };
    
    // The class PoolManagerFactory has pure virtual methods: Create a wrapper class!
    class PoolManagerFactoryWrapper: public PoolManagerFactory, public wrapper<PoolManagerFactory> {
        public:
        virtual PoolManager* createPoolManager(PluginManager* pm) throw (DmException) { return this->get_override("createPoolManager")(pm); } 
    };
