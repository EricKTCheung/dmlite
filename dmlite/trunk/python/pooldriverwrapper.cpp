/*
 * pooldriverwrapper.cpp
 *
 * Wrapper classes for Python bindings for pooldriver.h from
 * the c++ dmlite library via Boost:Python.
 * This file is included by pooldriver.cpp.
 */
    

    // The class PoolHandler has pure virtual methods: Create a wrapper class!
    class PoolHandlerWrapper: public PoolHandler, public wrapper<PoolHandler> {
        public:
        virtual std::string getPoolType(void) throw (DmException) { return this->get_override("getPoolType")(); } 
        virtual std::string getPoolName(void) throw (DmException) { return this->get_override("getPoolName")(); } 
        virtual uint64_t getTotalSpace(void) throw (DmException) { return this->get_override("getTotalSpace")(); } 
        virtual uint64_t getFreeSpace(void) throw (DmException) { return this->get_override("getFreeSpace")(); } 
        virtual bool poolIsAvailable(bool write = true) throw (DmException) { return this->get_override("poolIsAvailable")(write); } 
        virtual bool replicaIsAvailable(const Replica& replica) throw (DmException) { return this->get_override("replicaIsAvailable")(replica); } 
        virtual Location whereToRead(const Replica& replica) throw (DmException) { return this->get_override("whereToRead")(replica); } 
        virtual void removeReplica(const Replica& replica) throw (DmException) { this->get_override("removeReplica")(replica); } 
        virtual Location whereToWrite(const std::string& path) throw (DmException) { return this->get_override("whereToWrite")(path); } 
    };

    // The class PoolDriver has pure virtual methods: Create a wrapper class!
    class PoolDriverWrapper: public PoolDriver, public wrapper<PoolDriver> {
        public:
        virtual PoolHandler* createPoolHandler(const std::string& poolName) throw (DmException) { return this->get_override("createPoolHandler")(poolName); } 
    };

    // The class PoolDriverFactory has pure virtual methods: Create a wrapper class!
    class PoolDriverFactoryWrapper: public PoolDriverFactory, public wrapper<PoolDriverFactory> {
        public:
        virtual std::string implementedPool() throw () { return this->get_override("implementedPool")(); } 
        virtual PoolDriver* createPoolDriver(void) throw (DmException) { return this->get_override("createPoolDriver")(); } 
    };
