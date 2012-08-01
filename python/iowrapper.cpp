/*
 * iowrapper.cpp
 *
 * Wrapper classes for Python bindings for io.h from
 * the c++ dmlite library via Boost:Python.
 * This file is included by pydmlite.cpp.
 */
	

	// The class IOHandler has pure virtual methods: Create a wrapper class!
	class IOHandlerWrapper: public IOHandler, public wrapper<IOHandler> {
		public:
		virtual void close(void) throw (DmException) { this->get_override("close")(); }
 
		virtual size_t read(char* buffer, size_t count) throw (DmException) { return this->get_override("read")(buffer, count); } 
		virtual size_t write(const char* buffer, size_t count) throw (DmException) { return this->get_override("write")(buffer, count); } 
		virtual void seek(off_t offset, Whence whence) throw (DmException) { this->get_override("seek")(offset, whence); } 
		
		virtual off_t tell(void) throw (DmException) { return this->get_override("tell")(); } 
		virtual void flush(void) throw (DmException) { this->get_override("flush")(); }
		virtual bool eof(void) throw (DmException) { return this->get_override("eof")(); }
	};


	// The class IODriver has pure virtual methods: Create a wrapper class!
	class IODriverWrapper: public IODriver, public wrapper<IODriver> {
		public:
		virtual IOHandler* createIOHandler(const std::string& pfn, int flags, const Extensible& extras) throw (DmException) { return this->get_override("createIOHandler")(pfn, flags, extras); }
		virtual void doneWriting(const std::string& pfn, const Extensible& params) { this->get_override("doneWriting")(pfn, params); }
	};

	// The class IOFactory has pure virtual methods: Create a wrapper class!
	class IOFactoryWrapper: public IOFactory, public wrapper<IOFactory> {
		public:
		virtual IODriver* createIODriver(PluginManager* pm) throw (DmException) { return this->get_override("createIODriver")(pm); } 
	};
