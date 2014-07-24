#ifndef Logger_HH
#define Logger_HH

#include <syslog.h>

#include <sstream>
#include <string>

#include <map>
#include <vector>


#define Log(lvl, mymask, where, what) 												\
do{                                											\
	if (Logger::get()->getLevel() >= lvl && Logger::get()->isLogged(mymask)) 	\
	{    																	\
		std::ostringstream outs;                                   			\
		outs << "dmlite " << where << " " << __func__ << " : " << what;                      			\
		Logger::get()->log((Logger::Level)lvl, outs.str());    				\
	}                                                             			\
}while(0)                                                               			\


#define Err(where, what) 												\
do{                                											\
		std::ostringstream outs;                                   			\
		outs << "dmlite " << where << " ERROR " << __func__ << " : " << what;                      			\
		Logger::get()->log((Logger::Level)0, outs.str());    				\
}while(0)       

/**
 * A Logger class
 */
class Logger
{

public:
	/// typedef for a bitmask (long long)
	typedef unsigned long long bitmask;
	/// typedef for a component name (std:string)
	typedef std::string component;

	static bitmask unregistered;

    /**
     * Use the same values for log levels as syslog
     */
    enum Level
    {
        BASE,       // The default?
        WARNING,
        NOTICE,
        INFO,
        DEBUG
    };

    /// Destructor
    ~Logger();

    static Logger *instance;
    
    /// @return the singleton instance
    static Logger *get()
    {
    	if (instance == 0)
	  instance = new Logger();
    	return instance;
    }

    /// @return the current debug level
    short getLevel() const
    {
        return level;
    }

    /// @param lvl : the logging level that will be set
    void setLevel(Level lvl)
    {
        level = lvl;
    }

    /// @return true if the given component is being logged, false otherwise
    bool isLogged(bitmask m) const
    {
        if (mask == 0) return mask & unregistered;
    	return mask & m;
    }
    
    /// @param comp : the component that will be registered for logging
    void registerComponent(component const &  comp);
    
    /// @param components : list of components that will be registered for logging
    void registerComponents(std::vector<component> const & components);

    /// Sets if a component has to be logged or not
    /// @param comp : the component name
    /// @param tobelogged : true if we want to log this component
    void setLogged(component const &comp, bool tobelogged);
    
    /**
     * Logs the message
     *
     * @param lvl : log level of the message
     * @param component : bitmask assignet to the given component
     * @param msg : the message to be logged
     */
    void log(Level lvl, std::string const & msg) const;

    /**
     * @param if true all unregistered components will be logged,
     * 			if false only registered components will be logged
     */
    void logAll()
    {
    	mask = ~0;
    }
    
        
    /**
     * @param comp : component name
     * @return respectiv bitmask assigned to given component
     */
    bitmask getMask(component const & comp);

private:

    ///Private constructor
    Logger();
    // Copy constructor (not implemented)
    Logger(Logger const &);
    // Assignment operator (not implemented)
    Logger & operator=(Logger const &);

    /// current log level
    short level;
    /// number of components that were assigned with a bitmask
    int size;
    /// global bitmask with all registered components
    bitmask mask;
    /// component name to bitmask mapping
    std::map<component, bitmask> mapping;



};


#endif
