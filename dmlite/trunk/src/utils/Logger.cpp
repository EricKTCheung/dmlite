#include "utils/logger.h"
#include <stdlib.h>
#include <iostream>
#include <boost/iterator/iterator_concepts.hpp>

Logger *Logger::instance = 0;
Logger::bitmask Logger::unregistered = ~0;

Logger::Logger() : level(DEBUG), size(0)
{
    mask = 0;
    registerComponent("unregistered");
    mask = unregistered = getMask("unregistered");
    
    // log the process ID, connect to syslog without delay, log also to 'cerr'
    int options = LOG_PID | LOG_NDELAY | LOG_PERROR;
    // setting 'ident' to NULL means that the program name will be used
    openlog(0, options, LOG_USER);
}



Logger::~Logger()
{
    closelog();
}

void Logger::log(Level lvl, const std::string & msg) const
{
    syslog(LOG_ERR, "%s", msg.c_str());
}

void Logger::registerComponent(component const &  comp)
{
    // check if a mapping already exists
    std::map<component, bitmask>::const_iterator cit = mapping.find(comp);
    if (cit != mapping.end()) return;
    // otherwise generate a new bitmask for the component
    bitmask m = 0;
    m    |= 1 << size; // mask for the component
    mask |= 1 << size; // global mask with all components
    ++size;
    // store the mapping
    mapping.insert(std::make_pair(comp, m));
    
    std::ostringstream outs;
    outs << "Logger::registerComponent" << " : " << "Registered logger component" << comp << "," << size;                      			\
    log(BASE, outs.str());
}

void Logger::registerComponents(std::vector<component> const & components)
{
    std::vector<component>::const_iterator it;
    for (it = components.begin(); it != components.end(); ++it)
    {
        registerComponent(*it);
    }
}

Logger::bitmask Logger::getMask(component const & comp)
{
    // if the component has been registered return the respective bit mask
    std::map<component, bitmask>::const_iterator it = mapping.find(comp);
    if (it != mapping.end()) return it->second;
    // otherwise return the mask reserved for unregistered componentsS
    return unregistered;
}

void Logger::setLogged(component const &comp, bool tobelogged) {
    registerComponent(comp);
    bitmask b = getMask(comp);
    
    if (tobelogged)
      // Switch on the corresponsing bit
      mask |= b;
    else
      // Switch off the corresponding bit
      mask &= ~b;
    
    
}

