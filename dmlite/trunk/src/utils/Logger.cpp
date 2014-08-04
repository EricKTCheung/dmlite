#include "utils/logger.h"
#include <stdlib.h>
#include <iostream>
#include <boost/iterator/iterator_concepts.hpp>
#include <cxxabi.h>
#include <execinfo.h>

Logger *Logger::instance = 0;
Logger::bitmask Logger::unregistered = ~0;















// Build a printable stacktrace. Useful e.g. inside exceptions, to understand
// where they come from.
// Note: I don't think that the backtrace() function is thread safe, nor this function
// Returns the number of backtraces
int Logger::getStackTrace(std::string &s)
{
  std::ostringstream o;
  
  void * array[4];
  int size = backtrace(array, 4);
  
  char ** messages = backtrace_symbols(array, size);    
  
  // skip first stack frame (points here)
  for (int i = 1; i < size && messages != NULL; ++i)
  {
    char *mangled_name = 0, *offset_begin = 0, *offset_end = 0;
    
    // find parantheses and +address offset surrounding mangled name
    for (char *p = messages[i]; *p; ++p)
    {
      if (!p) break;
      
      if (*p == '(') {
	mangled_name = p; 
      }
      else if (*p == '+') {
	offset_begin = p;
      }
      else if (*p == ')') {
	offset_end = p;
	break;
      }
    }
    
    // if the line could be processed, attempt to demangle the symbol
    if (mangled_name && offset_begin && offset_end && 
      mangled_name < offset_begin)
    {
      *mangled_name++ = '\0';
      *offset_begin++ = '\0';
      *offset_end++ = '\0';
      
      int status;
      char * real_name = abi::__cxa_demangle(mangled_name, 0, 0, &status);
      
      // if demangling is successful, output the demangled function name
      if (status == 0)
      {
	o << "[bt]: (" << i << ") " << messages[i] << " : " 
	<< real_name << "+" << offset_begin << offset_end 
	<< std::endl;
	
      }
      // otherwise, output the mangled function name
      else
      {
	o << "[bt]: (" << i << ") " << messages[i] << " : " 
	<< mangled_name << "+" << offset_begin << offset_end 
	<< std::endl;
      }
      free(real_name);
    }
    // otherwise, print the whole line
    else
    {
      o << "[bt]: (" << i << ") " << messages[i] << std::endl;
    }
  }
  
  free(messages);
  
  s += o.str();
  
  return size;
}















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
    
    if (tobelogged) {
      // Switch on the corresponsing bit
      mask |= b;
      // Setting ON some logging disables the unregistered category where everything else falls
      if (comp != "unregistered")
	setLogged("unregistered", false);
    }
    else
      // Switch off the corresponding bit
      mask &= ~b;
    
    
}

