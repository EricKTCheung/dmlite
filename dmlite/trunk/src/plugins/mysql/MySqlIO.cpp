
#include "MySqlIO.h"



  
// ------------------------------------------------
// MysqlIODriverPassthrough
//
 
  
  

MysqlIOPassthroughDriver::MysqlIOPassthroughDriver(IODriver* decorates) throw (DmException) {
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Ctor");

  this->decorated_   = decorates;
  this->decoratedId_ = strdup( decorates->getImplId().c_str() );
}

MysqlIOPassthroughDriver::~MysqlIOPassthroughDriver() {
  
  delete this->decorated_;
  free(this->decoratedId_);

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "");
}

IOHandler* MysqlIOPassthroughDriver::createIOHandler(const std::string& pfn,
                                       int flags,
                                       const Extensible& extras,
                                       mode_t mode) throw (DmException) {
				       

      Log(Logger::Lvl4, mysqllogmask, mysqllogname, "pfn: " << pfn << " flags: " << flags);
      return decorated_->createIOHandler(pfn, flags, extras, mode);
      
}

void MysqlIOPassthroughDriver::setStackInstance(StackInstance* si) throw (DmException) {
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");
  BaseInterface::setStackInstance(this->decorated_, si);
  this->stack_ = si;
}


void MysqlIOPassthroughDriver::setSecurityContext(const SecurityContext* ctx) throw (DmException) {
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");
  BaseInterface::setSecurityContext(this->decorated_, ctx);
}


void MysqlIOPassthroughDriver::doneWriting(const Location& loc) throw (DmException) {
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, " loc:" << loc.toString());
  
  
  decorated_->doneWriting(loc);
  
}


  
  