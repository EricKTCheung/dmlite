/// @file   MySqlIO.h
/// @brief  functions in the mysql plugin that are related to the dmlite IO stack
/// @author Fabrizio Furano <furano@cern.ch>


#include "MySqlFactories.h"
#include "io.h"




using namespace dmlite;




class MysqlIOPassthroughDriver: public IODriver {
  
  public:
    MysqlIOPassthroughDriver(IODriver* decorates, int maxdirspacereportdepth) throw (DmException);
    virtual ~MysqlIOPassthroughDriver();

    std::string getImplId(void) const throw() {
      return std::string("MysqlIODriverPassthrough");
    }


    virtual IOHandler* createIOHandler(const std::string& pfn,
                                       int flags,
                                       const Extensible& extras,
                                       mode_t mode = 0660) throw (DmException);

    void setStackInstance(StackInstance* si) throw (DmException);

    void setSecurityContext(const SecurityContext* ctx) throw (DmException);

    virtual void doneWriting(const Location& loc) throw (DmException);
  protected:
    StackInstance *stack_;

    IODriver*  decorated_;
    char*      decoratedId_;
    
    int dirspacereportdepth;
};