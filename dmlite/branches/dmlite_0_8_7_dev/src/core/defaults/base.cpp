#include <dmlite/cpp/base.h>
#include "NotImplemented.h"


using namespace dmlite;



BaseInterface::~BaseInterface()
{
  // Nothing
}



BaseFactory::~BaseFactory()
{
  // Nothing
}

FACTORY_NOT_IMPLEMENTED(void BaseFactory::configure(const std::string&, const std::string&) throw (DmException));



void BaseInterface::setStackInstance(BaseInterface* i, StackInstance* si) throw (DmException)
{
  if (i != NULL) i->setStackInstance(si);
}



void BaseInterface::setSecurityContext(BaseInterface* i, const SecurityContext* ctx) throw (DmException)
{
  if (i != NULL) i->setSecurityContext(ctx);
}



NOT_IMPLEMENTED(void BaseInterface::setStackInstance(StackInstance*) throw (DmException));
NOT_IMPLEMENTED(void BaseInterface::setSecurityContext(const SecurityContext*) throw (DmException));
