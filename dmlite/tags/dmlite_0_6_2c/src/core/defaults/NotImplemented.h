#ifndef _NOTIMPLEMENTED_H
#define _NOTIMPLEMENTED_H



#define NOT_IMPLEMENTED(f)\
f {\
  throw DmException(DMLITE_SYSERR(ENOSYS), "'%s' does not implement '%s'", this->getImplId().c_str(), __func__);\
}



#define NOT_IMPLEMENTED_WITHOUT_ID(f)\
f {\
  throw DmException(DMLITE_SYSERR(ENOSYS), "'%s' not implemented", __func__);\
}



#define FACTORY_NOT_IMPLEMENTED(f) NOT_IMPLEMENTED_WITHOUT_ID(f)



#endif
