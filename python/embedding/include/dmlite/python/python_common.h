/// @file   python/embedding/include/dmlite/python/python_common.h
/// @brief  Python Common Parts.
/// @author Martin Philipp Hellmich <martin.hellmich@cern.ch>
#ifndef DMLITE_PYTHON_PYTHONCOMMON_H
#define DMLITE_PYTHON_PYTHONCOMMON_H

#include <boost/python.hpp>

#include <dmlite/cpp/exceptions.h>
#include <dmlite/cpp/utils/extensible.h>

#include <stdio.h>
#include <stdarg.h>

namespace dmlite {

#define CALL_PYTHON(funcname, ...) \
try { \
  object mod = boost::any_cast<object>(this->py["module"]); \
  object result = mod.attr(#funcname)(__VA_ARGS__); \
} catch (error_already_set const &) { \
  PyErr_Print(); \
}


class PythonMain: public Extensible {

};

void extractException() throw (DmException);

/*
class PythonCommon {
public:
template <class T>
inline T runPythonMethod(PythonMain py, const char *methodName, DmException e, ...)
{
  using namespace boost::python;

  T pythonResult;

  try {
    object mod = boost::any_cast<object>(py["module"]);
    object result = mod.attr(methodName)(va_arg);
    if (result.ptr() == Py_None) {
      throw e;
    }
    pythonResult = extract<T>(result);
  } catch (error_already_set const &) {
    PyErr_Print();
  }
  
  return pythonResult;
};
};
*/
};

#endif // DMLITE_PYTHON_PYTHONCOMMON_H
