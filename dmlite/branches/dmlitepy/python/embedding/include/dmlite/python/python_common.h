/// @file   python/embedding/include/dmlite/python/python_common.h
/// @brief  Python Common Parts.
/// @author Martin Philipp Hellmich <martin.hellmich@cern.ch>
#ifndef DMLITE_PYTHON_PYTHONCOMMON_H
#define DMLITE_PYTHON_PYTHONCOMMON_H

#include <boost/python.hpp>

#include <dmlite/cpp/exceptions.h>
#include <dmlite/cpp/inode.h>
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

class PythonExceptionHandler {
public:
  static void extractException() throw (DmException);
};

struct stl_vector_replica_from_python_list {
  static void* convertible(PyObject* obj_ptr)
  {
    if (!PyList_Check(obj_ptr)) return 0;
    PyObject *wantedItem = boost::python::object(Replica()).ptr();
    for (int i = 0; i < PyList_Size(obj_ptr); i++) {
      if (!PyObject_TypeCheck(PyList_GetItem(obj_ptr, i), wantedItem->ob_type)) return 0;
    }
    return obj_ptr;
  }

  static void construct(
  PyObject* obj_ptr,
  boost::python::converter::rvalue_from_python_stage1_data* data)
  {
    void* storage=((boost::python::converter::rvalue_from_python_storage<std::vector<Replica> >*)(data))->storage.bytes;
    new (storage) std::vector<Replica>();
    std::vector<Replica>* v=(std::vector<Replica>*)(storage);
    int l=PySequence_Size(obj_ptr); 
    if(l<0) abort();
    v->reserve(l);
    for(int i=0; i<l; i++) {
      v->push_back(boost::python::extract<Replica>(PySequence_GetItem(obj_ptr,i)));
    }
    data->convertible=storage;
  }
};


/*
class PythonCommon {
public:
  template <class T>
  T runPythonMethod(PythonMain py, const char *methodName, DmException e, ...)
  {
    using namespace boost::python;
  
    try {
      object mod = boost::any_cast<object>(py["module"]);
      object result = mod.attr(methodName)(va_arg);
      if (result.ptr() == Py_None) {
        throw e;
      }
      return extract<T>(result);
    } catch (error_already_set const &) {
      PyErr_Print();
    }
  };
};
*/
};

#endif // DMLITE_PYTHON_PYTHONCOMMON_H
