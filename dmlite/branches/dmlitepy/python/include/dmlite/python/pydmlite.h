/*
 * pydmlite.h
 *
 * Includes required by all the pydmlite module files.
 */

#include <boost/python.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>

#include "authn.h"
#include "base.h"
#include "catalog.h"
#include "dmlite.h"
#include "exceptions.h"
#include "inode.h"
#include "io.h"
#include "pooldriver.h"
#include "poolmanager.h"
#include "utils/urls.h"

using namespace dmlite;
using namespace boost::python;

template <typename T>
struct stl_vector_from_python_sequence {
  stl_vector_from_python_sequence() 
  {
    boost::python::converter::registry::push_back(
      &convertible,
      &construct,
      boost::python::type_id<std::vector<T> >());
  }

  static void* convertible(PyObject* obj_ptr)
  {
    if(!PySequence_Check(obj_ptr) || !PyObject_HasAttrString(obj_ptr,"__len__")) return 0;
    return obj_ptr;
  }

  static void construct(
  PyObject* obj_ptr,
  boost::python::converter::rvalue_from_python_stage1_data* data)
  {
    void* storage=((boost::python::converter::rvalue_from_python_storage<std::vector<T> >*)(data))->storage.bytes;
    new (storage) std::vector<T>();
    std::vector<T>* v=(std::vector<T>*)(storage);
    int l=PySequence_Size(obj_ptr); 
    if(l<0) abort();
    v->reserve(l);
    for(int i=0; i<l; i++) {
      v->push_back(boost::python::extract<T>(PySequence_GetItem(obj_ptr,i)));
    }
    data->convertible=storage;
  }
};

template <typename T>
struct stl_vector_from_python_list_with_typecheck {
  stl_vector_from_python_list_with_typecheck() 
  {
    boost::python::converter::registry::push_back(
      &convertible,
      &construct,
      boost::python::type_id<std::vector<T> >());
  }

  static void* convertible(PyObject* obj_ptr)
  {
    //if(!PySequence_Check(obj_ptr) || !PyObject_HasAttrString(obj_ptr,"__len__")) return 0;
    //*
    if (!PySequence_Check(obj_ptr)) return 0;
    PyObject *wantedItem = boost::python::object(T()).ptr();
    for (int i = 0; i < PyList_Size(obj_ptr); i++) {
      if (!PyObject_TypeCheck(PyList_GetItem(obj_ptr, i), wantedItem->ob_type)) return 0;
    }
    //*/
    return obj_ptr;
  }

  static void construct(
  PyObject* obj_ptr,
  boost::python::converter::rvalue_from_python_stage1_data* data)
  {
    void* storage=((boost::python::converter::rvalue_from_python_storage<std::vector<T> >*)(data))->storage.bytes;
    new (storage) std::vector<T>();
    std::vector<T>* v=(std::vector<T>*)(storage);
    int l=PySequence_Size(obj_ptr); 
    if(l<0) abort();
    v->reserve(l);
    for(int i=0; i<l; i++) {
      v->push_back(boost::python::extract<T>(PySequence_GetItem(obj_ptr,i)));
    }
    data->convertible=storage;
  }
};