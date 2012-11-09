/*
 * pydmlite.cpp
 *
 * Python bindings for the c++ dmlite library via Boost:Python,
 * especially the bindings for dmlite.h are directly here.
 * The resulting python module pydmlite gives access to most
 * the available functionality from dmlite.
 */

#include "pydmlite.h"

void export_extensible();
void export_errno();
void export_types();

void export_authn();
void export_base();
void export_catalog();
void export_exceptions();
void export_inode();
void export_io();
void export_pooldriver();
void export_poolmanager();

ExtendedStat::FileStatus identity_(ExtendedStat::FileStatus x)
{ 
    return x;
}

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

// taken from http://stackoverflow.com/questions/9620268/boost-python-custom-exception-class
/*
PyObject* createExceptionClass(const char* name, PyObject* baseTypeObj = PyExc_Exception)
{
    using std::string;
    namespace bp = boost::python;

    string scopeName = bp::extract<string>(bp::scope().attr("__name__"));
    string qualifiedName0 = scopeName + "." + name;
    char* qualifiedName1 = const_cast<char*>(qualifiedName0.c_str());

    PyObject* typeObj = PyErr_NewException(qualifiedName1, baseTypeObj, 0);
    if(!typeObj) bp::throw_error_already_set();
    bp::scope().attr(name) = bp::handle<>(bp::borrowed(typeObj));
    return typeObj;
}

PyObject* dmExceptionTypeObj = 0;

void translator(const DmException &x) {
    object exc(x); // wrap the C++ exception

    object exc_t(handle<>(borrowed(dmExceptionTypeObj)));
    exc_t.attr("cause") = exc; // add the wrapped exception to the Python exception

    PyErr_SetString(dmExceptionTypeObj, x.what());
}
//*/
BOOST_PYTHON_MODULE(pydmlite)
{
    // These python bindings are compliant with version 20120817.
    scope().attr("API_VERSION") = API_VERSION; 
/*
    register_exception_translator<DmException>(&translator);

    dmExceptionTypeObj = createExceptionClass("DmException");
//*/
    def("identity", identity_);

    stl_vector_replica_from_python_list();
  
    class_<PluginManager, boost::noncopyable>("PluginManager")
        .def("loadPlugin", &PluginManager::loadPlugin)
        .def("configure", &PluginManager::configure)
        .def("loadConfiguration", &PluginManager::loadConfiguration)

        .def("registerAuthnFactory", &PluginManager::registerAuthnFactory)
        .def("registerINodeFactory", &PluginManager::registerINodeFactory)
        .def("registerCatalogFactory", &PluginManager::registerCatalogFactory)
        .def("registerPoolManagerFactory", &PluginManager::registerPoolManagerFactory)
        .def("registerIOFactory", &PluginManager::registerIOFactory)
        .def("registerPoolDriverFactory", &PluginManager::registerPoolDriverFactory)
        
        .def("getAuthnFactory", &PluginManager::getAuthnFactory, return_value_policy<reference_existing_object>())
        .def("getINodeFactory", &PluginManager::getINodeFactory, return_value_policy<reference_existing_object>())
        .def("getCatalogFactory", &PluginManager::getCatalogFactory, return_value_policy<reference_existing_object>())
        .def("getPoolManagerFactory", &PluginManager::getPoolManagerFactory, return_value_policy<reference_existing_object>())
        .def("getPoolDriverFactory", &PluginManager::getPoolDriverFactory, return_value_policy<reference_existing_object>())
        .def("getIOFactory", &PluginManager::getIOFactory, return_value_policy<reference_existing_object>())
        ;
    
    class_<StackInstance>("StackInstance", init<PluginManager*>())
        .def("set", &StackInstance::set)
        .def("get", &StackInstance::get)
        .def("erase", &StackInstance::erase)
        .def("getPluginManager", &StackInstance::getPluginManager, return_value_policy<reference_existing_object>())
        .def("setSecurityCredentials", &StackInstance::setSecurityCredentials)
        .def("setSecurityContext", &StackInstance::setSecurityContext)
        .def("getSecurityContext", &StackInstance::getSecurityContext, return_value_policy<reference_existing_object>())
        .def("getAuthn", &StackInstance::getAuthn, return_value_policy<reference_existing_object>())
        .def("getINode", &StackInstance::getINode, return_value_policy<reference_existing_object>())
        .def("getCatalog", &StackInstance::getCatalog, return_value_policy<reference_existing_object>())
        .def("isTherePoolManager", &StackInstance::isTherePoolManager)
        .def("getPoolManager", &StackInstance::getPoolManager, return_value_policy<reference_existing_object>())
        .def("getPoolDriver", &StackInstance::getPoolDriver, return_value_policy<reference_existing_object>())
        .def("getIODriver", &StackInstance::getIODriver, return_value_policy<reference_existing_object>())
        ;

    class_<PluginIdCard>("PluginIdCard")
        .def_readonly("ApiVersion", &PluginIdCard::ApiVersion)
        // registerPlugin not exposed yet
        ;

    scope().attr("PLUGIN_ID_HEADER") = PLUGIN_ID_HEADER; 

    export_extensible();
    export_errno();
    export_types();

    export_authn();
    export_base();
    export_catalog();
    export_exceptions();
    export_inode();
    export_io();
    export_pooldriver();
    export_poolmanager();
}
