cmake_minimum_required (VERSION 2.6)

add_library(dmlite SHARED PluginManager.cpp
                          PluginManager-C.cpp
                          Catalog.cpp
                          Catalog-C.cpp
                          Exceptions.cpp
                          dummy/Dummy.cpp
                          dummy/DummyCatalog.cpp)

set_target_properties(dmlite PROPERTIES
                             VERSION ${libdm_MAJOR}.${libdm_MINOR}.${libdm_PATCH}
                             SOVERSION ${libdm_MAJOR}
                             SOVERSION ${libdm_MAJOR}.${libdm_MINOR})

install(TARGETS       dmlite
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}
                      COMPONENT   dmlite-libs)
