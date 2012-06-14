cmake_minimum_required (VERSION 2.6)

add_library(dmlite SHARED Auth.cpp
                          Defaults.cpp
                          Catalog-C.cpp
                          Exceptions.cpp
                          IO-C.cpp
                          PluginManager.cpp
                          PluginManager-C.cpp
                          Pool-C.cpp
                          StackInstance.cpp
                          builtin/Catalog.cpp
                          builtin/IO.cpp
                          dummy/DummyCatalog.cpp
                          dummy/DummyINode.cpp
                          dummy/DummyPool.cpp)

target_link_libraries(dmlite dl pthread dmlitecommon)

set_target_properties(dmlite PROPERTIES
                             VERSION ${dmlite_MAJOR}.${dmlite_MINOR}.${dmlite_PATCH}
                             SOVERSION ${dmlite_MAJOR}
                             SOVERSION ${dmlite_MAJOR}.${dmlite_MINOR})

install(TARGETS       dmlite
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}
                      COMPONENT   dmlite-libs)
