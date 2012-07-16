cmake_minimum_required (VERSION 2.6)

add_library(dmlite SHARED Auth.cpp
                          Defaults.cpp
                          Exceptions.cpp
                          PluginManager.cpp
                          StackInstance.cpp
                          Types.cpp

                          builtin/Catalog.cpp
                          builtin/UserGroupDb.cpp

                          c/Catalog-C.cpp
                          c/IO-C.cpp
                          c/PluginManager-C.cpp
                          c/Pool-C.cpp
                          c/Utils-C.cpp
                          
                          dummy/DummyCatalog.cpp
                          dummy/DummyPool.cpp)

target_link_libraries(dmlite dl pthread dmliteutils)

set_target_properties(dmlite PROPERTIES
                             VERSION ${dmlite_MAJOR}.${dmlite_MINOR}.${dmlite_PATCH}
                             SOVERSION ${dmlite_MAJOR})

install(TARGETS       dmlite
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX})
