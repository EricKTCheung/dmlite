cmake_minimum_required (VERSION 2.6)

add_library(dmlitec SHARED Catalog-C.cpp
                           IO-C.cpp
                           PluginManager-C.cpp
                           Pool-C.cpp
                           Private.h
                           Utils-C.cpp)

target_link_libraries(dmlitec dmlite dmliteutils dl pthread)

set_target_properties(dmlitec PROPERTIES
                              VERSION ${dmlite_MAJOR}.${dmlite_MINOR}.${dmlite_PATCH}
                              SOVERSION ${dmlite_MAJOR})

install(TARGETS       dmlitec
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX})
