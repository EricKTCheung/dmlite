cmake_minimum_required (VERSION 2.6)

add_library(dmliteutils SHARED Security.cpp Urls.cpp)

set_target_properties(dmliteutils PROPERTIES
                                   VERSION ${dmlite_MAJOR}.${dmlite_MINOR}.${dmlite_PATCH}
                                   SOVERSION ${dmlite_MAJOR}
                                   SOVERSION ${dmlite_MAJOR}.${dmlite_MINOR})

target_link_libraries (dmliteutils crypto ssl)

install(TARGETS       dmliteutils
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}
                      COMPONENT   dmlite-libs)
