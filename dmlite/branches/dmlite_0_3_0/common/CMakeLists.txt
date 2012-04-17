cmake_minimum_required (VERSION 2.6)

add_library(dmlitecommon SHARED Security.cpp Uris.cpp)

set_target_properties(dmlitecommon PROPERTIES
                                   VERSION ${dmlite_MAJOR}.${dmlite_MINOR}.${dmlite_PATCH}
                                   SOVERSION ${dmlite_MAJOR}
                                   SOVERSION ${dmlite_MAJOR}.${dmlite_MINOR})

install(TARGETS       dmlitecommon
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}
                      COMPONENT   dmlite-libs)
