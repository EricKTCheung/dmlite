cmake_minimum_required (VERSION 2.6)

add_library(dmlitecommon SHARED Security.cpp Urls.cpp)

set_target_properties(dmlitecommon PROPERTIES
                                   VERSION ${dmlite_MAJOR}.${dmlite_MINOR}.${dmlite_PATCH}
                                   SOVERSION ${dmlite_MAJOR}
                                   SOVERSION ${dmlite_MAJOR}.${dmlite_MINOR})

target_link_libraries (dmlitecommon crypto ssl)

install(TARGETS       dmlitecommon
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}
                      COMPONENT   dmlite-libs)
