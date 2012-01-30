cmake_minimum_required (VERSION 2.6)

add_library(dmlitecommon SHARED Security.cpp Uris.cpp)

target_link_libraries(dmlitecommon dmlite)

set_target_properties(dmlitecommon PROPERTIES
                                   VERSION ${libdm_MAJOR}.${libdm_MINOR}.${libdm_PATCH}
                                   SOVERSION ${libdm_MAJOR}
                                   SOVERSION ${libdm_MAJOR}.${libdm_MINOR})

install(TARGETS       dmlitecommon
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}
                      COMPONENT   dmlite-libs)
