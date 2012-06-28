cmake_minimum_required (VERSION 2.6)

add_library(s3pool MODULE S3.cpp s3driver.cpp)
target_link_libraries (s3pool dmlite dmliteutils dl)
set_target_properties (s3pool PROPERTIES PREFIX "plugin_")

install(TARGETS       s3pool
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/
                      COMPONENT   dmlite-plugins)
