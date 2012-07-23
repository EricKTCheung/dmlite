cmake_minimum_required (VERSION 2.6)

find_package(Protobuf REQUIRED)

# build the protobuf *.pb.(h|cc) files
wrap_proto(PROTO_SRC s3objects.proto)

add_library(s3pool MODULE S3.cpp S3Factory.cpp s3driver.cpp ${PROTO_SRC})
target_link_libraries (s3pool dmlite dmliteutils neon dl protobuf)
set_target_properties (s3pool PROPERTIES PREFIX "plugin_")

install(TARGETS       s3pool
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/
                      COMPONENT   dmlite-plugins)
