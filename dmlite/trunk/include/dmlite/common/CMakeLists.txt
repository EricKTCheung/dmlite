cmake_minimum_required (VERSION 2.6)

# Config header
configure_file (config.h.cmake
                ${CMAKE_CURRENT_SOURCE_DIR}/config.h)

# Install headers
install (FILES		${CMAKE_CURRENT_BINARY_DIR}/config.h errno.h
         DESTINATION	usr/include/dmlite/common
)
