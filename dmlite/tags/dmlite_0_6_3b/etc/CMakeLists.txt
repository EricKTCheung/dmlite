cmake_minimum_required (VERSION 2.6)

configure_file (${CMAKE_CURRENT_SOURCE_DIR}/dmlite.conf.in
                ${CMAKE_CURRENT_BINARY_DIR}/dmlite.conf
                @ONLY)

install (FILES       ${CMAKE_CURRENT_BINARY_DIR}/dmlite.conf
         DESTINATION etc)

# Create empty directories
file (MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/dmlite.conf.d)

install (DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/dmlite.conf.d
         DESTINATION etc)
