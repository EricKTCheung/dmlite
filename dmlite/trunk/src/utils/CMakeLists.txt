cmake_minimum_required (VERSION 2.6)

find_package(Boost COMPONENTS regex REQUIRED)

if(DAVIX_PKG_FOUND)
  set(DMLITE_DAVIX_POOL_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/DavixPool.cpp
                                ${CMAKE_CURRENT_SOURCE_DIR}/DomeTalker.cpp
                                PARENT_SCOPE)
endif()

set(DMLITE_UTILS_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/Checksums.cpp
                         ${CMAKE_CURRENT_SOURCE_DIR}/Extensible.cpp
                         ${CMAKE_CURRENT_SOURCE_DIR}/Security.cpp
                         ${CMAKE_CURRENT_SOURCE_DIR}/Urls.cpp
                         ${CMAKE_CURRENT_SOURCE_DIR}/Logger.cpp                       
                         ${CMAKE_CURRENT_SOURCE_DIR}/Config.cc
    PARENT_SCOPE)
