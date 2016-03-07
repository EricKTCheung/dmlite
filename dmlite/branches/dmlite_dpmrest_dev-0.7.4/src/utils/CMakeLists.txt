cmake_minimum_required (VERSION 2.6)

find_package(Boost COMPONENTS regex REQUIRED)

if(DAVIX_PKG_FOUND)
  set(DMLITE_DAVIX_POOL_SOURCES utils/DavixPool.cpp
                                utils/DomeTalker.cpp)
endif()

set(DMLITE_UTILS_SOURCES utils/Checksums.cpp
                         utils/Extensible.cpp
                         utils/Security.cpp
                         utils/Urls.cpp
                         utils/Logger.cpp                       
                         utils/Config.cc
                         ${DMLITE_DAVIX_POOL_SOURCES}
    PARENT_SCOPE)
