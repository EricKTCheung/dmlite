cmake_minimum_required (VERSION 2.6)

find_package(Boost COMPONENTS regex REQUIRED)

set(DMLITE_UTILS_SOURCES utils/Checksums.cpp
                         utils/Extensible.cpp
                         utils/Security.cpp
                         utils/Urls.cpp
    PARENT_SCOPE)
