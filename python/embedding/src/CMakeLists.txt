cmake_minimum_required (VERSION 2.6)

find_package(Boost COMPONENTS python REQUIRED)
find_package(PythonLibs)
find_package(PythonInterp)

execute_process (COMMAND python -c "from distutils.sysconfig import get_python_lib; print get_python_lib(1)" 
		 OUTPUT_VARIABLE PYTHON_SITE_PACKAGES
		 OUTPUT_STRIP_TRAILING_WHITESPACE)

include_directories (${Boost_INCLUDE_DIRS} ${PYTHON_INCLUDE_PATH} ${DMLITE_INCLUDE_DIR})
link_directories(${Boost_LIBRARY_DIRS} ${CMAKE_BINARY_DIR}/src)

add_library(dmlitepy SHARED cpp/PythonCommon.cpp
                            cpp/PythonINode.cpp
                            cpp/PythonCatalog.cpp
                            cpp/PythonPoolDriver.cpp
           )

target_link_libraries (dmlitepy ${Boost_LIBRARIES} ${PYTHON_LIBRARIES}
                                dmlite)
set_target_properties (dmlitepy PROPERTIES
                                LINK_FLAGS "-rdynamic"
                                VERSION ${dmlite_MAJOR}.${dmlite_MINOR}.${dmlite_PATCH}
                                SOVERSION ${dmlite_MAJOR})

install(TARGETS        dmlitepy
        LIBRARY        DESTINATION usr/lib${LIB_SUFFIX})


install(FILES          python/exceptions.py
        DESTINATION    ${PYTHON_SITE_PACKAGES}/dmlite
        PERMISSIONS    OWNER_EXECUTE OWNER_WRITE OWNER_READ
                       GROUP_EXECUTE GROUP_READ
                       WORLD_EXECUTE WORLD_READ)

install(FILES          python/__init__.py
        DESTINATION    ${PYTHON_SITE_PACKAGES}/dmlite
        PERMISSIONS    OWNER_EXECUTE OWNER_WRITE OWNER_READ 
                       GROUP_EXECUTE GROUP_READ
                       WORLD_EXECUTE
                       WORLD_READ)
