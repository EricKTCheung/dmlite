cmake_minimum_required (VERSION 2.6)

find_package(Boost COMPONENTS python REQUIRED)
find_package(PythonLibs)

if ( CMAKE_INSTALL_PREFIX STREQUAL "/" )
  message(STATUS "CMAKE_INSTALL_PREFIX points to system-wide: ${CMAKE_INSTALL_PREFIX} Will install the python stuff according to the python config")
  execute_process (COMMAND python -c "from distutils.sysconfig import get_python_lib; print get_python_lib(1)" 
		 OUTPUT_VARIABLE INSTALL_PYTHON_SITE_PACKAGES
		 OUTPUT_STRIP_TRAILING_WHITESPACE)
  set(INSTALL_PYTHON_SITE_PACKAGES ${INSTALL_PYTHON_SITE_PACKAGES} CACHE FILEPATH "Installation destination for python things, coming from python system-wide")
else()
  set(INSTALL_PYTHON_SITE_PACKAGES ${INSTALL_PFX_LIB}/python/dist-packages CACHE FILEPATH "Installation destination for python things")
endif ()
  
  
include_directories (${Boost_INCLUDE_DIRS} ${PYTHON_INCLUDE_PATH})
link_directories(${Boost_LIBRARY_DIRS} ${CMAKE_BINARY_DIR}/src)


add_library(pydmlite 
            SHARED pydmlite.cpp authn.cpp base.cpp catalog.cpp errno.cpp 
                   exceptions.cpp extensible.cpp inode.cpp io.cpp 
                   pooldriver.cpp poolmanager.cpp types.cpp)

IF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  target_link_libraries(pydmlite ${Boost_LIBRARIES} ${PYTHON_LIBRARIES} dmlite)
ELSE()
  target_link_libraries(pydmlite ${Boost_LIBRARIES} dmlite)
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

set_target_properties (pydmlite PROPERTIES PREFIX "")

install(TARGETS       pydmlite
        LIBRARY       DESTINATION ${INSTALL_PYTHON_SITE_PACKAGES}
        PERMISSIONS   OWNER_EXECUTE OWNER_WRITE OWNER_READ
                      GROUP_EXECUTE GROUP_READ
                      WORLD_EXECUTE WORLD_READ
)
